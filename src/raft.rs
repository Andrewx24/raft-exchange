//! Raft consensus, written sans-IO.
//!
//! This module never touches a socket, a disk or a clock. Time is `tick()`,
//! input is `step(message)` and `propose(command)`, and everything the
//! outside world must do is handed back from `ready()`: state to persist,
//! messages to send, entries to apply. The same code runs inside the
//! deterministic simulator (where the "network" is a priority queue the test
//! controls) and inside a real Tokio node (where it is TCP).
//!
//! The contract with the driver, as in etcd/raft: **persist `Ready`'s hard
//! state and entries before sending its messages.** A vote or an
//! acknowledgement that leaves the process before it is durable can be
//! forgotten by a crash, and then the same node votes twice in one term.
//!
//! Implemented: leader election with randomized timeouts, log replication
//! with conflict-term backtracking, commitment restricted to current-term
//! entries (Raft §5.4.2, the "Figure 8" case), a no-op on election so a new
//! leader commits its predecessors' entries promptly, and batching of
//! proposals into one broadcast per `ready()`.
//! Not implemented: snapshots/log compaction, membership changes, pre-vote,
//! read-index linearizable reads.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Payload<C> {
    /// Appended by every new leader; commits entries from earlier terms.
    Noop,
    Command(C),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry<C> {
    pub term: Term,
    pub payload: Payload<C>,
}

/// State that must survive a crash.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message<C> {
    RequestVote {
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
    },
    VoteResponse {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry<C>>,
        leader_commit: Index,
    },
    /// On success `index` is the follower's match index. On failure it is a
    /// hint: the highest index the leader should try next.
    AppendResponse {
        term: Term,
        success: bool,
        index: Index,
    },
}

impl<C> Message<C> {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::VoteResponse { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendResponse { term, .. } => *term,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope<C> {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message<C>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub election_timeout_min: u32,
    pub election_timeout_max: u32,
    pub heartbeat_interval: u32,
    pub max_entries_per_append: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            election_timeout_min: 10,
            election_timeout_max: 20,
            heartbeat_interval: 3,
            max_entries_per_append: 256,
        }
    }
}

/// Everything the driver must do after feeding the node input.
#[derive(Debug)]
pub struct Ready<C> {
    /// Persist if `Some`.
    pub hard_state: Option<HardState>,
    /// Persist log entries from this index to the end, replacing anything
    /// already stored at or after it.
    pub persist_from: Option<Index>,
    /// Send after persisting.
    pub messages: Vec<Envelope<C>>,
    /// Newly committed entries, in order. Apply after persisting.
    pub committed: Vec<(Index, Entry<C>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not the leader (leader hint: {leader:?})")]
pub struct NotLeader {
    pub leader: Option<NodeId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit: Index,
    pub last_index: Index,
}

#[derive(Debug, Clone)]
pub struct Raft<C> {
    id: NodeId,
    peers: Vec<NodeId>,
    cfg: Config,

    term: Term,
    voted_for: Option<NodeId>,
    /// `log[i]` holds index `i + 1`.
    log: Vec<Entry<C>>,

    commit: Index,
    applied: Index,
    role: Role,
    leader: Option<NodeId>,
    votes: BTreeSet<NodeId>,
    next_index: BTreeMap<NodeId, Index>,
    match_index: BTreeMap<NodeId, Index>,

    elapsed: u32,
    timeout: u32,
    rng: u64,

    outbox: Vec<Envelope<C>>,
    hard_state_dirty: bool,
    persist_from: Option<Index>,
    broadcast_pending: bool,
    scratch: Vec<Index>,
}

impl<C: Clone> Raft<C> {
    pub fn new(id: NodeId, peers: Vec<NodeId>, cfg: Config, seed: u64) -> Self {
        Self::restore(id, peers, cfg, seed, HardState::default(), Vec::new())
    }

    /// Restarts a node from its persisted state. Commit and applied indexes
    /// are volatile: the node relearns commit from the leader, and the
    /// driver rebuilds its state machine by applying from index 1.
    pub fn restore(
        id: NodeId,
        peers: Vec<NodeId>,
        cfg: Config,
        seed: u64,
        hs: HardState,
        log: Vec<Entry<C>>,
    ) -> Self {
        let peers: Vec<NodeId> = peers.into_iter().filter(|p| *p != id).collect();
        let mut r = Raft {
            id,
            peers,
            cfg,
            term: hs.term,
            voted_for: hs.voted_for,
            log,
            commit: 0,
            applied: 0,
            role: Role::Follower,
            leader: None,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            elapsed: 0,
            timeout: 0,
            rng: seed ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15),
            outbox: Vec::new(),
            hard_state_dirty: false,
            persist_from: None,
            broadcast_pending: false,
            scratch: Vec::new(),
        };
        r.reset_timeout();
        r
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn commit_index(&self) -> Index {
        self.commit
    }

    pub fn last_index(&self) -> Index {
        self.log.len() as Index
    }

    pub fn status(&self) -> Status {
        Status {
            id: self.id,
            role: self.role,
            term: self.term,
            leader: self.leader,
            commit: self.commit,
            last_index: self.last_index(),
        }
    }

    pub fn entry(&self, index: Index) -> Option<&Entry<C>> {
        index.checked_sub(1).and_then(|i| self.log.get(i as usize))
    }

    fn last_term(&self) -> Term {
        self.log.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, index: Index) -> Term {
        self.entry(index).map_or(0, |e| e.term)
    }

    fn quorum(&self) -> usize {
        let cluster = self.peers.len() + 1;
        cluster / 2 + 1
    }

    fn next_rand(&mut self) -> u64 {
        // SplitMix64: per-node, seeded, so simulated runs are reproducible.
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn reset_timeout(&mut self) {
        let span = (self.cfg.election_timeout_max - self.cfg.election_timeout_min).max(1) as u64;
        self.timeout = self.cfg.election_timeout_min + (self.next_rand() % span) as u32;
        self.elapsed = 0;
    }

    fn mark_persist(&mut self, from: Index) {
        self.persist_from = Some(self.persist_from.map_or(from, |p| p.min(from)));
    }

    fn send(&mut self, to: NodeId, msg: Message<C>) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg,
        });
    }

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        self.elapsed += 1;
        match self.role {
            Role::Leader => {
                if self.elapsed >= self.cfg.heartbeat_interval {
                    self.elapsed = 0;
                    self.broadcast_pending = true;
                }
            }
            _ => {
                if self.elapsed >= self.timeout {
                    self.start_election();
                }
            }
        }
    }

    fn start_election(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.hard_state_dirty = true;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_timeout();
        let (term, last_log_index, last_log_term) =
            (self.term, self.last_index(), self.last_term());
        for p in self.peers.clone() {
            self.send(
                p,
                Message::RequestVote {
                    term,
                    last_log_index,
                    last_log_term,
                },
            );
        }
        if self.votes.len() >= self.quorum() {
            self.become_leader();
        }
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.hard_state_dirty = true;
        }
        if self.role != Role::Follower {
            self.reset_timeout();
        }
        self.role = Role::Follower;
        self.leader = leader;
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.elapsed = 0;
        let next = self.last_index() + 1;
        self.next_index = self.peers.iter().map(|p| (*p, next)).collect();
        self.match_index = self.peers.iter().map(|p| (*p, 0)).collect();
        self.log.push(Entry {
            term: self.term,
            payload: Payload::Noop,
        });
        self.mark_persist(self.last_index());
        self.broadcast_pending = true;
        self.maybe_commit();
    }

    /// Appends a command if this node is the leader. Returns the (term,
    /// index) it was appended at: it is committed only if an entry with
    /// that same term is later committed at that index.
    pub fn propose(&mut self, cmd: C) -> Result<(Term, Index), NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader,
            });
        }
        self.log.push(Entry {
            term: self.term,
            payload: Payload::Command(cmd),
        });
        let index = self.last_index();
        self.mark_persist(index);
        self.broadcast_pending = true;
        self.maybe_commit();
        Ok((self.term, index))
    }

    fn send_append(&mut self, peer: NodeId) {
        let next = self.next_index[&peer];
        let prev_log_index = next - 1;
        let end =
            (self.last_index()).min(prev_log_index + self.cfg.max_entries_per_append as Index);
        let entries = self.log[prev_log_index as usize..end as usize].to_vec();
        // Pipelining: assume delivery and move on, so the next send carries
        // only new entries. Without this every proposal re-sent the whole
        // unacknowledged tail — throughput collapsed as load rose. A lost
        // message is self-healing: the next append arrives with a gap, the
        // follower rejects it, and the rejection rewinds `next_index`.
        self.next_index.insert(peer, end + 1);
        let msg = Message::AppendEntries {
            term: self.term,
            prev_log_index,
            prev_log_term: self.term_at(prev_log_index),
            entries,
            leader_commit: self.commit,
        };
        self.send(peer, msg);
    }

    /// Advances commit to the highest index replicated on a quorum.
    ///
    /// That index is the quorum-th largest match index (counting the
    /// leader's own log) — one small sort over the peers, instead of
    /// scanning every uncommitted index and counting acks for each, which
    /// made a batch of n proposals cost O(n²).
    fn maybe_commit(&mut self) {
        let mut acked = std::mem::take(&mut self.scratch);
        acked.clear();
        acked.extend(self.match_index.values().copied());
        acked.push(self.last_index());
        acked.sort_unstable_by(|a, b| b.cmp(a));
        let n = acked[self.quorum() - 1];
        self.scratch = acked;
        // §5.4.2: never commit an earlier term's entry by counting replicas;
        // it becomes committed only behind a current-term one. Terms never
        // decrease along the log, so if `n` is from an older term no smaller
        // index can be from this one.
        if n > self.commit && self.term_at(n) == self.term {
            self.commit = n;
            self.broadcast_pending = true;
        }
    }

    pub fn step(&mut self, env: Envelope<C>) {
        let Envelope { from, msg, .. } = env;
        if msg.term() > self.term {
            let leader = matches!(msg, Message::AppendEntries { .. }).then_some(from);
            self.become_follower(msg.term(), leader);
        }
        match msg {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                let up_to_date = last_log_term > self.last_term()
                    || (last_log_term == self.last_term() && last_log_index >= self.last_index());
                let free = self.voted_for.is_none() || self.voted_for == Some(from);
                let granted = term == self.term && free && up_to_date;
                if granted {
                    self.voted_for = Some(from);
                    self.hard_state_dirty = true;
                    self.elapsed = 0;
                }
                self.send(
                    from,
                    Message::VoteResponse {
                        term: self.term,
                        granted,
                    },
                );
            }
            Message::VoteResponse { term, granted } => {
                if self.role == Role::Candidate && term == self.term && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= self.quorum() {
                        self.become_leader();
                    }
                }
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if term < self.term {
                    let t = self.term;
                    self.send(
                        from,
                        Message::AppendResponse {
                            term: t,
                            success: false,
                            index: 0,
                        },
                    );
                    return;
                }
                // Same term: a candidate that hears from the winner stands down.
                self.become_follower(term, Some(from));
                self.elapsed = 0;

                if prev_log_index > self.last_index() {
                    let index = self.last_index();
                    self.send(
                        from,
                        Message::AppendResponse {
                            term,
                            success: false,
                            index,
                        },
                    );
                    return;
                }
                if self.term_at(prev_log_index) != prev_log_term {
                    // Skip back over the whole conflicting term in one round
                    // trip instead of one index per round trip.
                    let conflict = self.term_at(prev_log_index);
                    let mut i = prev_log_index;
                    while i > 1 && self.term_at(i - 1) == conflict {
                        i -= 1;
                    }
                    self.send(
                        from,
                        Message::AppendResponse {
                            term,
                            success: false,
                            index: i - 1,
                        },
                    );
                    return;
                }
                let count = entries.len() as Index;
                for (k, e) in entries.into_iter().enumerate() {
                    let idx = prev_log_index + 1 + k as Index;
                    if idx <= self.last_index() {
                        if self.term_at(idx) == e.term {
                            // Already have it. A delayed, reordered append
                            // must never truncate entries that match.
                            continue;
                        }
                        assert!(
                            idx > self.commit,
                            "raft: leader tried to overwrite a committed entry"
                        );
                        self.log.truncate(idx as usize - 1);
                    }
                    self.log.push(e);
                    self.mark_persist(idx);
                }
                let last_new = prev_log_index + count;
                if leader_commit > self.commit {
                    self.commit = leader_commit.min(last_new).max(self.commit);
                }
                self.send(
                    from,
                    Message::AppendResponse {
                        term,
                        success: true,
                        index: last_new,
                    },
                );
            }
            Message::AppendResponse {
                term,
                success,
                index,
            } => {
                if self.role != Role::Leader || term != self.term {
                    return;
                }
                if success {
                    let m = self.match_index.entry(from).or_default();
                    *m = (*m).max(index);
                    let next = self.next_index.entry(from).or_default();
                    *next = (*next).max(index + 1);
                    self.maybe_commit();
                    if self.next_index[&from] <= self.last_index() {
                        self.send_append(from);
                    }
                } else {
                    let floor = self.match_index[&from] + 1;
                    let next = self.next_index.entry(from).or_default();
                    *next = (index + 1).min(*next).max(floor);
                    self.send_append(from);
                }
            }
        }
    }

    /// Drains pending work for the driver.
    pub fn ready(&mut self) -> Ready<C> {
        if self.broadcast_pending && self.role == Role::Leader {
            for p in self.peers.clone() {
                self.send_append(p);
            }
        }
        self.broadcast_pending = false;
        let hard_state = std::mem::take(&mut self.hard_state_dirty).then_some(HardState {
            term: self.term,
            voted_for: self.voted_for,
        });
        let committed = (self.applied + 1..=self.commit)
            .map(|i| (i, self.log[i as usize - 1].clone()))
            .collect();
        self.applied = self.commit;
        Ready {
            hard_state,
            persist_from: self.persist_from.take(),
            messages: std::mem::take(&mut self.outbox),
            committed,
        }
    }

    /// The persisted log from `from` onwards, for the driver to write.
    pub fn log_from(&self, from: Index) -> &[Entry<C>] {
        &self.log[(from as usize - 1).min(self.log.len())..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type R = Raft<u32>;

    /// Delivers every message instantly until the cluster is quiet.
    fn pump(nodes: &mut [R]) -> Vec<Vec<(Index, Entry<u32>)>> {
        let mut applied = vec![Vec::new(); nodes.len()];
        loop {
            let mut msgs = Vec::new();
            for (i, n) in nodes.iter_mut().enumerate() {
                let r = n.ready();
                applied[i].extend(r.committed);
                msgs.extend(r.messages);
            }
            if msgs.is_empty() {
                return applied;
            }
            for m in msgs {
                nodes[(m.to - 1) as usize].step(m);
            }
        }
    }

    fn cluster(n: u64) -> Vec<R> {
        let ids: Vec<NodeId> = (1..=n).collect();
        ids.iter()
            .map(|id| R::new(*id, ids.clone(), Config::default(), 7))
            .collect()
    }

    fn elect(nodes: &mut [R], who: usize) {
        while nodes[who].role() != Role::Leader {
            nodes[who].tick();
            pump(nodes);
        }
    }

    #[test]
    fn elects_a_single_leader_and_replicates() {
        let mut c = cluster(3);
        elect(&mut c, 0);
        assert_eq!(c.iter().filter(|n| n.role() == Role::Leader).count(), 1);
        c[0].propose(41).unwrap();
        c[0].propose(42).unwrap();
        let applied = pump(&mut c);
        for log in &applied {
            let cmds: Vec<_> = log
                .iter()
                .filter_map(|(_, e)| match e.payload {
                    Payload::Command(c) => Some(c),
                    Payload::Noop => None,
                })
                .collect();
            assert_eq!(cmds, vec![41, 42]);
        }
    }

    #[test]
    fn followers_redirect_proposals() {
        let mut c = cluster(3);
        elect(&mut c, 0);
        assert_eq!(c[1].propose(1), Err(NotLeader { leader: Some(1) }));
    }

    #[test]
    fn stale_candidate_with_short_log_cannot_win() {
        let mut c = cluster(3);
        elect(&mut c, 0);
        c[0].propose(1).unwrap();
        pump(&mut c);
        // Node 3 missed nothing here, but simulate it having an older log by
        // building a fresh node and asking for votes at a higher term.
        let mut lagging = R::new(3, vec![1, 2, 3], Config::default(), 1);
        lagging.term = c[1].term();
        lagging.start_election();
        let req = lagging
            .ready()
            .messages
            .into_iter()
            .find(|m| m.to == 2)
            .unwrap();
        c[1].step(req);
        let resp = c[1].ready().messages.pop().unwrap();
        assert_eq!(
            resp.msg,
            Message::VoteResponse {
                term: c[1].term(),
                granted: false
            }
        );
    }

    #[test]
    fn single_node_cluster_commits_alone() {
        let mut c = cluster(1);
        elect(&mut c, 0);
        c[0].propose(9).unwrap();
        let r = c[0].ready();
        assert!(
            r.committed
                .iter()
                .any(|(_, e)| e.payload == Payload::Command(9))
        );
    }
}
