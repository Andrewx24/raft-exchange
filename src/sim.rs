//! Deterministic simulation of a whole cluster in one thread.
//!
//! Every source of nondeterminism — message delay, loss, duplication and
//! reordering; partitions; crashes and restarts; client traffic — comes from
//! one seeded RNG. A failing seed fails the same way every time, which turns
//! "a rare race in production" into "run seed 4127 under a debugger".
//!
//! Checked continuously, not just at the end:
//!
//! - **Election safety**: at most one leader per term, ever.
//! - **State machine safety**: no two nodes ever apply different commands
//!   at the same log index (which also covers leader completeness: a
//!   committed entry that a later leader lost would show up here).
//! - **Durability across crashes**: a restarted node rebuilds from its
//!   simulated disk and must converge to the same transcript.
//!
//! And at the end, after the network heals: every node converges to the
//! same commit index and the same exchange transcript hash, and every
//! node's exchange satisfies the conservation and lock invariants.

use crate::book::Side;
use crate::exchange::{Command, Event, Exchange, TimeInForce};
use crate::raft::{
    Config, Entry, Envelope, HardState, Index, Message, NodeId, Payload, Raft, Role, Term,
};
use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BinaryHeap};
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
    pub fn chance(&mut self, p: f64) -> bool {
        ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SimConfig {
    pub nodes: u64,
    pub ticks: u64,
    /// After faults stop, the cluster must converge within this many ticks
    /// (a liveness bound, not a fixed wait).
    pub settle_ticks: u64,
    pub drop: f64,
    pub duplicate: f64,
    pub max_delay: u64,
    pub partition: f64,
    pub heal: f64,
    pub crash: f64,
    pub restart: f64,
    pub client_rate: f64,
    /// Small batches split a leader's no-op from the entries before it,
    /// which is what exposes commit-rule bugs (Raft §5.4.2).
    pub max_entries_per_append: usize,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            nodes: 5,
            ticks: 3_000,
            settle_ticks: 20_000,
            drop: 0.05,
            duplicate: 0.02,
            max_delay: 4,
            partition: 0.003,
            heal: 0.02,
            crash: 0.002,
            restart: 0.02,
            client_rate: 0.6,
            max_entries_per_append: 256,
        }
    }
}

impl SimConfig {
    /// Rotates fault profiles and batch sizes across seeds, so a sweep
    /// explores crash storms, long partitions, very lossy networks and
    /// tiny append batches instead of one average-case mix.
    pub fn for_seed(seed: u64) -> Self {
        let base = SimConfig::default();
        let mut c = match seed % 4 {
            0 => base,
            1 => SimConfig {
                crash: 0.02,
                restart: 0.25,
                ..base
            },
            2 => SimConfig {
                partition: 0.02,
                heal: 0.04,
                ..base
            },
            _ => SimConfig {
                drop: 0.2,
                duplicate: 0.05,
                max_delay: 10,
                ..base
            },
        };
        c.max_entries_per_append = [1, 2, 8, 256][(seed / 4 % 4) as usize];
        c
    }

    fn raft(&self) -> Config {
        Config {
            max_entries_per_append: self.max_entries_per_append,
            ..Config::default()
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Disk {
    hard_state: HardState,
    log: Vec<Entry<Command>>,
}

struct Node {
    raft: Raft<Command>,
    disk: Disk,
    exchange: Exchange,
    up: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SimStats {
    pub committed: Index,
    pub applied_commands: u64,
    pub trades: u64,
    pub leaders_elected: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub messages: u64,
    pub dropped: u64,
    pub max_term: Term,
    /// Ticks the cluster took to converge once the network healed.
    pub settle_ticks: u64,
}

pub struct Sim {
    cfg: SimConfig,
    seed: u64,
    rng: Rng,
    nodes: Vec<Node>,
    now: u64,
    seq: u64,
    net: BinaryHeap<Reverse<(u64, u64, usize)>>,
    in_flight: Vec<Option<Envelope<Command>>>,
    /// Which side of a partition each node is on; `None` = fully connected.
    partition: Option<Vec<u8>>,
    leaders: BTreeMap<Term, NodeId>,
    votes: BTreeMap<(NodeId, Term), NodeId>,
    applied_at: BTreeMap<Index, u64>,
    pub stats: SimStats,
    events: Vec<Event>,
    proposed: u64,
}

fn digest(e: &Entry<Command>) -> u64 {
    let mut h = DefaultHasher::new();
    e.term.hash(&mut h);
    e.payload.hash(&mut h);
    h.finish()
}

fn genesis() -> Vec<Command> {
    vec![Command::CreateMarket {
        market: 1,
        base: 2,
        quote: 1,
    }]
}

impl Sim {
    pub fn new(seed: u64, cfg: SimConfig) -> Self {
        let ids: Vec<NodeId> = (1..=cfg.nodes).collect();
        let nodes = ids
            .iter()
            .map(|&id| Node {
                raft: Raft::new(id, ids.clone(), cfg.raft(), seed),
                disk: Disk::default(),
                exchange: Exchange::new(),
                up: true,
            })
            .collect();
        Sim {
            cfg,
            seed,
            rng: Rng::new(seed),
            nodes,
            now: 0,
            seq: 0,
            net: BinaryHeap::new(),
            in_flight: Vec::new(),
            partition: None,
            leaders: BTreeMap::new(),
            votes: BTreeMap::new(),
            applied_at: BTreeMap::new(),
            stats: SimStats::default(),
            events: Vec::new(),
            proposed: 0,
        }
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        self.partition
            .as_ref()
            .is_none_or(|p| p[a as usize - 1] == p[b as usize - 1])
    }

    fn leader(&self) -> Option<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.up && n.raft.role() == Role::Leader)
            .max_by_key(|(_, n)| n.raft.term())
            .map(|(i, _)| i)
    }

    fn random_command(&mut self) -> Command {
        self.proposed += 1;
        // The market must exist; re-propose it now and then in case the
        // leader that accepted it crashed before it committed.
        if self.proposed < 3 || self.rng.chance(0.005) {
            return genesis()[0].clone();
        }
        let account = 1 + self.rng.below(8);
        match self.rng.below(10) {
            0 | 1 => Command::Deposit {
                account,
                asset: 1 + self.rng.below(2) as u32,
                amount: 1 + self.rng.below(10_000),
            },
            2 => Command::Withdraw {
                account,
                asset: 1 + self.rng.below(2) as u32,
                amount: 1 + self.rng.below(2_000),
            },
            3 => Command::Cancel {
                account,
                order: 1 + self.rng.below(self.proposed),
            },
            _ => Command::Place {
                account,
                market: 1,
                side: if self.rng.chance(0.5) {
                    Side::Buy
                } else {
                    Side::Sell
                },
                price: 95 + self.rng.below(11),
                qty: 1 + self.rng.below(20),
                tif: match self.rng.below(6) {
                    0 => TimeInForce::Ioc,
                    1 => TimeInForce::PostOnly,
                    _ => TimeInForce::Gtc,
                },
            },
        }
    }

    fn faults(&mut self) {
        let n = self.nodes.len();
        if self.partition.is_none() && self.rng.chance(self.cfg.partition) {
            let sides: Vec<u8> = (0..n).map(|_| self.rng.below(2) as u8).collect();
            self.partition = Some(sides);
            self.stats.partitions += 1;
        } else if self.partition.is_some() && self.rng.chance(self.cfg.heal) {
            self.partition = None;
        }
        for i in 0..n {
            if self.nodes[i].up && self.rng.chance(self.cfg.crash) {
                // Crash: everything not on disk is gone.
                self.nodes[i].up = false;
                self.stats.crashes += 1;
            } else if !self.nodes[i].up && self.rng.chance(self.cfg.restart) {
                self.restart(i);
            }
        }
    }

    fn restart(&mut self, i: usize) {
        let ids: Vec<NodeId> = (1..=self.cfg.nodes).collect();
        let node = &mut self.nodes[i];
        node.raft = Raft::restore(
            i as NodeId + 1,
            ids,
            self.cfg.raft(),
            self.seed ^ self.now,
            node.disk.hard_state,
            node.disk.log.clone(),
        );
        node.exchange = Exchange::new();
        node.up = true;
    }

    fn process(&mut self, i: usize) -> Result<(), String> {
        let ready = self.nodes[i].raft.ready();
        let node = &mut self.nodes[i];
        // 1. Persist (before anything leaves the node).
        if let Some(hs) = ready.hard_state {
            node.disk.hard_state = hs;
        }
        if let Some(from) = ready.persist_from {
            node.disk.log.truncate(from as usize - 1);
            node.disk.log.extend_from_slice(node.raft.log_from(from));
        }
        // 2. Apply, checking state machine safety against every other node.
        for (index, entry) in ready.committed {
            let d = digest(&entry);
            match self.applied_at.get(&index) {
                Some(&other) if other != d => {
                    return Err(format!(
                        "state machine safety: node {} applied a different entry at index {index}",
                        i + 1
                    ));
                }
                _ => {
                    self.applied_at.insert(index, d);
                }
            }
            if let Payload::Command(cmd) = &entry.payload {
                self.events.clear();
                self.nodes[i].exchange.apply(cmd, &mut self.events);
            }
            self.stats.committed = self.stats.committed.max(index);
        }
        // 3. Send through the faulty network.
        for env in ready.messages {
            self.stats.messages += 1;
            // Vote safety, checked on the wire: one vote per node per term,
            // across crashes. This is what persisting the vote protects.
            if let Message::VoteResponse {
                term,
                granted: true,
            } = env.msg
            {
                match self.votes.insert((env.from, term), env.to) {
                    Some(prev) if prev != env.to => {
                        return Err(format!(
                            "vote safety: node {} voted for both {prev} and {} in term {term}",
                            env.from, env.to
                        ));
                    }
                    _ => {}
                }
            }
            if self.rng.chance(self.cfg.drop) {
                self.stats.dropped += 1;
                continue;
            }
            let copies = if self.rng.chance(self.cfg.duplicate) {
                2
            } else {
                1
            };
            for _ in 0..copies {
                let at = self.now + 1 + self.rng.below(self.cfg.max_delay);
                self.seq += 1;
                self.in_flight.push(Some(env.clone()));
                self.net
                    .push(Reverse((at, self.seq, self.in_flight.len() - 1)));
            }
        }
        // Election safety.
        let r = &self.nodes[i].raft;
        if r.role() == Role::Leader {
            match self.leaders.insert(r.term(), r.id()) {
                Some(prev) if prev != r.id() => {
                    return Err(format!(
                        "election safety: nodes {prev} and {} both led term {}",
                        r.id(),
                        r.term()
                    ));
                }
                None => self.stats.leaders_elected += 1,
                _ => {}
            }
            self.stats.max_term = self.stats.max_term.max(r.term());
        }
        Ok(())
    }

    fn step(&mut self, faults: bool) -> Result<(), String> {
        self.now += 1;
        if faults {
            self.faults();
        }
        if self.rng.chance(self.cfg.client_rate) {
            let cmd = self.random_command();
            // Clients find the leader; a follower answers with a hint.
            if let Some(l) = self.leader() {
                let _ = self.nodes[l].raft.propose(cmd);
            }
        }
        for i in 0..self.nodes.len() {
            if self.nodes[i].up {
                self.nodes[i].raft.tick();
            }
        }
        while let Some(Reverse((at, _, slot))) = self.net.peek().copied() {
            if at > self.now {
                break;
            }
            self.net.pop();
            let env = self.in_flight[slot].take().expect("delivered once");
            let to = env.to as usize - 1;
            if self.nodes[to].up && self.connected(env.from, env.to) {
                self.nodes[to].raft.step(env);
            }
        }
        if self.net.is_empty() {
            self.in_flight.clear();
        }
        for i in 0..self.nodes.len() {
            if self.nodes[i].up {
                self.process(i)?;
            }
        }
        Ok(())
    }

    /// One leader, and every node has committed exactly its full log.
    fn converged(&self) -> bool {
        let Some(l) = self.leader() else { return false };
        let last = self.nodes[l].raft.last_index();
        self.nodes
            .iter()
            .all(|n| n.raft.commit_index() == last && n.raft.last_index() == last)
    }

    /// Runs the scenario. `Err` names the first violated property.
    pub fn run(&mut self) -> Result<SimStats, String> {
        for _ in 0..self.cfg.ticks {
            self.step(true)?;
        }
        // Heal everything and let the cluster converge.
        self.partition = None;
        for i in 0..self.nodes.len() {
            if !self.nodes[i].up {
                self.restart(i);
            }
        }
        // From here the network is healthy: no loss, duplication or
        // reordering, and no new client traffic.
        let saved = self.cfg;
        self.cfg = SimConfig {
            client_rate: 0.0,
            drop: 0.0,
            duplicate: 0.0,
            max_delay: 1,
            ..self.cfg
        };
        let mut converged = false;
        for t in 0..saved.settle_ticks {
            self.step(false)?;
            if self.converged() {
                self.stats.settle_ticks = t + 1;
                converged = true;
                break;
            }
        }
        self.cfg = saved;

        if !converged {
            let detail: Vec<String> = self
                .nodes
                .iter()
                .map(|n| {
                    let st = n.raft.status();
                    format!(
                        "{}:{:?}/t{}/last{}/commit{}",
                        st.id, st.role, st.term, st.last_index, st.commit
                    )
                })
                .collect();
            return Err(format!(
                "liveness: no convergence {} ticks after healing: {}",
                saved.settle_ticks,
                detail.join(" ")
            ));
        }
        let hashes: Vec<u64> = self
            .nodes
            .iter()
            .map(|n| n.exchange.transcript_hash())
            .collect();
        if hashes.windows(2).any(|w| w[0] != w[1]) {
            return Err(format!("replicas diverged: transcript hashes {hashes:x?}"));
        }
        for (i, n) in self.nodes.iter().enumerate() {
            n.exchange
                .check_invariants()
                .map_err(|e| format!("node {}: {e}", i + 1))?;
        }
        self.stats.applied_commands = self.nodes[0].exchange.applied();
        self.stats.trades = self.nodes[0].exchange.trades();
        Ok(self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_calm_cluster_makes_progress() {
        let cfg = SimConfig {
            drop: 0.0,
            duplicate: 0.0,
            partition: 0.0,
            crash: 0.0,
            ..SimConfig::default()
        };
        let s = Sim::new(1, cfg).run().unwrap();
        assert!(s.applied_commands > 1_000, "{s:?}");
        assert!(s.trades > 0);
    }

    #[test]
    fn same_seed_same_history() {
        let a = Sim::new(42, SimConfig::for_seed(42)).run().unwrap();
        let b = Sim::new(42, SimConfig::for_seed(42)).run().unwrap();
        assert_eq!(
            (a.committed, a.messages, a.trades),
            (b.committed, b.messages, b.trades)
        );
    }
}
