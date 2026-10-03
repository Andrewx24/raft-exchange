//! A real node: the same `Raft` and `Exchange` the simulator tests, driven
//! by a Tokio runtime, TCP between peers and an HTTP API for clients.
//!
//! One task — the driver — owns all consensus and exchange state. Network
//! readers, network writers and HTTP handlers only talk to it through
//! channels, so there is no lock anywhere near the state machine. Each loop
//! iteration drains up to `MAX_BATCH` inputs before calling `ready()`, so a
//! burst of proposals costs one fsync and one broadcast, not one each.
//!
//! Peer writers use bounded queues and drop messages when a peer is slow or
//! down. That is safe — Raft is built for a lossy network and retransmits —
//! and it means one dead peer can never back up memory on the leader.

use crate::exchange::{Command, Event, Exchange};
use crate::raft::{Config, Envelope, Index, NodeId, Payload, Raft, Status, Term};
use crate::storage::Storage;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Copy, Debug)]
pub struct PeerAddr {
    pub raft: SocketAddr,
    pub http: SocketAddr,
}

/// Parses `1=127.0.0.1:7001/127.0.0.1:8001,2=...`.
pub fn parse_cluster(s: &str) -> Result<BTreeMap<NodeId, PeerAddr>, String> {
    s.split(',')
        .map(|part| {
            let (id, addrs) = part
                .split_once('=')
                .ok_or(format!("bad peer spec {part}"))?;
            let (raft, http) = addrs
                .split_once('/')
                .ok_or(format!("bad peer addrs {addrs}"))?;
            Ok((
                id.parse().map_err(|e| format!("{id}: {e}"))?,
                PeerAddr {
                    raft: raft.parse().map_err(|e| format!("{raft}: {e}"))?,
                    http: http.parse().map_err(|e| format!("{http}: {e}"))?,
                },
            ))
        })
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum ProposeError {
    NotLeader {
        leader: Option<NodeId>,
        leader_http: Option<SocketAddr>,
    },
    /// Leadership changed and a different entry committed at this index.
    Superseded,
}

enum Input {
    Message(Envelope<Command>),
    Propose(Command, oneshot::Sender<Result<Vec<Event>, ProposeError>>),
    Query(Query, oneshot::Sender<serde_json::Value>),
}

enum Query {
    Status,
    Book(u32),
    Account(u64),
}

#[derive(Serialize)]
struct NodeStatus {
    #[serde(flatten)]
    raft: Status,
    applied_commands: u64,
    transcript_hash: String,
    trades: u64,
    resting_orders: usize,
}

const MAX_BATCH: usize = 1024;
const FRAME_LIMIT: usize = 64 << 20;

async fn write_frame(w: &mut BufWriter<TcpStream>, env: &Envelope<Command>) -> std::io::Result<()> {
    let body = serde_json::to_vec(env)?;
    w.write_u32(body.len() as u32).await?;
    w.write_all(&body).await
}

async fn read_frames(mut stream: TcpStream, tx: mpsc::Sender<Input>) {
    let mut buf = Vec::new();
    loop {
        let Ok(len) = stream.read_u32().await else {
            return;
        };
        let len = len as usize;
        if len > FRAME_LIMIT {
            tracing::warn!(len, "oversized frame; dropping connection");
            return;
        }
        buf.resize(len, 0);
        if stream.read_exact(&mut buf).await.is_err() {
            return;
        }
        match serde_json::from_slice(&buf) {
            Ok(env) => {
                if tx.send(Input::Message(env)).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "undecodable frame; dropping connection");
                return;
            }
        }
    }
}

/// Owns the connection to one peer. Reconnects lazily; drops what it
/// cannot deliver.
async fn peer_writer(addr: SocketAddr, mut rx: mpsc::Receiver<Envelope<Command>>) {
    let mut conn: Option<BufWriter<TcpStream>> = None;
    while let Some(env) = rx.recv().await {
        if conn.is_none() {
            match tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(addr)).await {
                Ok(Ok(s)) => {
                    let _ = s.set_nodelay(true);
                    conn = Some(BufWriter::new(s));
                }
                _ => continue,
            }
        }
        let w = conn.as_mut().expect("connected");
        let mut ok = write_frame(w, &env).await.is_ok();
        // Coalesce whatever else is queued into the same flush.
        while ok && let Ok(more) = rx.try_recv() {
            ok = write_frame(w, &more).await.is_ok();
        }
        if !ok || w.flush().await.is_err() {
            conn = None;
        }
    }
}

/// Where a proposer is waiting for its command's outcome.
type Reply = oneshot::Sender<Result<Vec<Event>, ProposeError>>;

struct Driver {
    raft: Raft<Command>,
    exchange: Exchange,
    storage: Storage<Command>,
    peers: BTreeMap<NodeId, mpsc::Sender<Envelope<Command>>>,
    cluster: BTreeMap<NodeId, PeerAddr>,
    waiters: BTreeMap<Index, (Term, Reply)>,
    events: Vec<Event>,
}

impl Driver {
    fn handle(&mut self, input: Input) {
        match input {
            Input::Message(env) => self.raft.step(env),
            Input::Propose(cmd, reply) => match self.raft.propose(cmd) {
                Ok((term, index)) => {
                    self.waiters.insert(index, (term, reply));
                }
                Err(e) => {
                    let leader_http = e.leader.and_then(|l| self.cluster.get(&l)).map(|p| p.http);
                    let _ = reply.send(Err(ProposeError::NotLeader {
                        leader: e.leader,
                        leader_http,
                    }));
                }
            },
            Input::Query(q, reply) => {
                let v = match q {
                    Query::Status => serde_json::to_value(NodeStatus {
                        raft: self.raft.status(),
                        applied_commands: self.exchange.applied(),
                        transcript_hash: format!("{:016x}", self.exchange.transcript_hash()),
                        trades: self.exchange.trades(),
                        resting_orders: self.exchange.resting_orders(),
                    }),
                    Query::Book(m) => serde_json::to_value(self.exchange.depth(m, 10)),
                    Query::Account(a) => serde_json::to_value(self.exchange.balances_of(a)),
                };
                let _ = reply.send(v.unwrap_or(serde_json::Value::Null));
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let ready = self.raft.ready();
        // Persist first: nothing may leave this node that depends on state
        // a crash could still take back.
        if let Some(hs) = ready.hard_state {
            self.storage.save_hard_state(hs)?;
        }
        if let Some(from) = ready.persist_from {
            self.storage.append(from, self.raft.log_from(from))?;
        }
        self.storage.sync()?;
        for env in ready.messages {
            if let Some(tx) = self.peers.get(&env.to) {
                let _ = tx.try_send(env);
            }
        }
        for (index, entry) in ready.committed {
            self.events.clear();
            if let Payload::Command(cmd) = &entry.payload {
                self.exchange.apply(cmd, &mut self.events);
            }
            // Resolve every waiter at or below this index. The one at this
            // index succeeded only if its own entry (same term) committed.
            while let Some(e) = self.waiters.first_entry() {
                if *e.key() > index {
                    break;
                }
                let (i, (term, reply)) = e.remove_entry();
                let result = if i == index && term == entry.term {
                    Ok(self.events.clone())
                } else {
                    Err(ProposeError::Superseded)
                };
                let _ = reply.send(result);
            }
        }
        // Waiters from a term this node no longer leads stay put: a new
        // leader may still commit those entries. The HTTP layer times out
        // if it never does.
        Ok(())
    }
}

pub async fn run_node(
    id: NodeId,
    cluster: BTreeMap<NodeId, PeerAddr>,
    data: PathBuf,
    tick: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let me = *cluster
        .get(&id)
        .ok_or("this node's id is not in --cluster")?;
    let (storage, hs, log) = Storage::open(&data)?;
    tracing::info!(
        id,
        term = hs.term,
        entries = log.len(),
        "recovered from disk"
    );
    let ids: Vec<NodeId> = cluster.keys().copied().collect();
    let raft = Raft::restore(
        id,
        ids,
        Config::default(),
        id ^ u64::from(std::process::id()),
        hs,
        log,
    );

    let (tx, mut rx) = mpsc::channel::<Input>(65_536);

    let mut peers = BTreeMap::new();
    for (&pid, addr) in &cluster {
        if pid != id {
            let (ptx, prx) = mpsc::channel(4_096);
            tokio::spawn(peer_writer(addr.raft, prx));
            peers.insert(pid, ptx);
        }
    }

    let listener = TcpListener::bind(me.raft).await?;
    let in_tx = tx.clone();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            let _ = s.set_nodelay(true);
            tokio::spawn(read_frames(s, in_tx.clone()));
        }
    });

    let app = Router::new()
        .route("/v1/status", get(status))
        .route("/v1/commands", post(command))
        .route("/v1/books/{market}", get(book))
        .route("/v1/accounts/{account}", get(account))
        .with_state(tx.clone());
    let http = TcpListener::bind(me.http).await?;
    tokio::spawn(async move {
        let _ = axum::serve(http, app).await;
    });
    tracing::info!(id, raft = %me.raft, http = %me.http, "node up");

    let mut driver = Driver {
        raft,
        exchange: Exchange::new(),
        storage,
        peers,
        cluster,
        waiters: BTreeMap::new(),
        events: Vec::new(),
    };
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_role = driver.raft.role();
    loop {
        tokio::select! {
            _ = ticker.tick() => driver.raft.tick(),
            input = rx.recv() => {
                let Some(input) = input else { return Ok(()) };
                driver.handle(input);
                for _ in 1..MAX_BATCH {
                    match rx.try_recv() {
                        Ok(i) => driver.handle(i),
                        Err(_) => break,
                    }
                }
            }
        }
        driver.flush()?;
        if driver.raft.role() != last_role {
            last_role = driver.raft.role();
            tracing::info!(id, role = ?last_role, term = driver.raft.term(), "role change");
        }
    }
}

type Tx = mpsc::Sender<Input>;

async fn query(tx: &Tx, q: Query) -> Response {
    let (reply, rx) = oneshot::channel();
    if tx.send(Input::Query(q, reply)).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match rx.await {
        Ok(serde_json::Value::Null) => StatusCode::NOT_FOUND.into_response(),
        Ok(v) => Json(v).into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn status(State(tx): State<Tx>) -> Response {
    query(&tx, Query::Status).await
}

async fn book(State(tx): State<Tx>, Path(market): Path<u32>) -> Response {
    query(&tx, Query::Book(market)).await
}

async fn account(State(tx): State<Tx>, Path(account): Path<u64>) -> Response {
    query(&tx, Query::Account(account)).await
}

/// Proposes a command and answers once it is committed and applied, with
/// the events it produced. Followers answer `421` with the leader's address.
async fn command(State(tx): State<Tx>, Json(cmd): Json<Command>) -> Response {
    let (reply, rx) = oneshot::channel();
    if tx.send(Input::Propose(cmd, reply)).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match tokio::time::timeout(Duration::from_secs(3), rx).await {
        Ok(Ok(Ok(events))) => Json(events).into_response(),
        Ok(Ok(Err(e @ ProposeError::NotLeader { .. }))) => {
            (StatusCode::MISDIRECTED_REQUEST, Json(e)).into_response()
        }
        Ok(Ok(Err(e))) => (StatusCode::CONFLICT, Json(e)).into_response(),
        Ok(Err(_)) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        // Outcome unknown: it may still commit. Clients retry idempotently.
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            "not committed within 3s; outcome unknown",
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cluster_spec() {
        let c = parse_cluster("1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002")
            .unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[&2].http.port(), 8002);
        assert!(parse_cluster("1=nope").is_err());
    }
}
