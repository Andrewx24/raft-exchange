# raft-exchange

A replicated spot exchange in Rust: **Raft written from scratch**, a price-time **matching engine**,
and **atomic settlement**. It's verified by **deterministic simulation** (the technique FoundationDB
and TigerBeetle use) and runs as a real multi-node cluster over Tokio TCP.

```
 clients ──HTTP──▶ leader ─────────────── AppendEntries (TCP, pipelined) ──────────▶ followers
                    │                                                                  │
                    │  propose → fsync log (group commit) → replicate → quorum ack      │ fsync → ack
                    ▼                                                                  ▼
             ┌─────────────┐   same committed log, same order, on every node     ┌─────────────┐
             │  Exchange   │ ◀───────────────────────────────────────────────────▶│  Exchange   │
             │ match+settle│   ⇒ same balances, same book, same transcript hash   │ match+settle│
             └─────────────┘                                                      └─────────────┘
```

Raft decides the **order** of commands. The exchange decides what they **mean**. The exchange is
deterministic: no clocks, no randomness, order ids assigned by the state machine, integer
arithmetic with overflow rejected. So every node that applies the same log reaches the same
balances, book and 64-bit transcript hash.

## What's in it

| Module | |
| --- | --- |
| [`raft`](src/raft.rs) | Raft with no I/O inside it ("sans-IO"). Time is `tick()`, input is `step()`/`propose()`, and output is a `Ready` the driver must persist *before* sending. Covers randomized elections, log replication with conflict-term backtracking, pipelined appends, the §5.4.2 commit rule, a no-op on election, and a commit index computed as the quorum-th match index. |
| [`book`](src/book.rs) | Limit order book. Orders live in a slab (an arena of reused slots) and each price level is an intrusive doubly linked list through it. Cancel is O(1), there's no per-order allocation, and price-time priority is just list order. Self-trade prevention cancels the resting order. |
| [`exchange`](src/exchange.rs) | The replicated state machine. Placing an order **locks** funds; a fill **moves** them in the same transition (trades print at the maker's price, and a buyer filled below its limit gets the difference back). Supports GTC, IOC and post-only. |
| [`sim`](src/sim.rs) | A whole cluster in one thread with one seeded RNG: message loss, duplication and reordering, partitions, crashes with disk-backed restarts, and client traffic. |
| [`storage`](src/storage.rs) | Hard state replaced atomically (write temp file, fsync, rename, fsync directory). The log is append-only, and a log tail Raft overwrites is replayed as truncation. A torn final line is dropped; a bad line anywhere else is an error. |
| [`node`](src/node.rs) | A real node. One driver task owns all state, there are no locks, inputs are batched into one fsync and one broadcast per loop, and peer queues are bounded and drop under pressure (Raft retransmits). |
| [`load`](src/load.rs) | A closed-loop load generator over raw keep-alive HTTP/1.1. |

## Deterministic simulation testing

```bash
cargo run --release -- sim --seeds 2000
# 2000 seeds passed in 39.9s: 2362946 commands applied, 648717 trades, 47976 leaders elected,
# 179866 crashes, 31908 partitions, 65895109 messages (4923594 dropped)
```

Each seed is one full cluster history. Seeds rotate through fault profiles (crash storms, long
partitions, 20% loss with heavy reordering) and append batch sizes from 1 to 256 entries. These
properties are checked **on every step**, not just at the end:

- **Election safety:** at most one leader per term.
- **Vote safety:** no node votes for two candidates in one term, *across crashes*. Checked on the
  wire, because it's the property that persisting the vote protects.
- **State machine safety:** no two nodes ever apply different entries at the same index.
- **Liveness:** once faults stop, the cluster converges within a bound.
- **Agreement:** every replica ends with the same transcript hash, and on every node every asset is
  conserved and every lock exactly covers its resting orders.

A failing seed prints a one-line reproduction command and fails the same way every time.

### Does the simulator actually catch bugs?

Five classic Raft bugs were planted, one at a time, and the sweep was run against each:

| Planted bug | Caught by |
| --- | --- |
| Commit an old-term entry by counting replicas (the paper's Figure 8) | the follower's "overwrite a committed entry" assertion |
| Grant votes without the log up-to-date check | state machine safety, seed 1 |
| Forget to persist the vote | vote safety, seed 75: *"node 2 voted for both 4 and 1 in term 69"* |
| Truncate on a stale append whose terms match | out-of-bounds crash on the corrupted log |
| Follower commits past what it has verified | state machine safety, seed 1 |

**The first version of the simulator missed two of these.** The Figure 8 bug never fired, because
a new leader's no-op always travelled in the same message as the entries before it. The vote
bug never fired, because crashes were too rare to land between voting and the next election. Fixes
were to randomize append batch sizes (so the no-op gets split off), rotate in crash-heavy profiles,
and check vote safety directly instead of waiting for it to cause two leaders. Before those fixes,
"2,000 seeds pass" sounded much stronger than it was.

The order book is also checked against a deliberately naive `Vec`-based reference engine over
2,000 random sequences of orders and cancels ([`tests/book_model.rs`](tests/book_model.rs)). Fills,
fill order and resting orders must match exactly.

## A real cluster

```bash
./scripts/demo.sh
```

The demo starts three processes, trades, **`kill -9`s the leader**, commits 50 more orders through
the failover, restarts the dead node from its disk, and checks all three replicas agree:

```
leader elected: node 3
killing leader node 3
new leader: node 1
50 more orders committed while node 3 was down
restarting node 3 from disk
all three replicas agree: "commit":62 "transcript_hash":"68779a405bd63a0a"
account 1: {"1":{"available":984975,"locked":4725},"2":{"available":100,"locked":0}}
```

The transcript hash is identical across runs even when a different node is elected and killed. The
balances check out by hand: 100 BTC bought for $10,300, 50 resting bids lock $4,725, and
$1,000,000 − $10,300 − $4,725 = $984,975.

### API

```bash
raft-exchange node --id 1 --data ./data/1 \
  --cluster 1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002,3=127.0.0.1:7003/127.0.0.1:8003

curl -X POST localhost:8001/v1/commands -H 'content-type: application/json' \
  -d '{"type":"place","account":1,"market":1,"side":"buy","price":102,"qty":30,"tif":"gtc"}'
curl localhost:8001/v1/status         # role, term, commit, transcript hash
curl localhost:8001/v1/books/1        # depth
curl localhost:8001/v1/accounts/1     # available / locked per asset
```

A command returns `200` with its events once it is **committed and applied**. A follower returns
`421` with the leader's address. A timeout returns `504`, because the outcome is genuinely unknown
(the entry may still commit).

## Performance

Measured on a shared 4-vCPU Xeon @ 2.1 GHz cloud container, with all three nodes on one machine.
Every commit is fsynced on a majority before it's acknowledged.

**Cluster, end to end** (`raft-exchange load`, HTTP → Raft → fsync ×3 → apply → response):

| Clients | Commits/s | p50 | p99 |
| ---: | ---: | ---: | ---: |
| 1 | 929 | 1.0 ms | 2.0 ms |
| 16 | 8,135 | 1.9 ms | 3.5 ms |
| 64 | 19,100 | 3.2 ms | 6.9 ms |
| 256 | 40,208 | 6.0 ms | 12.8 ms |

**Components** (`cargo bench`):

| | |
| --- | ---: |
| Book: insert + cancel, 10k orders deep | 86 ns |
| Book: aggressive order filling a level | 287 ns |
| Exchange: apply mixed order flow (match + settle + hash) | 752 ns |
| Raft, 3 nodes in memory: commit a batch of 1,024 | 411 µs (2.5M entries/s) |

### Two performance bugs, found by measuring

1. **Quadratic commit.** Batching made Raft *slower* per entry: 64 → 2.0M/s, but 1,024 → 296k/s.
   `maybe_commit` scanned every uncommitted index and counted acks for each, on every proposal and
   every ack. The commit point is just the quorum-th highest match index, which is one sort over the
   peers. That made batches of 1,024 **8.4× faster**, and the 2,000-seed sweep produced identical
   histories before and after, so the rewrite didn't change behavior.
2. **Throughput collapse under load.** The first real-cluster measurement *fell* from 4.6k/s at 16
   clients to 1.3k/s at 64 (p99 336 ms), and 256 clients dropped connections. The logs showed no
   leader churn, which ruled out the obvious suspect. The cause was that the leader only advanced a
   follower's `next_index` on acknowledgement, so every proposal re-sent the whole
   unacknowledged tail. Pipelining (advance on send, rewind on rejection) gave **15× at 64
   clients**, and 256 clients now reach 40k/s. The simulator confirmed it independently: the same
   scenarios need 3.6× fewer messages and commit 18% more.

Also worth saying: the very first cluster measurement, done with Python's `urllib`, topped out
at ~1.8k/s. A read-only endpoint got the same number, so that was measuring the client, which is
why [`load`](src/load.rs) exists.

## Not here (yet)

- **Snapshots / log compaction.** The log grows forever and restart replays all of it.
- **Membership changes, pre-vote, leader leases.**
- **Linearizable reads.** `GET` endpoints read the local node's state, which on a follower may be
  stale. A read-index round trip would fix that.
- **Client idempotency keys.** A `504` means "unknown outcome", and a blind retry could double-place.
- **Binary wire and log formats.** JSON is used throughout for readability.

## Tests

```bash
cargo test --release                       # unit, model-based book test, 200-seed sim sweep
SIM_SEEDS=5000 cargo test --release --test simulation
cargo run --release -- sim --seeds 10000
```

## License

MIT
