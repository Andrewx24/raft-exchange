use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use raft_exchange::book::{Book, MatchEvent, Order, Side};
use raft_exchange::exchange::{Command, Exchange, TimeInForce};
use raft_exchange::raft::{Config, Raft, Role};
use raft_exchange::sim::Rng;
use std::hint::black_box;

/// A book with `n` resting orders per side spread over 50 price levels.
fn deep_book(n: u64) -> Book {
    let mut b = Book::new();
    for i in 0..n {
        b.insert(Order {
            id: 2 * i + 1,
            account: i % 100,
            side: Side::Buy,
            price: 9_950 + i % 50,
            remaining: 10,
        });
        b.insert(Order {
            id: 2 * i + 2,
            account: i % 100,
            side: Side::Sell,
            price: 10_001 + i % 50,
            remaining: 10,
        });
    }
    b
}

fn book(c: &mut Criterion) {
    let mut g = c.benchmark_group("book");
    g.throughput(Throughput::Elements(1));

    let mut b = deep_book(10_000);
    let mut id = 1_000_000u64;
    g.bench_function("insert_then_cancel_10k_deep", |bench| {
        bench.iter(|| {
            id += 1;
            b.insert(Order {
                id,
                account: 7,
                side: Side::Buy,
                price: 9_975,
                remaining: 5,
            });
            black_box(b.cancel(id));
        })
    });

    let mut out = Vec::with_capacity(64);
    g.bench_function("aggressive_order_fills_one_level", |bench| {
        bench.iter_batched_ref(
            || deep_book(1_000),
            |b| {
                out.clear();
                let mut t = Order {
                    id: u64::MAX,
                    account: 999,
                    side: Side::Buy,
                    price: 10_001,
                    remaining: 50,
                };
                b.execute(&mut t, &mut out);
                black_box(&out);
            },
            BatchSize::LargeInput,
        )
    });
    g.finish();
}

fn exchange(c: &mut Criterion) {
    // A realistic mixed stream: mostly passive limits around a mid, some
    // crossing, some cancels.
    let mut x = Exchange::new();
    let mut out = Vec::new();
    x.apply(
        &Command::CreateMarket {
            market: 1,
            base: 2,
            quote: 1,
        },
        &mut out,
    );
    for a in 1..=100 {
        x.apply(
            &Command::Deposit {
                account: a,
                asset: 1,
                amount: u64::MAX / 1_000,
            },
            &mut out,
        );
        x.apply(
            &Command::Deposit {
                account: a,
                asset: 2,
                amount: u64::MAX / 1_000,
            },
            &mut out,
        );
    }
    let mut rng = Rng::new(7);
    let mut g = c.benchmark_group("exchange");
    g.throughput(Throughput::Elements(1));
    g.bench_function("apply_mixed_order_flow", |bench| {
        bench.iter(|| {
            out.clear();
            let account = 1 + rng.below(100);
            let cmd = if rng.below(10) == 0 {
                Command::Cancel {
                    account,
                    order: 1 + rng.below(x.applied()),
                }
            } else {
                let buy = rng.below(2) == 0;
                Command::Place {
                    account,
                    market: 1,
                    side: if buy { Side::Buy } else { Side::Sell },
                    price: if buy {
                        9_990 + rng.below(12)
                    } else {
                        9_999 + rng.below(12)
                    },
                    qty: 1 + rng.below(10),
                    tif: TimeInForce::Gtc,
                }
            };
            x.apply(black_box(&cmd), &mut out);
        })
    });
    g.finish();
}

/// Consensus overhead with the network and disk taken out: three in-memory
/// nodes, a batch of proposals, pumped until the batch is committed.
fn raft(c: &mut Criterion) {
    let mut g = c.benchmark_group("raft_3_nodes_in_memory");
    for batch in [1u64, 64, 1024] {
        let ids = vec![1, 2, 3];
        let mut nodes: Vec<Raft<u64>> = ids
            .iter()
            .map(|&i| Raft::new(i, ids.clone(), Config::default(), 1))
            .collect();
        while nodes[0].role() != Role::Leader {
            nodes[0].tick();
            pump(&mut nodes);
        }
        g.throughput(Throughput::Elements(batch));
        g.bench_function(format!("commit_batch_of_{batch}"), |bench| {
            bench.iter(|| {
                let target = nodes[0].last_index() + batch;
                for i in 0..batch {
                    nodes[0].propose(i).unwrap();
                }
                while nodes[0].commit_index() < target {
                    pump(&mut nodes);
                }
            })
        });
    }
    g.finish();
}

fn pump(nodes: &mut [Raft<u64>]) {
    let mut msgs = Vec::new();
    for n in nodes.iter_mut() {
        msgs.extend(n.ready().messages);
    }
    for m in msgs {
        nodes[(m.to - 1) as usize].step(m);
    }
}

fn _unused(_: MatchEvent) {}

criterion_group!(benches, book, exchange, raft);
criterion_main!(benches);
