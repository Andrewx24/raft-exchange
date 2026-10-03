//! The slab/linked-list book against a deliberately naive reference: a flat
//! `Vec` searched linearly. Any sequence of inserts, cancels and aggressive
//! orders must produce identical fills, in identical order, and identical
//! resting orders.

use proptest::prelude::*;
use raft_exchange::book::{Book, MatchEvent, Order, Side};

#[derive(Default)]
struct Naive {
    orders: Vec<(u64, Order)>, // (arrival seq, order)
    seq: u64,
}

impl Naive {
    fn insert(&mut self, o: Order) {
        self.seq += 1;
        self.orders.push((self.seq, o));
    }
    fn cancel(&mut self, id: u64) -> Option<Order> {
        let i = self.orders.iter().position(|(_, o)| o.id == id)?;
        Some(self.orders.remove(i).1)
    }
    fn execute(&mut self, t: &mut Order, out: &mut Vec<MatchEvent>) {
        while t.remaining > 0 {
            let best = self
                .orders
                .iter()
                .enumerate()
                .filter(|(_, (_, o))| o.side != t.side)
                .filter(|(_, (_, o))| match t.side {
                    Side::Buy => o.price <= t.price,
                    Side::Sell => o.price >= t.price,
                })
                .min_by_key(|(_, (seq, o))| match t.side {
                    Side::Buy => (o.price, *seq),
                    Side::Sell => (u64::MAX - o.price, *seq),
                })
                .map(|(i, _)| i);
            let Some(i) = best else { break };
            let maker = self.orders[i].1;
            if maker.account == t.account {
                self.orders.remove(i);
                out.push(MatchEvent::SelfTradeCancel { maker });
                continue;
            }
            let q = maker.remaining.min(t.remaining);
            t.remaining -= q;
            self.orders[i].1.remaining -= q;
            let after = self.orders[i].1;
            if after.remaining == 0 {
                self.orders.remove(i);
            }
            out.push(MatchEvent::Fill {
                maker: after,
                qty: q,
            });
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Place(u64, bool, u64, u64),
    Cancel(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u64..5, any::<bool>(), 95u64..106, 1u64..20).prop_map(|(a, b, p, q)| Op::Place(a, b, p, q)),
        1 => (1u64..200).prop_map(Op::Cancel),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2_000, ..ProptestConfig::default() })]

    #[test]
    fn book_matches_reference_model(ops in proptest::collection::vec(op(), 1..300)) {
        let (mut book, mut naive) = (Book::new(), Naive::default());
        let (mut a, mut b) = (Vec::new(), Vec::new());
        for (i, op) in ops.into_iter().enumerate() {
            match op {
                Op::Place(account, buy, price, qty) => {
                    let side = if buy { Side::Buy } else { Side::Sell };
                    let mut t1 = Order { id: i as u64 + 1, account, side, price, remaining: qty };
                    let mut t2 = t1;
                    a.clear();
                    b.clear();
                    book.execute(&mut t1, &mut a);
                    naive.execute(&mut t2, &mut b);
                    prop_assert_eq!(&a, &b);
                    prop_assert_eq!(t1, t2);
                    if t1.remaining > 0 {
                        book.insert(t1);
                        naive.insert(t2);
                    }
                }
                Op::Cancel(id) => prop_assert_eq!(book.cancel(id), naive.cancel(id)),
            }
            book.check().map_err(TestCaseError::fail)?;
        }
        let mut rest: Vec<Order> = book.orders().collect();
        let mut want: Vec<Order> = naive.orders.iter().map(|(_, o)| *o).collect();
        rest.sort_by_key(|o| o.id);
        want.sort_by_key(|o| o.id);
        prop_assert_eq!(rest, want);
    }
}
