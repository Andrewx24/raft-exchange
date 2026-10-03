//! Price-time priority limit order book.
//!
//! Layout is chosen for the operations a matching engine actually does:
//!
//! - **Orders live in a slab** (`Vec<Node>` + free list), addressed by `u32`.
//!   No per-order heap allocation, and freed slots are reused, so a book
//!   that churns millions of orders does not fragment.
//! - **Each price level is an intrusive doubly linked list** threaded
//!   through the slab. Time priority is list order; cancelling any order is
//!   O(1) unlink, not an O(level) scan of a `VecDeque`.
//! - **Levels sit in a `BTreeMap` per side**, so best price is the first or
//!   last key and a sweep walks prices in order.
//! - An `OrderId → slot` map makes cancel-by-id O(1). Its iteration order is
//!   never observed, so a `HashMap` does not break determinism.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub type OrderId = u64;
pub type AccountId = u64;
pub type Price = u64;
pub type Qty = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct Order {
    pub id: OrderId,
    pub account: AccountId,
    pub side: Side,
    pub price: Price,
    pub remaining: Qty,
}

/// What happened to resting orders while an incoming order matched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchEvent {
    /// `maker` traded `qty` at `maker.price`; `maker.remaining` is after.
    Fill { maker: Order, qty: Qty },
    /// Self-trade prevention: the taker's own resting order was cancelled
    /// rather than traded against.
    SelfTradeCancel { maker: Order },
}

const NIL: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
struct Node {
    order: Order,
    prev: u32,
    next: u32,
}

#[derive(Clone, Copy, Debug)]
struct Level {
    head: u32,
    tail: u32,
    qty: Qty,
    orders: u32,
}

#[derive(Clone, Debug, Default)]
pub struct Book {
    bids: BTreeMap<Price, Level>,
    asks: BTreeMap<Price, Level>,
    slab: Vec<Node>,
    free: Vec<u32>,
    index: HashMap<OrderId, u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct LevelView {
    pub price: Price,
    pub qty: Qty,
    pub orders: u32,
}

impl Book {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn get(&self, id: OrderId) -> Option<Order> {
        self.index.get(&id).map(|&s| self.slab[s as usize].order)
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    /// Would an order on `side` at `price` trade immediately?
    pub fn crosses(&self, side: Side, price: Price) -> bool {
        match side {
            Side::Buy => self.best_ask().is_some_and(|a| price >= a),
            Side::Sell => self.best_bid().is_some_and(|b| price <= b),
        }
    }

    fn alloc(&mut self, node: Node) -> u32 {
        match self.free.pop() {
            Some(s) => {
                self.slab[s as usize] = node;
                s
            }
            None => {
                self.slab.push(node);
                (self.slab.len() - 1) as u32
            }
        }
    }

    /// Adds a resting order at the back of its price level.
    pub fn insert(&mut self, order: Order) {
        debug_assert!(order.remaining > 0 && !self.index.contains_key(&order.id));
        let slot = self.alloc(Node {
            order,
            prev: NIL,
            next: NIL,
        });
        let levels = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = levels.entry(order.price).or_insert(Level {
            head: NIL,
            tail: NIL,
            qty: 0,
            orders: 0,
        });
        if level.tail == NIL {
            level.head = slot;
        } else {
            self.slab[level.tail as usize].next = slot;
            self.slab[slot as usize].prev = level.tail;
        }
        level.tail = slot;
        level.qty += order.remaining;
        level.orders += 1;
        self.index.insert(order.id, slot);
    }

    fn unlink(&mut self, slot: u32) -> Order {
        let Node { order, prev, next } = self.slab[slot as usize];
        let levels = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = levels.get_mut(&order.price).expect("order's level exists");
        if prev == NIL {
            level.head = next;
        } else {
            self.slab[prev as usize].next = next;
        }
        if next == NIL {
            level.tail = prev;
        } else {
            self.slab[next as usize].prev = prev;
        }
        level.qty -= order.remaining;
        level.orders -= 1;
        if level.orders == 0 {
            levels.remove(&order.price);
        }
        self.index.remove(&order.id);
        self.free.push(slot);
        order
    }

    /// Removes a resting order. O(1) apart from the level lookup.
    pub fn cancel(&mut self, id: OrderId) -> Option<Order> {
        let slot = *self.index.get(&id)?;
        Some(self.unlink(slot))
    }

    /// Matches `taker` against the opposite side while prices cross,
    /// best price first, oldest order first within a price. Reduces
    /// `taker.remaining`; the caller decides whether a remainder rests.
    pub fn execute(&mut self, taker: &mut Order, out: &mut Vec<MatchEvent>) {
        while taker.remaining > 0 {
            let best = match taker.side {
                Side::Buy => self
                    .asks
                    .first_key_value()
                    .filter(|(p, _)| **p <= taker.price),
                Side::Sell => self
                    .bids
                    .last_key_value()
                    .filter(|(p, _)| **p >= taker.price),
            };
            let Some((_, level)) = best else { break };
            let slot = level.head;
            let maker = self.slab[slot as usize].order;

            if maker.account == taker.account {
                // Cancel-resting STP: a trader can never be both sides of a
                // print, which would be wash trading and would also corrupt
                // the settlement accounting below.
                let maker = self.unlink(slot);
                out.push(MatchEvent::SelfTradeCancel { maker });
                continue;
            }

            let qty = maker.remaining.min(taker.remaining);
            taker.remaining -= qty;
            if qty == maker.remaining {
                let mut done = self.unlink(slot);
                done.remaining = 0;
                out.push(MatchEvent::Fill { maker: done, qty });
            } else {
                let node = &mut self.slab[slot as usize];
                node.order.remaining -= qty;
                let maker = node.order;
                let levels = match maker.side {
                    Side::Buy => &mut self.bids,
                    Side::Sell => &mut self.asks,
                };
                levels.get_mut(&maker.price).expect("level exists").qty -= qty;
                out.push(MatchEvent::Fill { maker, qty });
            }
        }
    }

    /// Top `n` levels per side, best first.
    pub fn depth(&self, n: usize) -> (Vec<LevelView>, Vec<LevelView>) {
        let view = |(p, l): (&Price, &Level)| LevelView {
            price: *p,
            qty: l.qty,
            orders: l.orders,
        };
        (
            self.bids.iter().rev().take(n).map(view).collect(),
            self.asks.iter().take(n).map(view).collect(),
        )
    }

    /// Resting orders in priority order, for invariant checks.
    pub fn orders(&self) -> impl Iterator<Item = Order> + '_ {
        self.bids
            .values()
            .rev()
            .chain(self.asks.values())
            .flat_map(move |l| {
                let mut s = l.head;
                std::iter::from_fn(move || {
                    (s != NIL).then(|| {
                        let n = self.slab[s as usize];
                        s = n.next;
                        n.order
                    })
                })
            })
    }

    /// Structural invariants: links, cached level totals, index.
    pub fn check(&self) -> Result<(), String> {
        let mut seen = 0usize;
        for (side, levels) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (price, l) in levels {
                let (mut s, mut prev, mut qty, mut n) = (l.head, NIL, 0, 0);
                while s != NIL {
                    let node = self.slab[s as usize];
                    if node.prev != prev || node.order.price != *price || node.order.side != side {
                        return Err(format!("bad link at price {price}"));
                    }
                    if node.order.remaining == 0 {
                        return Err(format!("empty order {} resting", node.order.id));
                    }
                    if self.index.get(&node.order.id) != Some(&s) {
                        return Err(format!("index mismatch for {}", node.order.id));
                    }
                    qty += node.order.remaining;
                    n += 1;
                    prev = s;
                    s = node.next;
                }
                if prev != l.tail || qty != l.qty || n != l.orders || n == 0 {
                    return Err(format!("level {price} totals stale"));
                }
                seen += n as usize;
            }
        }
        if let (Some(b), Some(a)) = (self.best_bid(), self.best_ask())
            && b >= a
        {
            return Err(format!("crossed book {b} >= {a}"));
        }
        if seen != self.index.len() {
            return Err("index holds orders not in any level".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(id: OrderId, account: AccountId, side: Side, price: Price, qty: Qty) -> Order {
        Order {
            id,
            account,
            side,
            price,
            remaining: qty,
        }
    }

    #[test]
    fn price_then_time_priority() {
        let mut b = Book::new();
        b.insert(o(1, 1, Side::Sell, 101, 5));
        b.insert(o(2, 2, Side::Sell, 100, 5));
        b.insert(o(3, 3, Side::Sell, 100, 5));
        let mut t = o(9, 9, Side::Buy, 101, 12);
        let mut out = Vec::new();
        b.execute(&mut t, &mut out);
        let fills: Vec<(OrderId, Qty)> = out
            .iter()
            .map(|e| match e {
                MatchEvent::Fill { maker, qty } => (maker.id, *qty),
                _ => panic!(),
            })
            .collect();
        // Best price first (100), oldest first within it (2 before 3).
        assert_eq!(fills, vec![(2, 5), (3, 5), (1, 2)]);
        assert_eq!(t.remaining, 0);
        assert_eq!(b.get(1).unwrap().remaining, 3);
        b.check().unwrap();
    }

    #[test]
    fn limit_price_is_respected() {
        let mut b = Book::new();
        b.insert(o(1, 1, Side::Sell, 105, 5));
        let mut t = o(2, 2, Side::Buy, 104, 5);
        let mut out = Vec::new();
        b.execute(&mut t, &mut out);
        assert!(out.is_empty());
        assert_eq!(t.remaining, 5);
    }

    #[test]
    fn cancel_from_the_middle_of_a_level() {
        let mut b = Book::new();
        for id in 1..=3 {
            b.insert(o(id, id, Side::Buy, 100, 1));
        }
        assert_eq!(b.cancel(2).unwrap().id, 2);
        b.check().unwrap();
        assert_eq!(b.orders().map(|o| o.id).collect::<Vec<_>>(), vec![1, 3]);
        assert_eq!(b.cancel(2), None);
        // Freed slot is reused.
        b.insert(o(4, 4, Side::Buy, 100, 1));
        assert_eq!(b.slab.len(), 3);
        b.check().unwrap();
    }

    #[test]
    fn self_trade_cancels_the_resting_order() {
        let mut b = Book::new();
        b.insert(o(1, 7, Side::Sell, 100, 5));
        b.insert(o(2, 8, Side::Sell, 100, 5));
        let mut t = o(3, 7, Side::Buy, 100, 5);
        let mut out = Vec::new();
        b.execute(&mut t, &mut out);
        assert!(matches!(out[0], MatchEvent::SelfTradeCancel { maker } if maker.id == 1));
        assert!(matches!(out[1], MatchEvent::Fill { maker, qty: 5 } if maker.id == 2));
        assert!(b.is_empty());
    }
}
