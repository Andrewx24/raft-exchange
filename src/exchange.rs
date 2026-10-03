//! The replicated state machine: a spot exchange with atomic settlement.
//!
//! Raft agrees on the *order* of commands; this module defines what they
//! *mean*. It must be a deterministic function of the command sequence, or
//! replicas that agree on the log will still disagree on balances. So: no
//! clocks, no randomness, order ids assigned by the state machine, integer
//! arithmetic with explicit overflow rejection, and a transcript hash over
//! every event so replicas can prove they agree.
//!
//! Settlement model: placing an order **locks** the funds it could need
//! (a buy locks `price × qty` of quote, a sell locks `qty` of base). A fill
//! moves locked funds between the two accounts in the same transition, so
//! there is never a moment where a trade has happened but has not settled.
//! A buy filled below its limit gets the difference unlocked. The
//! conservation invariant — per asset, the sum of every account's available
//! plus locked balance equals deposits minus withdrawals — is checked by the
//! simulator on every node after every run.

use crate::book::{AccountId, Book, LevelView, MatchEvent, Order, OrderId, Price, Qty, Side};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry as MapEntry;
use std::hash::{Hash, Hasher};

pub type AssetId = u32;
pub type MarketId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    /// Rest whatever does not fill immediately.
    Gtc,
    /// Fill what you can now, cancel the rest.
    Ioc,
    /// Reject rather than take liquidity.
    PostOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    CreateMarket {
        market: MarketId,
        base: AssetId,
        quote: AssetId,
    },
    Deposit {
        account: AccountId,
        asset: AssetId,
        amount: u64,
    },
    Withdraw {
        account: AccountId,
        asset: AssetId,
        amount: u64,
    },
    Place {
        account: AccountId,
        market: MarketId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
    },
    Cancel {
        account: AccountId,
        order: OrderId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    MarketCreated {
        market: MarketId,
    },
    Deposited {
        account: AccountId,
        asset: AssetId,
        amount: u64,
    },
    Withdrawn {
        account: AccountId,
        asset: AssetId,
        amount: u64,
    },
    Accepted {
        order: OrderId,
        account: AccountId,
        market: MarketId,
        side: Side,
        price: Price,
        qty: Qty,
    },
    Trade {
        market: MarketId,
        maker_order: OrderId,
        taker_order: OrderId,
        maker: AccountId,
        taker: AccountId,
        taker_side: Side,
        price: Price,
        qty: Qty,
    },
    Rested {
        order: OrderId,
        remaining: Qty,
    },
    Canceled {
        order: OrderId,
        remaining: Qty,
        reason: CancelReason,
    },
    Rejected {
        account: Option<AccountId>,
        reason: Reject,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    User,
    ImmediateOrCancel,
    SelfTrade,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reject {
    MarketExists,
    InvalidMarket,
    UnknownMarket,
    InvalidAmount,
    InsufficientFunds,
    Overflow,
    WouldTake,
    UnknownOrder,
    NotOwner,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Balance {
    pub available: u64,
    pub locked: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct MarketSpec {
    pub base: AssetId,
    pub quote: AssetId,
}

#[derive(Clone, Debug)]
struct Market {
    spec: MarketSpec,
    book: Book,
}

#[derive(Clone, Debug)]
pub struct Exchange {
    markets: BTreeMap<MarketId, Market>,
    balances: BTreeMap<(AccountId, AssetId), Balance>,
    /// Which market each resting order is in, for cancel-by-id.
    order_market: BTreeMap<OrderId, MarketId>,
    /// Net deposits per asset, for the conservation check.
    supply: BTreeMap<AssetId, u64>,
    next_order: OrderId,
    applied: u64,
    transcript: u64,
    trades: u64,
    scratch: Vec<MatchEvent>,
}

impl Default for Exchange {
    fn default() -> Self {
        Self::new()
    }
}

struct Fnv(u64);

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = (self.0 ^ *b as u64).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.write(&i.to_le_bytes());
    }
    fn write_u32(&mut self, i: u32) {
        self.write(&i.to_le_bytes());
    }
    fn write_usize(&mut self, i: usize) {
        self.write(&(i as u64).to_le_bytes());
    }
    fn write_isize(&mut self, i: isize) {
        self.write(&(i as i64).to_le_bytes());
    }
}

impl Exchange {
    pub fn new() -> Self {
        Exchange {
            markets: BTreeMap::new(),
            balances: BTreeMap::new(),
            order_market: BTreeMap::new(),
            supply: BTreeMap::new(),
            next_order: 1,
            applied: 0,
            transcript: 0xcbf2_9ce4_8422_2325,
            trades: 0,
            scratch: Vec::new(),
        }
    }

    /// Commands applied so far.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// Rolling hash over every event emitted. Equal on two replicas iff they
    /// produced identical event streams.
    pub fn transcript_hash(&self) -> u64 {
        self.transcript
    }

    pub fn trades(&self) -> u64 {
        self.trades
    }

    pub fn balance(&self, account: AccountId, asset: AssetId) -> Balance {
        self.balances
            .get(&(account, asset))
            .copied()
            .unwrap_or_default()
    }

    pub fn balances_of(&self, account: AccountId) -> BTreeMap<AssetId, Balance> {
        self.balances
            .range((account, 0)..=(account, AssetId::MAX))
            .map(|((_, a), b)| (*a, *b))
            .collect()
    }

    pub fn depth(&self, market: MarketId, n: usize) -> Option<(Vec<LevelView>, Vec<LevelView>)> {
        self.markets.get(&market).map(|m| m.book.depth(n))
    }

    pub fn resting_orders(&self) -> usize {
        self.markets.values().map(|m| m.book.len()).sum()
    }

    fn bal(&mut self, account: AccountId, asset: AssetId) -> &mut Balance {
        self.balances.entry((account, asset)).or_default()
    }

    /// Funds an order of `qty` at `price` on `side` must lock.
    fn lock_for(spec: MarketSpec, side: Side, price: Price, qty: Qty) -> Option<(AssetId, u64)> {
        match side {
            Side::Buy => price.checked_mul(qty).map(|n| (spec.quote, n)),
            Side::Sell => Some((spec.base, qty)),
        }
    }

    pub fn apply(&mut self, cmd: &Command, out: &mut Vec<Event>) {
        let start = out.len();
        match *cmd {
            Command::CreateMarket {
                market,
                base,
                quote,
            } => match self.markets.entry(market) {
                MapEntry::Occupied(_) => out.push(Event::Rejected {
                    account: None,
                    reason: Reject::MarketExists,
                }),
                MapEntry::Vacant(_) if base == quote => out.push(Event::Rejected {
                    account: None,
                    reason: Reject::InvalidMarket,
                }),
                MapEntry::Vacant(slot) => {
                    slot.insert(Market {
                        spec: MarketSpec { base, quote },
                        book: Book::new(),
                    });
                    out.push(Event::MarketCreated { market });
                }
            },
            Command::Deposit {
                account,
                asset,
                amount,
            } => self.deposit(account, asset, amount, out),
            Command::Withdraw {
                account,
                asset,
                amount,
            } => self.withdraw(account, asset, amount, out),
            Command::Place {
                account,
                market,
                side,
                price,
                qty,
                tif,
            } => self.place(account, market, side, price, qty, tif, out),
            Command::Cancel { account, order } => self.cancel(account, order, out),
        }
        self.applied += 1;
        let mut h = Fnv(self.transcript);
        self.applied.hash(&mut h);
        out[start..].hash(&mut h);
        self.transcript = h.finish();
    }

    fn deposit(&mut self, account: AccountId, asset: AssetId, amount: u64, out: &mut Vec<Event>) {
        let supply = self.supply.get(&asset).copied().unwrap_or(0);
        let (Some(new_supply), Some(new_avail)) = (
            supply.checked_add(amount),
            self.balance(account, asset).available.checked_add(amount),
        ) else {
            return out.push(Event::Rejected {
                account: Some(account),
                reason: Reject::Overflow,
            });
        };
        if amount == 0 {
            return out.push(Event::Rejected {
                account: Some(account),
                reason: Reject::InvalidAmount,
            });
        }
        self.supply.insert(asset, new_supply);
        self.bal(account, asset).available = new_avail;
        out.push(Event::Deposited {
            account,
            asset,
            amount,
        });
    }

    fn withdraw(&mut self, account: AccountId, asset: AssetId, amount: u64, out: &mut Vec<Event>) {
        if amount == 0 {
            return out.push(Event::Rejected {
                account: Some(account),
                reason: Reject::InvalidAmount,
            });
        }
        if self.balance(account, asset).available < amount {
            return out.push(Event::Rejected {
                account: Some(account),
                reason: Reject::InsufficientFunds,
            });
        }
        self.bal(account, asset).available -= amount;
        *self.supply.get_mut(&asset).expect("balance implies supply") -= amount;
        out.push(Event::Withdrawn {
            account,
            asset,
            amount,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn place(
        &mut self,
        account: AccountId,
        market_id: MarketId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        out: &mut Vec<Event>,
    ) {
        let reject = |out: &mut Vec<Event>, reason| {
            out.push(Event::Rejected {
                account: Some(account),
                reason,
            })
        };
        let Some(market) = self.markets.get(&market_id) else {
            return reject(out, Reject::UnknownMarket);
        };
        if price == 0 || qty == 0 {
            return reject(out, Reject::InvalidAmount);
        }
        let spec = market.spec;
        let Some((lock_asset, lock_amount)) = Self::lock_for(spec, side, price, qty) else {
            return reject(out, Reject::Overflow);
        };
        if tif == TimeInForce::PostOnly && market.book.crosses(side, price) {
            return reject(out, Reject::WouldTake);
        }
        if self.balance(account, lock_asset).available < lock_amount {
            return reject(out, Reject::InsufficientFunds);
        }
        let b = self.bal(account, lock_asset);
        b.available -= lock_amount;
        b.locked += lock_amount;

        let id = self.next_order;
        self.next_order += 1;
        out.push(Event::Accepted {
            order: id,
            account,
            market: market_id,
            side,
            price,
            qty,
        });

        let mut taker = Order {
            id,
            account,
            side,
            price,
            remaining: qty,
        };
        let mut events = std::mem::take(&mut self.scratch);
        events.clear();
        self.markets
            .get_mut(&market_id)
            .expect("checked")
            .book
            .execute(&mut taker, &mut events);

        for ev in &events {
            match *ev {
                MatchEvent::Fill { maker, qty } => {
                    self.settle(spec, &taker, &maker, qty);
                    if maker.remaining == 0 {
                        self.order_market.remove(&maker.id);
                    }
                    self.trades += 1;
                    out.push(Event::Trade {
                        market: market_id,
                        maker_order: maker.id,
                        taker_order: id,
                        maker: maker.account,
                        taker: account,
                        taker_side: side,
                        price: maker.price,
                        qty,
                    });
                }
                MatchEvent::SelfTradeCancel { maker } => {
                    self.release(spec, &maker);
                    self.order_market.remove(&maker.id);
                    out.push(Event::Canceled {
                        order: maker.id,
                        remaining: maker.remaining,
                        reason: CancelReason::SelfTrade,
                    });
                }
            }
        }
        self.scratch = events;

        if taker.remaining == 0 {
            return;
        }
        match tif {
            TimeInForce::Gtc | TimeInForce::PostOnly => {
                self.markets
                    .get_mut(&market_id)
                    .expect("checked")
                    .book
                    .insert(taker);
                self.order_market.insert(id, market_id);
                out.push(Event::Rested {
                    order: id,
                    remaining: taker.remaining,
                });
            }
            TimeInForce::Ioc => {
                self.release(spec, &taker);
                out.push(Event::Canceled {
                    order: id,
                    remaining: taker.remaining,
                    reason: CancelReason::ImmediateOrCancel,
                });
            }
        }
    }

    /// Moves funds for one fill. Trades print at the maker's price.
    fn settle(&mut self, spec: MarketSpec, taker: &Order, maker: &Order, qty: Qty) {
        let px = maker.price;
        // The maker's lock was sized at this same price, and the taker's at
        // a price at least this good, so neither product can overflow.
        let notional = px * qty;
        let (buyer, seller) = match taker.side {
            Side::Buy => (taker, maker),
            Side::Sell => (maker, taker),
        };
        // Buyer: pays notional out of locked quote. A taker buy locked at its
        // own limit, so the improvement (limit − px) × qty is released.
        let buyer_locked_at = buyer.price * qty;
        let b = self.bal(buyer.account, spec.quote);
        b.locked -= buyer_locked_at;
        b.available += buyer_locked_at - notional;
        self.bal(buyer.account, spec.base).available += qty;
        // Seller: delivers base out of locked, receives quote.
        self.bal(seller.account, spec.base).locked -= qty;
        self.bal(seller.account, spec.quote).available += notional;
    }

    /// Returns a dead order's remaining lock to available.
    fn release(&mut self, spec: MarketSpec, order: &Order) {
        let (asset, amount) = Self::lock_for(spec, order.side, order.price, order.remaining)
            .expect("locked without overflow");
        let b = self.bal(order.account, asset);
        b.locked -= amount;
        b.available += amount;
    }

    fn cancel(&mut self, account: AccountId, order: OrderId, out: &mut Vec<Event>) {
        let Some(&market_id) = self.order_market.get(&order) else {
            return out.push(Event::Rejected {
                account: Some(account),
                reason: Reject::UnknownOrder,
            });
        };
        let market = self
            .markets
            .get_mut(&market_id)
            .expect("indexed market exists");
        if market.book.get(order).is_some_and(|o| o.account != account) {
            return out.push(Event::Rejected {
                account: Some(account),
                reason: Reject::NotOwner,
            });
        }
        let spec = market.spec;
        let o = market.book.cancel(order).expect("indexed order rests");
        self.order_market.remove(&order);
        self.release(spec, &o);
        out.push(Event::Canceled {
            order,
            remaining: o.remaining,
            reason: CancelReason::User,
        });
    }

    /// Conservation of every asset, locks that exactly cover resting orders,
    /// and structurally sound books.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut held: BTreeMap<AssetId, u128> = BTreeMap::new();
        for ((_, asset), b) in &self.balances {
            *held.entry(*asset).or_default() += b.available as u128 + b.locked as u128;
        }
        for (asset, supply) in &self.supply {
            let h = held.get(asset).copied().unwrap_or(0);
            if h != *supply as u128 {
                return Err(format!(
                    "asset {asset}: balances sum to {h}, supply is {supply}"
                ));
            }
        }
        let mut expected_locks: BTreeMap<(AccountId, AssetId), u64> = BTreeMap::new();
        for (id, m) in &self.markets {
            m.book.check().map_err(|e| format!("market {id}: {e}"))?;
            for o in m.book.orders() {
                let (asset, amount) =
                    Self::lock_for(m.spec, o.side, o.price, o.remaining).ok_or("lock overflow")?;
                *expected_locks.entry((o.account, asset)).or_default() += amount;
                if self.order_market.get(&o.id) != Some(id) {
                    return Err(format!("order {} missing from market index", o.id));
                }
            }
        }
        for (key, b) in &self.balances {
            let want = expected_locks.get(key).copied().unwrap_or(0);
            if b.locked != want {
                return Err(format!(
                    "{key:?}: locked {} but resting orders need {want}",
                    b.locked
                ));
            }
        }
        if self.order_market.len() != self.resting_orders() {
            return Err("market index holds orders that are not resting".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USD: AssetId = 1;
    const BTC: AssetId = 2;
    const M: MarketId = 1;

    fn run(x: &mut Exchange, cmds: &[Command]) -> Vec<Event> {
        let mut out = Vec::new();
        for c in cmds {
            x.apply(c, &mut out);
            x.check_invariants().unwrap();
        }
        out
    }

    fn setup() -> Exchange {
        let mut x = Exchange::new();
        run(
            &mut x,
            &[
                Command::CreateMarket {
                    market: M,
                    base: BTC,
                    quote: USD,
                },
                Command::Deposit {
                    account: 1,
                    asset: USD,
                    amount: 1_000_000,
                },
                Command::Deposit {
                    account: 2,
                    asset: BTC,
                    amount: 100,
                },
            ],
        );
        x
    }

    fn place(account: AccountId, side: Side, price: Price, qty: Qty, tif: TimeInForce) -> Command {
        Command::Place {
            account,
            market: M,
            side,
            price,
            qty,
            tif,
        }
    }

    #[test]
    fn a_cross_settles_atomically_at_the_maker_price() {
        let mut x = setup();
        run(&mut x, &[place(2, Side::Sell, 100, 10, TimeInForce::Gtc)]);
        assert_eq!(
            x.balance(2, BTC),
            Balance {
                available: 90,
                locked: 10
            }
        );
        // Buyer bids 105 for 4: locks 420, trades at 100, gets 20 back.
        let ev = run(&mut x, &[place(1, Side::Buy, 105, 4, TimeInForce::Gtc)]);
        assert!(ev.iter().any(|e| matches!(
            e,
            Event::Trade {
                price: 100,
                qty: 4,
                ..
            }
        )));
        assert_eq!(
            x.balance(1, USD),
            Balance {
                available: 1_000_000 - 400,
                locked: 0
            }
        );
        assert_eq!(x.balance(1, BTC).available, 4);
        assert_eq!(x.balance(2, USD).available, 400);
        assert_eq!(
            x.balance(2, BTC),
            Balance {
                available: 90,
                locked: 6
            }
        );
    }

    #[test]
    fn insufficient_funds_rejects_without_side_effects() {
        let mut x = setup();
        let before = x.balance(1, USD);
        let ev = run(
            &mut x,
            &[place(1, Side::Buy, 1_000_001, 1, TimeInForce::Gtc)],
        );
        assert!(matches!(
            ev[0],
            Event::Rejected {
                reason: Reject::InsufficientFunds,
                ..
            }
        ));
        assert_eq!(x.balance(1, USD), before);
    }

    #[test]
    fn ioc_remainder_is_released_and_post_only_never_takes() {
        let mut x = setup();
        run(&mut x, &[place(2, Side::Sell, 100, 3, TimeInForce::Gtc)]);
        let ev = run(&mut x, &[place(1, Side::Buy, 100, 5, TimeInForce::Ioc)]);
        assert!(ev.iter().any(|e| matches!(
            e,
            Event::Canceled {
                remaining: 2,
                reason: CancelReason::ImmediateOrCancel,
                ..
            }
        )));
        assert_eq!(x.balance(1, USD).locked, 0);
        run(&mut x, &[place(2, Side::Sell, 101, 1, TimeInForce::Gtc)]);
        let ev = run(
            &mut x,
            &[place(1, Side::Buy, 101, 1, TimeInForce::PostOnly)],
        );
        assert!(matches!(
            ev[0],
            Event::Rejected {
                reason: Reject::WouldTake,
                ..
            }
        ));
    }

    #[test]
    fn only_the_owner_can_cancel() {
        let mut x = setup();
        run(&mut x, &[place(2, Side::Sell, 100, 3, TimeInForce::Gtc)]);
        let ev = run(
            &mut x,
            &[Command::Cancel {
                account: 1,
                order: 1,
            }],
        );
        assert!(matches!(
            ev[0],
            Event::Rejected {
                reason: Reject::NotOwner,
                ..
            }
        ));
        let ev = run(
            &mut x,
            &[Command::Cancel {
                account: 2,
                order: 1,
            }],
        );
        assert!(matches!(ev[0], Event::Canceled { remaining: 3, .. }));
        assert_eq!(
            x.balance(2, BTC),
            Balance {
                available: 100,
                locked: 0
            }
        );
    }

    #[test]
    fn overflowing_notional_is_rejected() {
        let mut x = setup();
        let ev = run(
            &mut x,
            &[place(1, Side::Buy, u64::MAX, 2, TimeInForce::Gtc)],
        );
        assert!(matches!(
            ev[0],
            Event::Rejected {
                reason: Reject::Overflow,
                ..
            }
        ));
    }
}
