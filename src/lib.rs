//! A replicated spot exchange.
//!
//! - [`raft`]: Raft consensus, sans-IO
//! - [`book`]: price-time limit order book (slab + intrusive lists)
//! - [`exchange`]: the replicated state machine — matching and settlement
//! - [`sim`]: deterministic cluster simulation with fault injection
//! - [`storage`]: durable hard state and log for real nodes
//! - [`node`]: a real node: Tokio TCP transport, HTTP API
//! - [`load`]: keep-alive HTTP load generator for a running cluster

pub mod book;
pub mod exchange;
pub mod load;
pub mod node;
pub mod raft;
pub mod sim;
pub mod storage;
