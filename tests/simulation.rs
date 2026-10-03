//! A CI-sized sweep of the deterministic cluster simulation. Every seed
//! rotates through fault profiles (crash storms, long partitions, 20% loss
//! with heavy reordering) and append batch sizes. Run the full sweep with
//! `cargo run --release -- sim --seeds 10000`.

use raft_exchange::sim::{Sim, SimConfig};

#[test]
fn every_seed_preserves_safety_and_converges() {
    let seeds: u64 = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    for seed in 0..seeds {
        if let Err(e) = Sim::new(seed, SimConfig::for_seed(seed)).run() {
            panic!(
                "seed {seed}: {e}\nreproduce: cargo run --release -- sim --seed {seed} --seeds 1"
            );
        }
    }
}
