use raft_exchange::node::{parse_cluster, run_node};
use raft_exchange::sim::{Sim, SimConfig};
use std::time::{Duration, Instant};

const USAGE: &str = "\
raft-exchange node --id N --cluster SPEC --data DIR [--tick-ms 10]
    SPEC: 1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002,...
          (raft address / http address per node)

raft-exchange load --target HOST:PORT [--clients 64] [--requests 20000]
    Closed-loop load against a node's HTTP API (point it at the leader).

raft-exchange sim [--seed S] [--seeds N] [--nodes K] [--ticks T]
    Runs N deterministic simulations from seed S with faults injected and
    every safety property checked. Exits non-zero with the failing seed.";

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn parse<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    flag(args, name).map_or(default, |v| {
        v.parse()
            .unwrap_or_else(|_| die(&format!("bad value for {name}")))
    })
}

fn die(msg: &str) -> ! {
    eprintln!("error: {msg}\n\n{USAGE}");
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("node") => {
            let id = parse(&args, "--id", 0u64);
            let cluster = parse_cluster(
                flag(&args, "--cluster").unwrap_or_else(|| die("--cluster is required")),
            )
            .unwrap_or_else(|e| die(&e));
            let data = flag(&args, "--data")
                .unwrap_or_else(|| die("--data is required"))
                .into();
            let tick = Duration::from_millis(parse(&args, "--tick-ms", 10u64));
            if let Err(e) = run_node(id, cluster, data, tick).await {
                eprintln!("node failed: {e}");
                std::process::exit(1);
            }
        }
        Some("sim") => {
            let seed = parse(&args, "--seed", 1u64);
            let seeds = parse(&args, "--seeds", 100u64);
            let nodes = parse(&args, "--nodes", 5u64);
            let ticks = parse(&args, "--ticks", 3_000u64);
            let start = Instant::now();
            let mut total = raft_exchange::sim::SimStats::default();
            for s in seed..seed + seeds {
                let cfg = SimConfig {
                    nodes,
                    ticks,
                    ..SimConfig::for_seed(s)
                };
                match Sim::new(s, cfg).run() {
                    Ok(st) => {
                        total.applied_commands += st.applied_commands;
                        total.trades += st.trades;
                        total.leaders_elected += st.leaders_elected;
                        total.crashes += st.crashes;
                        total.partitions += st.partitions;
                        total.messages += st.messages;
                        total.dropped += st.dropped;
                    }
                    Err(e) => {
                        eprintln!("seed {s} FAILED: {e}");
                        eprintln!(
                            "reproduce: raft-exchange sim --seed {s} --seeds 1 --nodes {nodes} --ticks {ticks}"
                        );
                        std::process::exit(1);
                    }
                }
            }
            println!(
                "{seeds} seeds passed in {:.1?}: {} commands applied, {} trades, {} leaders elected, \
                 {} crashes, {} partitions, {} messages ({} dropped)",
                start.elapsed(),
                total.applied_commands,
                total.trades,
                total.leaders_elected,
                total.crashes,
                total.partitions,
                total.messages,
                total.dropped
            );
        }
        Some("load") => {
            let target = flag(&args, "--target")
                .unwrap_or_else(|| die("--target is required"))
                .parse()
                .unwrap_or_else(|_| die("--target must be HOST:PORT"));
            let clients = parse(&args, "--clients", 64usize);
            let total = parse(&args, "--requests", 20_000usize);
            // Every client gets its own account so orders cross between
            // accounts rather than self-trade.
            let setup = |c: usize| {
                vec![
                    r#"{"type":"create_market","market":1,"base":2,"quote":1}"#.to_string(),
                    format!(
                        r#"{{"type":"deposit","account":{c},"asset":1,"amount":1000000000000}}"#
                    ),
                    format!(
                        r#"{{"type":"deposit","account":{c},"asset":2,"amount":1000000000000}}"#
                    ),
                ]
            };
            let warm =
                raft_exchange::load::run(target, clients, 3, move |c, i| setup(c + 1)[i].clone())
                    .await;
            if let Err(e) = warm {
                die(&format!("cannot reach {target}: {e}"));
            }
            let r = raft_exchange::load::run(target, clients, total / clients, |c, i| {
                let side = if (c + i) % 2 == 0 { "buy" } else { "sell" };
                let price = 100 + (c * 7 + i * 13) % 5;
                format!(
                    r#"{{"type":"place","account":{},"market":1,"side":"{side}","price":{price},"qty":1,"tif":"gtc"}}"#,
                    c + 1
                )
            })
            .await
            .unwrap_or_else(|e| die(&format!("load failed: {e}")));
            println!(
                "{} commands in {:.2?} with {clients} clients: {:.0} commits/s, p50 {:.2} ms, p99 {:.2} ms, p99.9 {:.2} ms, {} errors",
                r.requests,
                r.elapsed,
                r.requests as f64 / r.elapsed.as_secs_f64(),
                r.percentile(0.5),
                r.percentile(0.99),
                r.percentile(0.999),
                r.errors
            );
        }
        _ => die("expected a subcommand"),
    }
}
