//! A minimal closed-loop load generator: `clients` connections, each with
//! one request in flight, over HTTP/1.1 keep-alive. Hand-rolled on a raw
//! `TcpStream` so the measurement is not bounded by a client library — the
//! first measurement of this cluster, made with Python, was really a
//! measurement of Python.

use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct Report {
    pub requests: u64,
    pub errors: u64,
    pub elapsed: Duration,
    pub latencies_us: Vec<u64>,
}

impl Report {
    pub fn percentile(&self, q: f64) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let i = ((self.latencies_us.len() as f64 * q) as usize).min(self.latencies_us.len() - 1);
        self.latencies_us[i] as f64 / 1000.0
    }
}

async fn request(conn: &mut BufReader<TcpStream>, host: &str, body: &str) -> std::io::Result<u16> {
    let req = format!(
        "POST /v1/commands HTTP/1.1\r\nhost: {host}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    conn.get_mut().write_all(req.as_bytes()).await?;
    let mut line = String::new();
    conn.read_line(&mut line).await?;
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut len = 0usize;
    loop {
        line.clear();
        if conn.read_line(&mut line).await? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let l = line.trim_end();
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':')
            && k.eq_ignore_ascii_case("content-length")
        {
            len = v.trim().parse().unwrap_or(0);
        }
    }
    let mut buf = vec![0; len];
    conn.read_exact(&mut buf).await?;
    Ok(status)
}

/// Runs `clients` concurrent loops issuing `per_client` commands each.
/// `body(client, i)` builds each request body.
pub async fn run(
    target: SocketAddr,
    clients: usize,
    per_client: usize,
    body: impl Fn(usize, usize) -> String + Send + Sync + 'static,
) -> std::io::Result<Report> {
    let body = std::sync::Arc::new(body);
    let host = target.to_string();
    let start = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..clients {
        let (body, host) = (body.clone(), host.clone());
        tasks.push(tokio::spawn(async move {
            let s = TcpStream::connect(target).await?;
            s.set_nodelay(true)?;
            let mut conn = BufReader::new(s);
            let (mut lat, mut errors) = (Vec::with_capacity(per_client), 0u64);
            for i in 0..per_client {
                let t = Instant::now();
                match request(&mut conn, &host, &body(c, i)).await? {
                    200 => lat.push(t.elapsed().as_micros() as u64),
                    _ => errors += 1,
                }
            }
            Ok::<_, std::io::Error>((lat, errors))
        }));
    }
    let mut report = Report {
        requests: 0,
        errors: 0,
        elapsed: Duration::ZERO,
        latencies_us: Vec::new(),
    };
    for t in tasks {
        let (lat, errors) = t.await.map_err(std::io::Error::other)??;
        report.requests += lat.len() as u64 + errors;
        report.errors += errors;
        report.latencies_us.extend(lat);
    }
    report.elapsed = start.elapsed();
    report.latencies_us.sort_unstable();
    Ok(report)
}
