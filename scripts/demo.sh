#!/usr/bin/env bash
# Three real nodes over TCP: trade, kill the leader, keep trading through the
# failover, restart the dead node from disk, and check every replica agrees.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=target/release/raft-exchange
DATA=${DATA:-$(mktemp -d)}
CLUSTER="1=127.0.0.1:7101/127.0.0.1:8101,2=127.0.0.1:7102/127.0.0.1:8102,3=127.0.0.1:7103/127.0.0.1:8103"
declare -A PID

cargo build --release -q
start() { RUST_LOG=warn $BIN node --id "$1" --cluster "$CLUSTER" --data "$DATA/$1" & PID[$1]=$!; }
http() { echo "127.0.0.1:810$1"; }
cleanup() { kill "${PID[@]}" 2>/dev/null || true; }
trap cleanup EXIT

leader() {
  for _ in $(seq 1 100); do
    for i in 1 2 3; do
      if curl -sf "$(http "$i")/v1/status" 2>/dev/null | grep -q '"role":"Leader"'; then echo "$i"; return; fi
    done
    sleep 0.1
  done
  echo "no leader elected" >&2; exit 1
}

# Submit to the leader; on 421 (not leader) find the new one and retry.
submit() {
  for _ in $(seq 1 20); do
    local l; l=$(leader)
    local out code
    out=$(curl -s -w '\n%{http_code}' -X POST "$(http "$l")/v1/commands" -H 'content-type: application/json' -d "$1") || true
    code=${out##*$'\n'}
    if [ "$code" = 200 ]; then echo "${out%$'\n'*}"; return; fi
    sleep 0.2
  done
  echo "command never committed: $1" >&2; exit 1
}

for i in 1 2 3; do start "$i"; done
L=$(leader); echo "leader elected: node $L"

submit '{"type":"create_market","market":1,"base":2,"quote":1}' >/dev/null
submit '{"type":"deposit","account":1,"asset":1,"amount":1000000}' >/dev/null
submit '{"type":"deposit","account":2,"asset":2,"amount":500}' >/dev/null
for p in 101 102 103 104 105; do
  submit "{\"type\":\"place\",\"account\":2,\"market\":1,\"side\":\"sell\",\"price\":$p,\"qty\":20,\"tif\":\"gtc\"}" >/dev/null
done
echo "buy 30 @ 102 ->" "$(submit '{"type":"place","account":1,"market":1,"side":"buy","price":102,"qty":30,"tif":"gtc"}' | grep -o '"type":"trade"[^}]*' | tr '\n' ' ')"

echo "killing leader node $L"
kill -9 "${PID[$L]}"; unset "PID[$L]"
NEW=$(leader); echo "new leader: node $NEW"

for i in $(seq 1 50); do
  submit "{\"type\":\"place\",\"account\":1,\"market\":1,\"side\":\"buy\",\"price\":$((90 + i % 10)),\"qty\":1,\"tif\":\"gtc\"}" >/dev/null
done
echo "50 more orders committed while node $L was down"
echo "sweep -> $(submit '{"type":"place","account":1,"market":1,"side":"buy","price":105,"qty":100,"tif":"ioc"}' | grep -o '"type":"trade"' | wc -l) trades"

echo "restarting node $L from disk"
start "$L"
for _ in $(seq 1 100); do
  s1=$(curl -sf "$(http 1)/v1/status" || true); s2=$(curl -sf "$(http 2)/v1/status" || true); s3=$(curl -sf "$(http 3)/v1/status" || true)
  h() { echo "$1" | grep -o '"transcript_hash":"[0-9a-f]*"'; }
  c() { echo "$1" | grep -o '"commit":[0-9]*'; }
  if [ -n "$(h "$s1")" ] && [ "$(h "$s1")" = "$(h "$s2")" ] && [ "$(h "$s2")" = "$(h "$s3")" ] && [ "$(c "$s1")" = "$(c "$s3")" ] && [ "$(c "$s1")" = "$(c "$s2")" ]; then
    echo "all three replicas agree: $(c "$s1") $(h "$s1")"
    echo "book: $(curl -s "$(http "$L")/v1/books/1")"
    echo "account 1: $(curl -s "$(http "$L")/v1/accounts/1")"
    exit 0
  fi
  sleep 0.1
done
echo "replicas did not converge" >&2
for i in 1 2 3; do curl -s "$(http "$i")/v1/status"; echo; done
exit 1
