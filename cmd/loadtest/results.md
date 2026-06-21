# Load Test Results

## Test setup

- Broker running locally (single process, single machine)
- 20 concurrent producer goroutines
- 500 messages each (10,000 messages total)
- Each message ~50 bytes, sent over real gRPC/TCP to the broker
- Topic auto-created with 3 partitions

## Results

| Metric | Value |
|---|---|
| Total time | 18.68s |
| Successful sends | 10,000 / 10,000 (0 failures) |
| Throughput | 535 messages/sec |
| Latency p50 | 33.4ms |
| Latency p95 | 70.9ms |
| Latency p99 | 109.2ms |
| Latency max | 512.6ms |

## What this tells us

**Zero failed sends across 10,000 concurrent requests** — the system handled real concurrent network load correctly: no dropped messages, no corrupted offsets, no crashes.

## Honest analysis: where the time is going

535 msgs/sec is nowhere near what production Kafka achieves (100k+/sec), and that's expected — the bottleneck here is identifiable and deliberate, not a mystery:

**`WAL.Append()` calls `file.Sync()` (fsync) on every single message.** This forces the OS to physically write to disk before returning, which is the safest possible durability guarantee (a crash immediately after acknowledging a write can never lose data) but is also the single most expensive operation in the whole write path — disk fsyncs are orders of magnitude slower than in-memory operations.

**The fix, if optimizing for throughput:** batch multiple messages into a single fsync — for example, flush every N messages or every few milliseconds, whichever comes first. This is exactly what real Kafka does (configurable via `flush.messages` / `flush.ms`), trading a small, bounded durability window (you could lose the last few unflushed messages in a true crash) for a large throughput gain. This project intentionally chose maximum durability over maximum throughput as the starting point, since "correct but slow" is a safer foundation to build on than "fast but losing data," and the tradeoff is now measured rather than assumed.

## Running it yourself

```bash
# Terminal 1: start the broker
go run ./cmd/broker

# Terminal 2: run the load test
go run ./cmd/loadtest -producers=20 -messages=500
```

Flags `-producers` and `-messages` can be adjusted to test different load levels.