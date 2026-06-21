# MiniKafka

A simplified Kafka-style message broker built from scratch in Go — topics, partitions, concurrent producers, and consumer groups with offset tracking.

This is a learning/portfolio project focused on backend systems engineering: concurrency, safe shared state, and the core mechanics of a distributed log, rather than wrapping an existing library.

## Status

🚧 Work in progress — built incrementally over 10 days.

- [x] Core `Partition` log with single-writer-goroutine concurrency model
- [x] `Topic` with key-based hash routing across partitions
- [x] Consumer groups with per-partition offset tracking ("resume where I left off")
- [x] Concurrency-safety verified with stress tests (50 concurrent producers, 5,000 messages)
- [ ] Write-ahead log (WAL) — persist messages to disk, survive restarts
- [ ] gRPC server — talk to the broker over the network instead of in-process
- [ ] Consumer group rebalancing on join/leave
- [ ] Benchmarks (throughput, latency)

## Why this design

**One writer goroutine per partition.** All appends to a partition's log go through a single goroutine, reached only via a channel — not a shared mutex on the hot write path. This avoids lock contention entirely and makes offset assignment trivially correct, since there's never a race between "read the last offset" and "append the next message." Reads are far more frequent than writes in typical pub/sub workloads, so they go straight to the in-memory log under an `RWMutex` instead of waiting behind the writer.

**Hash-based partition routing.** Messages are routed to a partition via `hash(key) % numPartitions`. This guarantees all messages sharing a key land on the same partition, preserving relative order for that key — the same ordering guarantee real Kafka makes.

**Consumer groups as a simple offset map.** A consumer group tracks the last committed offset per `(topic, partition)`. A consumer asks "where did I leave off?", reads forward from there, and commits its new position once done — this is the entire mechanism behind "at-least-once" delivery semantics.

## Project structure

```
broker/
  message.go         — core Message type
  partition.go        — concurrent, single-writer append-only log
  topic.go             — routes messages to partitions by key hash
  consumer_group.go   — tracks committed offsets per group
  partition_test.go   — concurrency stress tests
cmd/
  broker/main.go       — in-process demo of concurrent producers
  producer/main.go     — standalone producer CLI
  consumer/main.go     — standalone consumer CLI with offset tracking
proto/
  kafka.proto           — gRPC contract (not yet wired up — Day 5)

```
### Running it

```bash
go mod tidy

# Run the in-process concurrency demo
go run ./cmd/broker

# Run the producer (sends 10 messages)
go run ./cmd/producer

# Run the consumer (reads messages, tracks offsets)
go run ./cmd/consumer

# Run the concurrency stress tests
go test ./broker/... -v
```

## What's tested

`broker/partition_test.go` verifies:
- 50 concurrent producers writing 5,000 messages total never produce duplicate or skipped offsets
- Messages sharing the same key are always routed to the same partition

## Roadmap

| Day | Milestone |
|---|---|
| 1–2 | Core partition/topic engine, consumer groups, CLI producer/consumer ✅ |
| 3–4 | Write-ahead log persistence + crash recovery |
| 5–6 | gRPC server, multi-partition routing over the network, consumer group join/leave |
| 7 | Rebalancing logic |
| 8 | Concurrency hardening, load testing |
| 9–10 | Benchmarks, polish, documentation |