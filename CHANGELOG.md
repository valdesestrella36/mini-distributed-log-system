# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.0] - 2026-03-01
- Initial public MVP: framed JSON TCP protocol, append-only per-topic logs, SQLite metadata
- Idempotent produces via `message_id`
- Token-bucket rate limiting and backpressure (pause/resume)
- Basic metrics and configurable JSON/plain logging
- Tests: protocol, idempotence, backpressure, storage corruption, concurrency

## [0.2.0] - 2026-03-01
- Storage improvements: configurable `MDLS_FSYNC` durability option
- Log segmentation and rotation (`MDLS_SEGMENT_BYTES`)
- In-memory per-segment indices and recovery scan at startup
- End-to-end crash-recovery test (fsync behaviour)

## [0.3.0] - 2026-03-01
- Simple leader->follower replication (async replication RPC)
- Replication failure handling test

---
For release notes and tagging guidance see `RELEASES.md`.
