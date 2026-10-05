---
status: accepted
date: 2026-10-05
---

# 0018: Real-time push protocol

## Context and Problem Statement

The console resumes its stream after reconnecting, but sequence semantics were unspecified. An in-process counter resets when the API restarts, and Kafka offsets can't serve as a client sequence because every client sees a filtered view. Filtering per client in Python for 25k vehicles, encoding JSON at about 120 bytes per vehicle, would saturate both the API and mobile links. v1 defects 11 and 12 are the same class of bug (review findings 17, 18).

## Decision Outcome

- **Every frame carries `(epoch, seq)`.** The epoch is a random ID per API process, and seq increments once per batch tick. The client uses them only to *detect gaps*.
- **Any gap, epoch change or reconnect triggers a resync:** a fresh snapshot of the client's viewport plus all open alerts. There is no server-side delta replay buffer.
- **Binary columnar tile frames.** The server groups vehicles by map tile and encodes one frame per tile per tick, about 17 bytes per vehicle: id index, int32 coordinates in 1e-6 degrees, speed, heading, aspect flags, age. Each client receives the frames for its subscribed tiles, so encoding cost scales with updates, not with clients.
- **Level of detail:** above about 2,000 visible vehicles, the server sends density cells plus every non-Clear vehicle.
- **Each client has a bounded send queue;** overflow forces a resync. Kafka consumption runs in a thread.
- **Cadence:** 250 ms batches, or 1 s for phones and hidden tabs, which can also subscribe to alerts only.
- **Alerts** reach the push path only after projection (ADR-0006), and they carry per-alert versions for idempotent merging.
- **Console history:** a minute-resolution fleet history endpoint, read from `minute_series`, backs the time handle's full-fleet replay.
