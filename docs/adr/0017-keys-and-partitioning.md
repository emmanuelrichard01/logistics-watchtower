---
status: accepted
date: 2026-10-05
---

# 0017: Keys and partitioning

## Context and Problem Statement

- Risk was keyed by shipment while state was keyed by vehicle, so one shipment's exposure could span partitions.
- librdkafka's default partitioner (CRC32) differs from the murmur2 used by Java and Redpanda tooling. Any producer using a different partitioner silently splits a vehicle's records across partitions.
- Vehicle IDs mixed text and UUIDs.

(Review findings 9, 11, 30.)

## Decision Outcome

- **`vehicle_id` (text, for example `TRK-101`) keys every topic,** including risk, alerts and `fleet.state`. `shipment_id` travels in the payload.
- **Exposure is computed per (shipment, vehicle leg)** and summed in the projection and in Gold.
- **One producer factory** in `packages/platform` pins `partitioner=murmur2_random`, `enable.idempotence=true` and `acks=all`. A test asserts the configuration.
- **12 partitions** on `wt.input.v1` and on the vehicle-keyed outputs.
- **`fleet.state.v1`** is compacted with `segment.ms` set to 10 minutes and throttled to state changes or one update per 5 s per vehicle.
- **`org_id`** lives in payloads and in Postgres unique indexes, never in Kafka keys.
