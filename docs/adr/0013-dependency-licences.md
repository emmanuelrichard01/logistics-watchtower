---
status: accepted
date: 2026-10-05
---

# 0013: Dependency licences

## Context and Problem Statement

The core stack mixes open-source and source-available components. This is a portfolio project with no commercial deployment, but a licence surprise later would be expensive. Which licences apply, and what do they forbid?

## Decision Outcome

Accept the stack below for development and demonstration, with these caveats recorded.

| Component | Licence | Caveat |
| --- | --- | --- |
| Redpanda (broker, Schema Registry) | Business Source License 1.1 for the community edition | Source-available, not open source. It forbids offering Redpanda itself as a hosted streaming service; each release converts to Apache 2.0 after its change date. Fine for running the platform. Re-check before any commercial offering. |
| Redpanda Console | Source-available (assumed the same BSL terms as Redpanda; not re-verified) | Development UI only, never shipped. |
| **MinIO** | AGPL-3.0 | **Dropped.** The `minio/minio` GitHub repository is archived (last push 24 Apr 2026; last release Oct 2025), and community images are no longer published: `minio/minio` returns "object not found" on Docker Hub and `quay.io/minio/minio:latest` doesn't resolve (checked 5 Oct 2026). AGPL would also have obliged us to publish source for network use of a modified server. |
| SeaweedFS (chosen S3 store) | Apache-2.0 | None. A single `weed server -s3` container plus a one-shot bucket init. Compose names the service `objectstore`, and the archive code talks plain S3, so the store stays swappable. Garage (AGPL-3.0) was the alternative: also lightweight, but it brings back the AGPL question and needs a layout-assignment step at init. |
| PostgreSQL / PostGIS | PostgreSQL Licence / GPL-2.0-or-later | PostGIS is used as a server extension, not linked into our code, so the GPL doesn't reach the application. |
| OpenStreetMap data (Protomaps tiles, route polylines) | ODbL 1.0 | Attribution must be shown on every map. Publicly distributed derived databases must stay ODbL. The console and case-study page must carry the OSM attribution. |
| Python libraries (confluent-kafka, fastavro, SQLAlchemy, Alembic, Testcontainers) | Apache-2.0 / MIT | psycopg 3 is LGPL-3.0, used as a dynamically imported library, which is compatible. |

### Consequences

- Good: no copyleft obligation reaches Watchtower's own code.
- Good: the object store no longer depends on a vendor's image policy.
- Bad: "Parquet on MinIO" in plan sections 5 and 11 now reads as "Parquet on an S3-compatible store (SeaweedFS locally)". The plan text is left as written, and this ADR is the correction.
- Bad: Redpanda's BSL is a real restriction for anyone who wants to resell the platform as a service.

### Confirmation

Before adding any dependency, record its licence here or in a superseding ADR (plan section 20, risk "licence surprises").
