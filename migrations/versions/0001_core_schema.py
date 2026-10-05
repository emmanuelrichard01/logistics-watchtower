"""Core schema (plan section 6).

Postgres holds what must be durable and transactional. Raw telemetry never lands
here; it goes to the Parquet archive.

Roles: the migration creates `watchtower_app` (NOLOGIN) as the privilege set for
services. Services log in as a member of it, never as the table owner, because
the owner bypasses the append-only revokes below.

Revision ID: 0001
Revises:
Create Date: 2026-10-05
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

APP_ROLE = "watchtower_app"

UPGRADE = """
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE organizations (
  id          uuid PRIMARY KEY,
  name        text NOT NULL UNIQUE,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Vehicles use their natural key ('TRK-101') everywhere: topic keys, dedup keys,
-- the API and the UI all speak it, so no surrogate UUID exists to translate.
CREATE TABLE vehicles (
  vehicle_id  text PRIMARY KEY CHECK (vehicle_id ~ '^[A-Za-z0-9._-]{1,64}$'),
  org_id      uuid NOT NULL REFERENCES organizations(id),
  device_id   text NOT NULL UNIQUE,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sensors (
  id                    uuid PRIMARY KEY,
  org_id                uuid NOT NULL REFERENCES organizations(id),
  vehicle_id            text NOT NULL REFERENCES vehicles(vehicle_id),
  probe                 text NOT NULL CHECK (probe IN ('SUPPLY_AIR', 'RETURN_AIR', 'CARGO', 'AMBIENT')),
  calibration_offset_c  double precision NOT NULL DEFAULT 0,
  last_calibrated_at    timestamptz,
  UNIQUE (vehicle_id, probe)
);

-- Limits are data, never code. A profile without a source is illustrative only.
CREATE TABLE cargo_profiles (
  id                          uuid PRIMARY KEY,
  org_id                      uuid NOT NULL REFERENCES organizations(id),
  name                        text NOT NULL,
  min_temp_c                  double precision NOT NULL,
  max_temp_c                  double precision NOT NULL,
  allowed_excursion_minutes   integer NOT NULL CHECK (allowed_excursion_minutes >= 0),
  mkt_limit_c                 double precision,
  value_per_kg_ngn            numeric(14, 2) NOT NULL CHECK (value_per_kg_ngn >= 0),
  reference_shelf_life_hours  double precision CHECK (reference_shelf_life_hours > 0),
  source                      text,
  created_at                  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, name),
  CHECK (min_temp_c < max_temp_c)
);

CREATE TABLE shipments (
  id                uuid PRIMARY KEY,
  org_id            uuid NOT NULL REFERENCES organizations(id),
  reference         text NOT NULL,
  cargo_profile_id  uuid NOT NULL REFERENCES cargo_profiles(id),
  cargo_weight_kg   double precision NOT NULL CHECK (cargo_weight_kg > 0),
  origin            text NOT NULL,
  destination       text NOT NULL,
  status            text NOT NULL CHECK (status IN ('PLANNED', 'IN_TRANSIT', 'DELIVERED', 'CANCELLED')),
  created_at        timestamptz NOT NULL DEFAULT now(),
  closed_at         timestamptz,
  UNIQUE (org_id, reference)
);

-- A truck can carry several shipments; a shipment can move between trucks, but is
-- on exactly one truck at any instant.
CREATE TABLE shipment_assignments (
  id           uuid PRIMARY KEY,
  org_id       uuid NOT NULL REFERENCES organizations(id),
  shipment_id  uuid NOT NULL REFERENCES shipments(id),
  vehicle_id   text NOT NULL REFERENCES vehicles(vehicle_id),
  valid_from   timestamptz NOT NULL,
  valid_to     timestamptz,
  CHECK (valid_to IS NULL OR valid_to > valid_from),
  EXCLUDE USING gist (shipment_id WITH =, tstzrange(valid_from, valid_to) WITH &&)
);

-- One row per vehicle, upserted by the processor and guarded by event time.
CREATE TABLE vehicle_state (
  vehicle_id             text PRIMARY KEY REFERENCES vehicles(vehicle_id),
  org_id                 uuid NOT NULL REFERENCES organizations(id),
  last_event_time        timestamptz NOT NULL,
  last_ingest_time       timestamptz NOT NULL,
  lat                    double precision,
  lon                    double precision,
  speed_kmh              double precision,
  cargo_temp_c           double precision,
  cargo_temp_confidence  double precision CHECK (cargo_temp_confidence BETWEEN 0 AND 1),
  state                  jsonb NOT NULL DEFAULT '{}',
  updated_at             timestamptz NOT NULL DEFAULT now()
);

-- Append-only (UPDATE and DELETE revoked below). A correction is a new row that
-- references the one it supersedes.
CREATE TABLE risk_assessments (
  id                       uuid PRIMARY KEY,
  org_id                   uuid NOT NULL REFERENCES organizations(id),
  shipment_id              uuid NOT NULL REFERENCES shipments(id),
  vehicle_id               text NOT NULL REFERENCES vehicles(vehicle_id),
  assessed_at              timestamptz NOT NULL,
  score                    double precision NOT NULL CHECK (score BETWEEN 0 AND 1),
  time_to_breach_p10_min   double precision,
  time_to_breach_p50_min   double precision,
  time_to_breach_p90_min   double precision,
  confidence               double precision NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  exposure_degree_minutes  double precision NOT NULL DEFAULT 0 CHECK (exposure_degree_minutes >= 0),
  mkt_c                    double precision,
  rule_version             integer NOT NULL,
  evidence                 jsonb NOT NULL,
  supersedes               uuid REFERENCES risk_assessments(id),
  created_at               timestamptz NOT NULL DEFAULT now(),
  CHECK (time_to_breach_p10_min <= time_to_breach_p50_min
         AND time_to_breach_p50_min <= time_to_breach_p90_min)
);
CREATE INDEX risk_assessments_latest ON risk_assessments (shipment_id, assessed_at DESC);

-- id is supplied by the application as a deterministic uuid5, so a reprocessed
-- event reproduces the same alert identity. version orders projector updates.
CREATE TABLE alerts (
  id                uuid PRIMARY KEY,
  org_id            uuid NOT NULL REFERENCES organizations(id),
  shipment_id       uuid REFERENCES shipments(id),
  vehicle_id        text NOT NULL REFERENCES vehicles(vehicle_id),
  alert_type        text NOT NULL,
  severity          text NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
  state             text NOT NULL CHECK (state IN ('OPEN', 'ACKNOWLEDGED', 'MITIGATING', 'RESOLVED', 'AUTO_CLEARED')),
  dedup_key         text NOT NULL CHECK (dedup_key ~ '^[^:]+:[^:]+:[A-Z_]+$'),
  rule_version      integer NOT NULL,
  opened_at         timestamptz NOT NULL,
  last_seen_at      timestamptz NOT NULL,
  occurrence_count  integer NOT NULL DEFAULT 1 CHECK (occurrence_count >= 1),
  evidence          jsonb NOT NULL,
  version           bigint NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CHECK (last_seen_at >= opened_at)
);
-- dedup_key is '{vehicle}:{shipment or -}:{alert type}'. One live alert per key;
-- repeats update the row instead of inserting.
CREATE UNIQUE INDEX alerts_one_live ON alerts (org_id, dedup_key)
  WHERE state IN ('OPEN', 'ACKNOWLEDGED', 'MITIGATING');

CREATE TABLE interventions (
  id               uuid PRIMARY KEY,
  org_id           uuid NOT NULL REFERENCES organizations(id),
  alert_id         uuid NOT NULL REFERENCES alerts(id),
  actor_id         uuid NOT NULL,
  action           text NOT NULL CHECK (action IN ('ACK', 'ASSIGN', 'MITIGATE', 'RESOLVE', 'COMMENT')),
  note             text,
  idempotency_key  text NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (alert_id, idempotency_key)
);

CREATE TABLE rule_sets (
  id              uuid PRIMARY KEY,
  org_id          uuid NOT NULL REFERENCES organizations(id),
  version         integer NOT NULL CHECK (version > 0),
  effective_from  timestamptz NOT NULL,
  rules           jsonb NOT NULL,
  created_by      uuid,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, version)
);

-- Plumbing (section 10). Not business data, so no org_id.
CREATE TABLE processed_events (
  consumer_group  text NOT NULL,
  event_id        uuid NOT NULL,
  processed_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE outbox (
  id            bigserial PRIMARY KEY,
  topic         text NOT NULL,
  msg_key       text NOT NULL,
  payload       bytea NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Hash-chained and append-only, so tampering is detectable. The application
-- serialises each entry once into `canonical` and stores those exact bytes;
-- hash = sha256(prev_hash || canonical). jsonb is never hashed, because Postgres
-- normalises it (key order, whitespace) and the bytes would not survive a round trip.
CREATE TABLE audit_log (
  id           bigserial PRIMARY KEY,
  org_id       uuid NOT NULL REFERENCES organizations(id),
  actor_id     uuid,
  action       text NOT NULL,
  entity_type  text NOT NULL,
  entity_id    text NOT NULL,
  occurred_at  timestamptz NOT NULL DEFAULT now(),
  canonical    bytea NOT NULL,
  prev_hash    bytea UNIQUE CHECK (octet_length(prev_hash) = 32),
  hash         bytea NOT NULL UNIQUE CHECK (octet_length(hash) = 32)
);
-- Exactly one genesis row; with UNIQUE (prev_hash) the chain cannot fork.
CREATE UNIQUE INDEX audit_log_one_genesis ON audit_log ((prev_hash IS NULL)) WHERE prev_hash IS NULL;

-- Minute buckets for the last 72 hours, partitioned by day and dropped by
-- partition. Values are integer hundredths, matching watchtower_domain.buckets,
-- so sums are exact. bucket_version guards upserts: only a newer bucket applies.
CREATE TABLE minute_series (
  org_id          uuid NOT NULL REFERENCES organizations(id),
  vehicle_id      text NOT NULL REFERENCES vehicles(vehicle_id),
  minute          timestamptz NOT NULL,
  probe           text NOT NULL,
  sample_count    integer NOT NULL CHECK (sample_count > 0),
  total_centi     bigint NOT NULL,
  low_centi       integer NOT NULL,
  high_centi      integer NOT NULL,
  bucket_version  bigint NOT NULL,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (vehicle_id, probe, minute),
  CHECK (low_centi <= high_centi),
  CHECK (minute = date_trunc('minute', minute))
) PARTITION BY RANGE (minute);

-- Day partitions are created ahead of time by the projector's maintenance task, not
-- by this migration, so the migration stays independent of the date it runs.
CREATE FUNCTION ensure_minute_series_partitions(first_day date, days integer)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
  d date;
BEGIN
  -- Bounds are UTC midnights. No format() placeholders: the driver would read them
  -- as bind parameters.
  FOR i IN 0 .. days - 1 LOOP
    d := first_day + i;
    EXECUTE 'CREATE TABLE IF NOT EXISTS '
      || quote_ident('minute_series_' || to_char(d, 'YYYYMMDD'))
      || ' PARTITION OF minute_series FOR VALUES FROM ('
      || quote_literal(d::timestamp AT TIME ZONE 'UTC')
      || ') TO ('
      || quote_literal((d + 1)::timestamp AT TIME ZONE 'UTC')
      || ')';
  END LOOP;
END
$fn$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'watchtower_app') THEN
    CREATE ROLE watchtower_app NOLOGIN;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA public TO watchtower_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO watchtower_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO watchtower_app;
REVOKE ALL ON alembic_version FROM watchtower_app;
REVOKE UPDATE, DELETE, TRUNCATE ON risk_assessments, audit_log FROM watchtower_app;
"""

DOWNGRADE = """
DROP FUNCTION ensure_minute_series_partitions(date, integer);
DROP TABLE minute_series, audit_log, outbox, processed_events, rule_sets, interventions, alerts,
  risk_assessments, vehicle_state, shipment_assignments, shipments, cargo_profiles,
  sensors, vehicles, organizations;
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'watchtower_app') THEN
    REVOKE ALL ON SCHEMA public FROM watchtower_app;
    REVOKE ALL ON alembic_version FROM watchtower_app;
    DROP ROLE watchtower_app;
  END IF;
END
$$;
DROP EXTENSION IF EXISTS btree_gist;
"""


def upgrade() -> None:
    op.execute(UPGRADE)


def downgrade() -> None:
    op.execute(DOWNGRADE)
