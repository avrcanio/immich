# Phase 8 Shared PostGIS Migration

This document turns issue `#8` into an execution runbook for this host.

## Current Verified State

The Immich stack in this workspace still uses the local `database` service:

- container: `immich_postgres`
- image: `ghcr.io/immich-app/postgres:14-vectorchord0.4.3-pgvectors0.2.0`
- current database: `immich`

The shared PostgreSQL target already exists on this host as container `postgis` and is reachable on the external Docker network `postgis`.

Verified extension gap on `2026-04-04`:

- current Immich DB extensions:
  - `vector`
  - `vchord`
  - `pg_trgm`
  - `unaccent`
  - `uuid-ossp`
  - `cube`
  - `earthdistance`
- current shared `postgis` extensions:
  - `postgis`
  - `postgis_topology`
  - `postgis_tiger_geocoder`
  - `fuzzystrmatch`

That means the shared cluster is not ready for Immich yet. Do not attempt cutover until the shared image and target database can provide all required Immich extensions.

## Scope

The migration goal is:

- move Immich from the local `database` service to the shared `postgis` cluster
- keep Nextcloud as the storage system of record
- keep rollback simple by preserving the old local data directory until production validation is complete

This workspace now includes the external `postgis` network on `immich-server` so the app can reach the shared cluster once DB variables are switched.

## Shared DB Preparation

The shared cluster lives outside this workspace under `/opt/stacks/data`.

Implemented image preparation currently lives in:

- [`/opt/stacks/data/Dockerfile.postgis`](/opt/stacks/data/Dockerfile.postgis)
- [`/opt/stacks/data/docker-compose.yml`](/opt/stacks/data/docker-compose.yml)

Required preparation there:

1. Build a reproducible custom shared PostgreSQL 16 image that keeps PostGIS and adds the Immich requirements:
   - `vector`
   - `vchord`
   - `pg_trgm`
   - `unaccent`
   - `uuid-ossp`
   - `cube`
   - `earthdistance`
   - `shared_preload_libraries=vchord,pg_cron`
2. Restart the shared `postgis` stack only after confirming the image change is safe for every workload already using that cluster.
3. Create dedicated Immich resources on the shared cluster:
   - role: `immich`
   - database: `immich`
   - strong application password
4. Pre-create the required extensions inside the new `immich` database before any restore.

Validated preparation state on `2026-04-04`:

- image build passes for `postgis-local:16-bookworm-postgis3-cron-vchord1.1.1`
- the image is currently built from:
  - `postgres:16-bookworm`
  - PGDG packages for PostGIS, `pg_cron`, and `pgvector`
  - official TensorChord PG16 `vchord` artifacts
- a disposable test container successfully created:
  - `postgis`
  - `vector`
  - `vchord`
  - `pg_trgm`
  - `unaccent`
  - `uuid-ossp`
  - `cube`
  - `earthdistance`

Suggested validation commands on the shared cluster after image prep:

```bash
docker exec -it postgis psql -U postgres -d postgres -c "SELECT version();"
docker exec -it postgis psql -U postgres -d immich -c \"SELECT extname FROM pg_extension ORDER BY extname;\"
```

The target extension list must include every extension currently present in the source Immich database before continuing.

Important note:

- `vchord` requires `shared_preload_libraries`
- the prepared shared compose now sets:
  - `shared_preload_libraries=vchord,pg_cron`
  - `cron.database_name=postgres`

## Immich Maintenance Window

Use a simple controlled cutover. Near-zero-downtime migration is not the goal here.

Stop all application services that can write through Immich:

```bash
cd /opt/stacks/immich
docker compose stop immich-server nextcloud-immich-bridge media-operations immich-machine-learning redis
```

Create a source backup from the current local Immich database:

```bash
cd /opt/stacks/immich
mkdir -p ./docker-data/migration
docker compose exec -T database pg_dump -U "${DB_USERNAME}" -d "${DB_DATABASE_NAME}" -Fc > ./docker-data/migration/immich-$(date +%F-%H%M%S).dump
```

Prepare the target DB resources on the shared cluster:

```bash
docker exec -i postgis psql -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'immich') THEN
    CREATE ROLE immich LOGIN PASSWORD 'replace-with-strong-password';
  END IF;
END
$$;
SQL

docker exec postgis psql -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='immich'" | grep -q 1 || \
  docker exec postgis psql -U postgres -d postgres -c "CREATE DATABASE immich OWNER immich;"

docker exec -i postgis psql -U postgres -d immich -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS vchord;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;
SQL
```

Restore into the shared target:

```bash
LATEST_DUMP="$(ls -1t ./docker-data/migration/immich-*.dump | head -n1)"
docker exec -i postgis pg_restore \
  -U immich \
  -d immich \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges < "$LATEST_DUMP"
```

## Pre-Cutover Validation

Before changing Immich config, validate the target database.

Extensions:

```bash
docker exec postgis psql -U postgres -d immich -Atc "SELECT extname FROM pg_extension ORDER BY extname;"
```

Core row counts:

```bash
docker exec immich_postgres psql -U postgres -d immich -Atc "SELECT 'users', count(*) FROM users UNION ALL SELECT 'libraries', count(*) FROM libraries UNION ALL SELECT 'assets', count(*) FROM assets ORDER BY 1;"
docker exec postgis psql -U postgres -d immich -Atc "SELECT 'users', count(*) FROM users UNION ALL SELECT 'libraries', count(*) FROM libraries UNION ALL SELECT 'assets', count(*) FROM assets ORDER BY 1;"
```

Only continue when the extension list and row counts line up with expectations.

## Cutover

Update [`.env`](/opt/stacks/immich/.env) for the shared cluster values:

- `DB_HOSTNAME=postgis`
- `DB_PORT=5432`
- `DB_USERNAME=immich`
- `DB_DATABASE_NAME=immich`
- `DB_PASSWORD=<shared-immich-password>`

Optional: if you prefer a single DSN, Immich also supports `DB_URL`, which overrides the other `DB_*` connection settings.

Then remove the local `database` service and `depends_on: database` from [`docker-compose.yml`](/opt/stacks/immich/docker-compose.yml).

Bring the stack back up:

```bash
cd /opt/stacks/immich
docker compose up -d --build
docker compose ps
docker compose logs --tail=200 immich-server
```

## Post-Cutover Validation

Validate:

- `immich-server` is healthy
- `nextcloud-immich-bridge` is healthy
- `media-operations` is healthy
- login works at `https://media.finestar.hr`
- an existing user can see expected assets
- bridge state stays consistent
- one narrow write smoke test works:
  - create album
  - or update asset metadata

## Rollback

If validation fails:

1. Stop the Immich application services.
2. Revert DB variables in [`.env`](/opt/stacks/immich/.env) back to the local database values.
3. Restore the `database` service and `depends_on: database` in [`docker-compose.yml`](/opt/stacks/immich/docker-compose.yml).
4. Start the stack again against the preserved local `immich_postgres`.

Only after a stable production period on the shared cluster should you:

- archive or snapshot `./docker-data/postgres`
- remove the local `database` service permanently
- remove the `immich_postgres` container

## Notes

- The shared DB image change is high-risk because other production workloads already use that cluster.
- Do not assume a plain restore into a stock PostGIS image will work. The extension layer is the blocker.
- This workspace has been prepared for network reachability to `postgis`, but it has not been cut over automatically.
