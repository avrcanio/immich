# AGENTS.md

## Environment Overview

This workspace is a standalone Docker-based Immich stack under `/opt/stacks/immich`.
Treat this directory as the source of truth for the Immich deployment, separate from the existing Nextcloud stack in `/opt/stacks/nextcloud`.

Prefer container-first workflows. Do not install duplicate host services unless a task explicitly requires it.

## Stack Layout

Current workspace files:

- `docker-compose.yml` defines the Immich stack
- `.env` contains runtime configuration for the stack

Expected local data paths:

- `./docker-data/library` for Immich-managed storage
- `./docker-data/postgres` for the Immich PostgreSQL data directory

Important read-only bind mount used by the stack:

- `/opt/stacks/nextcloud/docker-data/nextcloud/data/ante@vitalgroupsa.com/files/Photos:/external-libraries/ante-photos:ro`

That mount exists so Immich can index the main Nextcloud `Photos` folder as an external library.
Do not change it to read-write unless the task explicitly requires a different architecture.

## Docker Workflow

Use Docker Compose from this workspace:

```bash
cd /opt/stacks/immich
docker compose config
docker compose up -d
docker compose logs -f immich-server
```

Useful service names in this stack:

- `immich-server`
- `immich-machine-learning`
- `redis`
- `database`

When debugging, prefer:

- `docker compose ps`
- `docker compose logs --tail=200 <service>`
- `docker compose exec <service> <command>`

This host runs many unrelated containers. Be careful not to disrupt other stacks while testing or restarting services.

## Networking And Routing

Immich is intended to be exposed through Traefik on:

- `media.finestar.hr`

The compose file joins the external Docker network:

- `proxy`

Before changing routing labels, verify the `proxy` network still exists and avoid editing Nextcloud routing unless the task explicitly spans both stacks.

## Database

This stack uses its own PostgreSQL container:

- service: `database`
- image: `ghcr.io/immich-app/postgres:14-vectorchord0.4.3-pgvectors0.2.0`

Do not assume the shared PostGIS containers under other stacks are part of this deployment.
For Immich tasks, the default database target is the local compose `database` service unless the task says otherwise.

## Working Notes

- This workspace is deployment-oriented, not application-source-oriented.
- Prefer changing `docker-compose.yml` and `.env` rather than inventing extra wrapper scripts unless they materially improve operations.
- Keep Immich logically separate from Nextcloud:
  Nextcloud remains the primary sync/storage system, while Immich is the fast read-only media viewer over the mounted library.
- If you need GitHub CLI on this host, use:

```bash
GH_CONFIG_DIR=/opt/stacks gh <command>
```
