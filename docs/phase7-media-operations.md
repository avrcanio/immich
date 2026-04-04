# Phase 7 Media Operations v1

This document captures the initial implementation skeleton for issue #7.

## What Exists Now

- A new `media-operations` service provides an internal HTTP and CLI layer.
- The service uses managed Immich user credentials from the bridge state instead of a global wildcard mutation flow.
- Album operations are executed through the real Immich user context:
  - `create-album`
  - `update-album`
  - `delete-album`
  - `add-assets-to-album`
  - `remove-assets-from-album`
- Asset metadata updates are executed through the real Immich user context:
  - `update-asset-metadata`
- Destructive file-oriented operations are intentionally audit-first:
  - `trash-assets`
  - `confirm-delete-assets`
  - `move-assets-to-folder`

## Security Model

- Every request must include `nextcloudUserId`.
- The service resolves the managed user through bridge state:
  - `docker-data/bridge/managed-state.json`
  - `docker-data/bridge/credentials.json`
- Asset mutations validate ownership by:
  - logging into Immich as the managed user
  - fetching each requested asset through the user-scoped Immich API
  - checking `ownerId` and/or original path against the user's library root
- This avoids a global admin API mutation path for normal user operations.

## Runtime Modes

Environment defaults are conservative:

- `MEDIA_OPS_DRY_RUN=true`
- `MEDIA_OPS_WRITEBACK_ENABLED=false`
- `MEDIA_OPS_DELETE_ENABLED=false`
- `MEDIA_OPS_FOLDER_MOVE_ENABLED=false`

That means:

- album and metadata operations return an auditable planned request only
- no live Immich mutation is executed while `MEDIA_OPS_DRY_RUN=true`
- writeback to originals is logged as a plan only
- trash and folder move flows create audit records and staged batches only
- no physical file move or hard delete is attempted by default

## Endpoints

- `GET /healthz`
- `GET /capabilities`
- `POST /operations`

Example payloads:

```json
{
  "operation": "create-album",
  "nextcloudUserId": "ante@vitalgroupsa.com",
  "albumName": "Road Trip",
  "description": "Phase 7 smoke test",
  "assetIds": ["<asset-id>"]
}
```

```json
{
  "operation": "update-asset-metadata",
  "nextcloudUserId": "ante@vitalgroupsa.com",
  "assetIds": ["<asset-id>"],
  "dateTimeOriginal": "2024-08-01T12:34:56.000Z",
  "description": "Updated through media-operations",
  "rating": 5,
  "isFavorite": true
}
```

```json
{
  "operation": "trash-assets",
  "nextcloudUserId": "ante@vitalgroupsa.com",
  "assetIds": ["<asset-id>"],
  "reason": "user requested delete"
}
```

## Audit Trail

The service writes:

- `docker-data/media-operations/last-operation.json`
- `docker-data/media-operations/audit.log`
- `docker-data/media-operations/operations-state.json`

`operations-state.json` stores staged trash batches for later confirmation.

## Known Gaps

- EXIF/XMP writeback is still a planned operation, not a live worker yet.
- Physical folder moves are still planned operations, not live storage mutations.
- Confirmed delete does not perform hard delete unless explicit live flags are enabled.
- There is no external auth layer in front of the operations API yet.
- There is no queue worker split yet; this is a single-service v1 skeleton.

## Next Logical Step

The best first live vertical slice after this skeleton is:

1. real `create-album`
2. real `update-asset-metadata`
3. sidecar writeback worker for a narrow metadata subset
4. staged trash restore / confirm flow
