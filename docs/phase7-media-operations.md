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
- Managed trash lifecycle events from Immich now mirror to Nextcloud:
  - `Delete` moves managed files to Nextcloud trash
  - `Restore` restores managed files from Nextcloud trash
  - `Permanent delete` and `Empty trash` remove matching managed items from Nextcloud trash
  - `AssetDeleteAll` fallback resolution now uses:
    - `trash-sync-state.json`
    - `delete-lookup-index.json`
    - `audit.log`
  - audit markers distinguish the fallback layer:
    - `resolved_from_trash_sync_state`
    - `resolved_from_delete_lookup_index`
    - `resolved_from_audit_log`

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

## Bridge Password Recovery Flow

If a managed Immich user exists in `managed-state.json` but is missing from `credentials.json`, the bridge now supports an admin recovery flow for issuing a direct-login password without recreating the user.

Preview:

```bash
docker compose exec nextcloud-immich-bridge \
  node /app/bridge.js issue-password --user carolina@vitalgroupsa.com
```

Apply:

```bash
docker compose exec nextcloud-immich-bridge \
  node /app/bridge.js issue-password --user carolina@vitalgroupsa.com --apply
```

Behavior:

- resolves the managed user by email or `nextcloudUserId`
- generates a new random Immich password unless `--password <value>` is provided
- updates the Immich account through the admin API
- stores the password in `docker-data/bridge/credentials.json`
- writes an audit record to `docker-data/bridge/last-password-issue.json`
- keeps `shouldChangePassword=true` by default for the next direct Immich login

## Runtime Modes

Environment defaults are conservative:

- `MEDIA_OPS_DRY_RUN=true`
- `MEDIA_OPS_WRITEBACK_ENABLED=false`
- `MEDIA_OPS_DELETE_ENABLED=false`
- `MEDIA_OPS_FOLDER_MOVE_ENABLED=false`
- `MEDIA_OPS_NEXTCLOUD_ALBUM_WRITEBACK_ENABLED=false`

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

## Verified Live Smoke Tests

The service itself remains deployed with `MEDIA_OPS_DRY_RUN=true`, but the following live smoke tests were verified by running one-off commands with `MEDIA_OPS_DRY_RUN=false` and then immediately restoring state:

- `update-asset-metadata`
  - verified with a real `ante@vitalgroupsa.com` asset by toggling `isFavorite`
  - confirmed through the Immich API that the change was applied
  - immediately restored the original `isFavorite` value
- `create-album`
  - verified with a real `ante@vitalgroupsa.com` asset
  - confirmed the album appeared in the user's album list
  - confirmed the requested asset was present in the created album
  - immediately deleted the album and confirmed it no longer appeared in the user's album list

## Nextcloud Album Writeback v1

- Immich remains the source of truth for albums.
- In live mode, `create-album` and `add-assets-to-album` can optionally write through to native Nextcloud Photos albums.
- The write-through uses local `occ` commands in the `nextcloud` container:
  - `photos:albums:create`
  - `photos:albums:add`
- Only assets that map cleanly from:
  - `/external-libraries/nextcloud-data/<user>/files/...`
  - to user-relative Nextcloud paths such as `Photos/...`
  are eligible for Nextcloud album writeback.
- If an asset is not mappable into the user's Nextcloud `files` tree, the Immich mutation still succeeds and the result is returned as `partial_failure` for the Nextcloud writeback portion.
- The response payload for live album operations now includes:
  - `immichApplied`
  - `nextcloudWritebackApplied`
  - `nextcloudWritebackStatus`
  - `nextcloudWritebackErrors`

## Known Gaps

- EXIF/XMP writeback is still a planned operation, not a live worker yet.
- Physical folder moves are still planned operations, not live storage mutations.
- There is no external auth layer in front of the operations API yet.
- There is no queue worker split yet; this is a single-service v1 skeleton.
- Album rename, remove-membership, and delete are not yet written back to Nextcloud.

## Next Logical Step

The best first live vertical slice after this skeleton is:

1. live Nextcloud album writeback verification for `create-album`
2. live Nextcloud album writeback verification for `add-assets-to-album`
3. album rename/remove/delete writeback
4. sidecar writeback worker for a narrow metadata subset
