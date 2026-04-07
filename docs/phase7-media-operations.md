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

## EXIF Date Fix Utility

- The stack now includes a dedicated EXIF date-fix utility UI exposed through Immich utilities.
- The utility looks for assets whose filename contains a trustworthy date pattern and where the stored capture date is either missing or suspicious.
- Supported filename families currently include:
  - `YYYYMMDD_HHMMSS`
  - `YYYYMMDDHHMMSS`
  - `YYYYMMDDHMMSS`
  - `YYYY-MM-DD HH.MM.SS`
  - `YYYY_MM_DD_HH_MM_SS`
  - `PXL_YYYYMMDD_HHMMSS`
  - `Screenshot_YYYYMMDD-HHMMSS`
  - `screen_<hash>_<epoch-ms>` and `screen_<hash>_<epoch-ms>-edited`
  - `signal-YYYY-MM-DD-HHMMSS`
  - `IMG-YYYYMMDD-WA...` and `VID-YYYYMMDD-WA...`
  - `DJI<epoch-ms>`
  - `FB_IMG_<epoch-ms>`
  - `FACE_SC_<epoch-ms>`
  - `PicPlus_<epoch-ms>`
- The parser now validates extracted date parts before proposing them:
  - invalid month/day/hour/minute/second combinations are discarded instead of breaking the whole queue
  - this keeps compact numeric filename support usable without accepting accidental false positives
- When a match is found, the utility normalizes the detected value into:
  - Immich metadata update payloads
  - file-level EXIF/XMP writeback via `exiftool`
- The writeback path is now verified for:
  - `jpg`
  - `jpeg`
  - `png`
  - `webp`
  - `gif`
  - `heic`
  - `heif`
  - `mp4`
  - `mov`
  - `3gp`
- For video assets, the writeback path also updates QuickTime metadata fields:
  - `CreateDate`
  - `ModifyDate`
  - `TrackCreateDate`
  - `TrackModifyDate`
  - `MediaCreateDate`
  - `MediaModifyDate`
  - `Keys:CreationDate`
- The utility was also exercised against real problem clusters during cleanup:
  - DJI exports where the filename is an epoch timestamp
  - `screen_...` Android exports that store an epoch timestamp at the end of the name
  - compact numeric camera names such as `20140407235852.jpg`, `2014040803738.jpg`, and `20200223125641.gif`
- The utility is still intentionally conservative:
  - it only proposes dates when the filename pattern is recognized with confidence
  - assets with opaque names such as `image-<hash>.jpg` still require manual review or another source of truth

## EXIF GPS Fix Utility

- The stack now also includes a dedicated EXIF GPS fix utility UI exposed at `/utilities/exifgpsfix`.
- The GPS utility is DB-first:
  - it reads candidate assets from Immich/Postgres
  - it targets images where `asset_exif.latitude` or `asset_exif.longitude` is missing
- Candidate review is user-scoped and limited to the managed library paths for the authenticated user.
- For each candidate, the utility looks up:
  - the nearest previous image with GPS
  - the nearest next image with GPS
  - a default suggestion chosen by the smaller time delta
- Refresh behavior is now explicit for the currently open asset:
  - the refresh button re-runs suggestion lookup for that asset
  - it does not advance the queue
  - it clears the GPS utility row/queue caches before rebuilding suggestions
- Reference selection logic now handles real-world edited variants more robustly:
  - if a filename belongs to the same normalized capture group as another asset with GPS, that sibling/original variant is preferred
  - normalization strips common suffixes such as `-editado`, `-edited`, `-edit`, `-copy`, `-copia`, `-copie`, and `-final`
  - if no such sibling GPS asset exists, the utility falls back to the nearest chronological GPS neighbor
- GPS correction UI currently supports:
  - map click to choose a manual point
  - numeric latitude/longitude entry
  - date picker and time picker for correcting `dateTimeOriginal` on the same screen
  - `Apply GPS`
  - `Skip`
  - `Delete`
  - refresh references button
- Apply behavior:
  - updates Immich asset metadata immediately
  - can update GPS and `dateTimeOriginal` together in a single apply action
  - enqueues background EXIF writeback for the original file
- File-level writeback currently covers:
  - GPS EXIF/XMP writeback via `exiftool`
  - date/time EXIF writeback via `exiftool` when a manual date and time are provided
- The GPS utility was exercised directly against the managed library during metadata cleanup and iterative tuning of refresh/reference behavior.

## Known Gaps

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
