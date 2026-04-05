# Operating Model

## Primary Role Split

This stack follows a simple operating policy:

- Immich is the primary system for photos.
- Nextcloud is the storage and fallback system.

## What "Immich Is Primary" Means

For photo users, the default entrypoint is:

- `https://media.finestar.hr`

Users should prefer Immich for:

- browsing and searching photos
- timeline view
- maps and metadata-driven discovery
- albums and day-to-day photo management
- normal photo delete and restore workflows

Direct Immich login and Nextcloud SSO are both supported, but Immich is the intended day-to-day photo interface.

## What "Nextcloud Is Storage/Fallback" Means

Nextcloud remains important, but mainly as the storage layer behind Immich.

Nextcloud is used for:

- primary file storage
- source-of-truth filesystem paths
- trash/fallback behavior
- broader file management outside the photo-focused Immich UX
- account lifecycle input for bridge provisioning

In this model, users should not be trained to use Nextcloud Photos as the main photo experience unless a specific fallback or troubleshooting case requires it.

## Operational Implications

- External libraries continue to point at Nextcloud-backed user data.
- Immich user provisioning remains linked to Nextcloud users.
- Trash operations should continue syncing from Immich toward Nextcloud trash where supported.
- Nextcloud remains the recovery path if a user needs file-level access outside Immich.

## User Guidance

- For photos: use Immich first.
- For direct file access, recovery, or non-photo file workflows: use Nextcloud.
- If Immich is unavailable, Nextcloud remains the fallback system.
