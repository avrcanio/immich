# Phase 4 OIDC Prep

This document captures the prepared OIDC integration state for Immich login against the Nextcloud stack on `cloud.finestar.hr`.

## Current Status

- Nextcloud app `oidc` is installed and enabled.
- Nextcloud issuer discovery is available at:
  - `https://cloud.finestar.hr/index.php/apps/oidc/openid-configuration`
- A confidential OIDC client named `immich` has been created in Nextcloud.
- The Nextcloud `admin` user now has the email `admin@finestar.hr` so it can reuse the existing Immich admin account instead of creating a duplicate user.

## Immich OAuth Settings

Enter these values in Immich under `Administration -> Settings -> OAuth Authentication`.

- `issuer_url`: `https://cloud.finestar.hr`
- `scope`: `openid profile email groups offline_access`
- `id_token_signed_response_alg`: `RS256`
- `storage_label_claim`: `preferred_username`
- `mobile_redirect_uri_override`: `https://media.finestar.hr/api/oauth/mobile-redirect`

Leave these rollout defaults in place for the first activation:

- `enabled`: `false` until the first admin validation window
- `auto_register`: `true`
- `auto_launch`: `false`
- `password_authentication`: keep enabled for fallback

## Redirect URIs

The prepared Nextcloud OIDC client allows these Immich redirects:

- `https://media.finestar.hr/auth/login`
- `https://media.finestar.hr/user-settings`
- `https://media.finestar.hr/api/oauth/mobile-redirect`

## Identity Mapping Rules

To avoid duplicate Immich accounts, the OIDC email claim must match the existing Immich user email.

Expected mappings:

- `admin` -> `admin@finestar.hr`
- `ante@vitalgroupsa.com` -> `ante@vitalgroupsa.com`
- `avrcan@finestar.hr` -> `avrcan@finestar.hr`

Recommended matching strategy:

- Treat `email` as the canonical user identity for existing Immich accounts.
- Use `preferred_username` only as the Immich storage label claim.
- Do not enable auto-launch before a successful admin OIDC login is verified.

## Fallback And Recovery

- Keep password login enabled in Immich during the first rollout.
- Keep the existing Immich admin account available for emergency access.
- If OAuth login fails, disable OAuth from the Immich admin settings or re-enable password login from the server CLI.

## Remaining Work Before Enabling

- Validate which claim Immich uses to link existing users during first OIDC login.
- Perform first-login verification with the admin account.
- Perform one non-admin verification to confirm the user still sees only their own library.
- Decide whether any Nextcloud group claim should later map to Immich role handling.
