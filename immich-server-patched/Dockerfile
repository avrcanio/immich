ARG IMMICH_VERSION=v3.4.2
FROM ghcr.io/immich-app/immich-server:${IMMICH_VERSION}

COPY exifdatefix.html /build/www/exifdatefix.html
COPY exifgpsfix.html /build/www/exifgpsfix.html
COPY patch-server.js /tmp/patch-server.js

# Keep the local webhooks, EXIF utility proxy, offline-asset copy, and the
# album-owner trigger correction. The patch script fails the build
# when an expected anchor is missing.
RUN node /tmp/patch-server.js && rm /tmp/patch-server.js
