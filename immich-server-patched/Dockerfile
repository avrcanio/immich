ARG IMMICH_VERSION=v2
FROM ghcr.io/immich-app/immich-server:${IMMICH_VERSION}

# Disable motion-photo extraction to avoid noisy ffprobe errors on certain
# externally indexed Android/Google Photos JPEGs. Normal asset import,
# metadata extraction, and library scans continue to work.
RUN perl -0pi -e "s/isMotionPhoto\\(asset, tags\\) \\{.*?\\n    \\}/isMotionPhoto(asset, tags) {\\n        return false;\\n    }/s" /usr/src/app/server/dist/services/metadata.service.js
