ARG IMMICH_VERSION=v2
FROM ghcr.io/immich-app/immich-server:${IMMICH_VERSION}

# Disable motion-photo extraction to avoid noisy ffprobe errors on certain
# externally indexed Android/Google Photos JPEGs. Normal asset import,
# metadata extraction, and library scans continue to work.
RUN perl -0pi -e "s/isMotionPhoto\\(asset, tags\\) \\{.*?\\n    \\}/isMotionPhoto(asset, tags) {\\n        return false;\\n    }/s" /usr/src/app/server/dist/services/metadata.service.js

# Emit local webhooks for trash/restore events so media-operations can mirror
# managed external-library deletes into Nextcloud trash immediately.
RUN node <<'EOF'
const fs = require('fs');

function patchFile(filePath, search, replacement) {
  const source = fs.readFileSync(filePath, 'utf8');
  if (!source.includes(search)) {
    throw new Error(`Unable to find patch target in ${filePath}`);
  }
  fs.writeFileSync(filePath, source.replace(search, replacement));
}

const helperAnchor = `const base_service_1 = require("./base.service");\n`;
const helperCode = `const base_service_1 = require("./base.service");\nconst emitExternalImmichEvent = (eventType, payload, logger) => {\n    try {\n        const webhookUrl = process.env.IMMICH_EVENT_WEBHOOK_URL;\n        const webhookSecret = process.env.IMMICH_EVENT_WEBHOOK_SECRET;\n        if (!webhookUrl || !webhookSecret) {\n            return;\n        }\n        const body = JSON.stringify({\n            eventType,\n            timestamp: new Date().toISOString(),\n            correlationId: globalThis.crypto?.randomUUID?.(),\n            ...payload,\n        });\n        void fetch(webhookUrl, {\n            method: 'POST',\n            headers: {\n                'content-type': 'application/json',\n                'x-immich-event-secret': webhookSecret,\n            },\n            body,\n        })\n            .then(async (response) => {\n                if (!response.ok) {\n                    const text = await response.text().catch(() => '');\n                    logger?.warn?.(\`External event webhook \${eventType} failed with \${response.status}\${text ? \`: \${text}\` : ''}\`);\n                }\n            })\n            .catch((error) => {\n                logger?.warn?.(\`External event webhook \${eventType} failed: \${error.message}\`);\n            });\n    }\n    catch (error) {\n        logger?.warn?.(\`External event webhook \${eventType} failed: \${error.message}\`);\n    }\n};\n`;

patchFile(
  '/usr/src/app/server/dist/services/asset.service.js',
  helperAnchor,
  helperCode,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  helperAnchor,
  helperCode,
);

patchFile(
  '/usr/src/app/server/dist/services/asset.service.js',
  `        await this.eventRepository.emit(force ? 'AssetDeleteAll' : 'AssetTrashAll', {\n            assetIds: ids,\n            userId: auth.user.id,\n        });`,
  `        await this.eventRepository.emit(force ? 'AssetDeleteAll' : 'AssetTrashAll', {\n            assetIds: ids,\n            userId: auth.user.id,\n        });\n        if (!force) {\n            emitExternalImmichEvent('AssetTrashAll', {\n                assetIds: ids,\n                userId: auth.user.id,\n            }, this.logger);\n        }`,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  `        await this.eventRepository.emit('AssetRestoreAll', { assetIds: ids, userId: auth.user.id });`,
  `        await this.eventRepository.emit('AssetRestoreAll', { assetIds: ids, userId: auth.user.id });\n        emitExternalImmichEvent('AssetRestoreAll', { assetIds: ids, userId: auth.user.id }, this.logger);`,
);
EOF
