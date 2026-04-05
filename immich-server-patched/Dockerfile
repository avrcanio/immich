ARG IMMICH_VERSION=v2
FROM ghcr.io/immich-app/immich-server:${IMMICH_VERSION}

# Disable motion-photo extraction to avoid noisy ffprobe errors on certain
# externally indexed Android/Google Photos JPEGs. Normal asset import,
# metadata extraction, and library scans continue to work.
RUN perl -0pi -e "s/isMotionPhoto\\(asset, tags\\) \\{.*?\\n    \\}/isMotionPhoto(asset, tags) {\\n        return false;\\n    }/s" /usr/src/app/server/dist/services/metadata.service.js

# Emit local webhooks for trash/restore/delete events so media-operations can
# mirror managed external-library deletes into Nextcloud trash and clean up
# permanent delete / empty trash actions from Nextcloud trash as well.
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
  `        await this.eventRepository.emit(force ? 'AssetDeleteAll' : 'AssetTrashAll', {\n            assetIds: ids,\n            userId: auth.user.id,\n        });\n        emitExternalImmichEvent(force ? 'AssetDeleteAll' : 'AssetTrashAll', {\n            assetIds: ids,\n            userId: auth.user.id,\n        }, this.logger);`,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  `        await this.eventRepository.emit('AssetRestoreAll', { assetIds: ids, userId: auth.user.id });`,
  `        await this.eventRepository.emit('AssetRestoreAll', { assetIds: ids, userId: auth.user.id });\n        emitExternalImmichEvent('AssetRestoreAll', { assetIds: ids, userId: auth.user.id }, this.logger);`,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  `    async empty(auth) {\n        const count = await this.trashRepository.empty(auth.user.id);\n        if (count > 0) {\n            await this.jobRepository.queue({ name: enum_1.JobName.AssetEmptyTrash, data: {} });\n        }\n        return { count };\n    }`,
  `    async empty(auth) {\n        const count = await this.trashRepository.empty(auth.user.id);\n        if (count > 0) {\n            this.emptyTrashUserId = auth.user.id;\n            await this.jobRepository.queue({ name: enum_1.JobName.AssetEmptyTrash, data: {} });\n        }\n        return { count };\n    }`,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  `    async handleBatch(ids) {\n        this.logger.debug(\`Queueing \${ids.length} asset(s) for deletion from the trash\`);\n        await this.jobRepository.queueAll(ids.map((assetId) => ({\n            name: enum_1.JobName.AssetDelete,\n            data: {\n                id: assetId,\n                deleteOnDisk: true,\n            },\n        })));\n    }`,
  `    async handleBatch(ids) {\n        this.logger.debug(\`Queueing \${ids.length} asset(s) for deletion from the trash\`);\n        await this.jobRepository.queueAll(ids.map((assetId) => ({\n            name: enum_1.JobName.AssetDelete,\n            data: {\n                id: assetId,\n                deleteOnDisk: true,\n            },\n        })));\n        if (ids.length > 0 && this.emptyTrashUserId) {\n            emitExternalImmichEvent('AssetDeleteAll', { assetIds: ids, userId: this.emptyTrashUserId }, this.logger);\n        }\n    }`,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  `        this.logger.log(\`Queued \${count} asset(s) for deletion from the trash\`);\n        return enum_1.JobStatus.Success;`,
  `        this.emptyTrashUserId = null;\n        this.logger.log(\`Queued \${count} asset(s) for deletion from the trash\`);\n        return enum_1.JobStatus.Success;`,
);

function patchFirstMatch(rootDir, matcher, replacement) {
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = `${current}/${entry.name}`;
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile() || !fullPath.endsWith('.js')) {
        continue;
      }
      const source = fs.readFileSync(fullPath, 'utf8');
      if (!source.includes(matcher)) {
        continue;
      }
      fs.writeFileSync(fullPath, source.replace(matcher, replacement));
      return fullPath;
    }
  }

  throw new Error(`Unable to find frontend patch target: ${matcher}`);
}

const shortcutChunkPath = patchFirstMatch(
  '/build/www',
  `t.key.toLowerCase()===e.key.toLowerCase()`,
  `typeof t.key=="string"&&typeof e.key=="string"&&t.key.toLowerCase()===e.key.toLowerCase()`,
);

const shortcutChunkName = shortcutChunkPath.split('/').pop();
const patchedShortcutChunkName = shortcutChunkName.replace('.js', '.patched.js');
const patchedShortcutChunkPath = shortcutChunkPath.replace(shortcutChunkName, patchedShortcutChunkName);
fs.copyFileSync(shortcutChunkPath, patchedShortcutChunkPath);

patchFirstMatch(
  '/build/www',
  shortcutChunkName,
  patchedShortcutChunkName,
);

function replaceEverywhere(rootDir, search, replacement) {
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = `${current}/${entry.name}`;
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      const source = fs.readFileSync(fullPath, 'utf8');
      if (!source.includes(search)) {
        continue;
      }
      fs.writeFileSync(fullPath, source.split(search).join(replacement));
    }
  }
}

replaceEverywhere('/build/www', shortcutChunkName, patchedShortcutChunkName);

// Improve the external-asset offline warning copy for our Nextcloud-backed
// external libraries so users understand the file was moved to trash rather
// than lost entirely.
replaceEverywhere(
  '/build/www',
  'gn=`Asset Offline`,_n=`This external asset is no longer found on disk. Please contact your Immich administrator for help.`',
  'gn=`Moved to Nextcloud trash`,_n=`This external library asset is no longer available at its original path because it was moved to Nextcloud trash. Restore it from Immich trash to bring it back, or permanently delete it to remove it from Nextcloud trash as well.`',
);

const trashNoticeChunkPath = patchFirstMatch(
  '/build/www',
  'gn=`Moved to Nextcloud trash`,_n=`This external library asset is no longer available at its original path because it was moved to Nextcloud trash. Restore it from Immich trash to bring it back, or permanently delete it to remove it from Nextcloud trash as well.`',
  'gn=`Moved to Nextcloud trash`,_n=`This external library asset is no longer available at its original path because it was moved to Nextcloud trash. Restore it from Immich trash to bring it back, or permanently delete it to remove it from Nextcloud trash as well.`',
);

const trashNoticeChunkName = trashNoticeChunkPath.split('/').pop();
const patchedTrashNoticeChunkName = trashNoticeChunkName.replace('.js', '.trashnotice.js');
const patchedTrashNoticeChunkPath = trashNoticeChunkPath.replace(trashNoticeChunkName, patchedTrashNoticeChunkName);
fs.copyFileSync(trashNoticeChunkPath, patchedTrashNoticeChunkPath);
replaceEverywhere('/build/www', trashNoticeChunkName, patchedTrashNoticeChunkName);

replaceEverywhere(
  '/build/www',
  'Zt=`Stavka izvan mreže`,Qt=`Ova vanjska stavka nije pronađena na disku. Za pomoć se obratite Immich administratoru.`',
  'Zt=`Premješteno u Nextcloud smeće`,Qt=`Ova vanjska stavka više nije dostupna na izvornom putu jer je premještena u Nextcloud smeće. Vratite je iz Immich smeća ako je želite obnoviti, ili je trajno obrišite kako bi se uklonila i iz Nextcloud smeća.`',
);

const croatianTrashNoticeChunkPath = patchFirstMatch(
  '/build/www',
  'Zt=`Premješteno u Nextcloud smeće`,Qt=`Ova vanjska stavka više nije dostupna na izvornom putu jer je premještena u Nextcloud smeće. Vratite je iz Immich smeća ako je želite obnoviti, ili je trajno obrišite kako bi se uklonila i iz Nextcloud smeća.`',
  'Zt=`Premješteno u Nextcloud smeće`,Qt=`Ova vanjska stavka više nije dostupna na izvornom putu jer je premještena u Nextcloud smeće. Vratite je iz Immich smeća ako je želite obnoviti, ili je trajno obrišite kako bi se uklonila i iz Nextcloud smeća.`',
);

const croatianTrashNoticeChunkName = croatianTrashNoticeChunkPath.split('/').pop();
const patchedCroatianTrashNoticeChunkName = croatianTrashNoticeChunkName.replace('.js', '.trashnotice.js');
const patchedCroatianTrashNoticeChunkPath = croatianTrashNoticeChunkPath.replace(croatianTrashNoticeChunkName, patchedCroatianTrashNoticeChunkName);
fs.copyFileSync(croatianTrashNoticeChunkPath, patchedCroatianTrashNoticeChunkPath);
replaceEverywhere('/build/www', croatianTrashNoticeChunkName, patchedCroatianTrashNoticeChunkName);
EOF
