const fs = require('fs');
const path = require('path');

function patchFile(filePath, search, replacement) {
  const source = fs.readFileSync(filePath, 'utf8');
  if (!source.includes(search)) {
    throw new Error(`Unable to find patch target in ${filePath}: ${search.slice(0, 180)}`);
  }
  fs.writeFileSync(filePath, source.replace(search, replacement));
}

function replaceEverywhere(rootDir, search, replacement) {
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      const extension = path.extname(entry.name);
      if (!['.js', '.html', '.css'].includes(extension)) {
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

function retargetChunks(rootDir, marker, suffix) {
  const matches = [];
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile() || !entry.name.endsWith('.js') || entry.name.endsWith(suffix)) {
        continue;
      }
      const source = fs.readFileSync(fullPath, 'utf8');
      if (source.includes(marker)) {
        matches.push(fullPath);
      }
    }
  }
  if (matches.length === 0) {
    throw new Error(`Unable to find frontend chunk for ${marker}`);
  }
  for (const fullPath of matches) {
    const chunkName = path.basename(fullPath);
    const patchedName = chunkName.replace(/\.js$/, suffix);
    fs.copyFileSync(fullPath, path.join(path.dirname(fullPath), patchedName));
    replaceEverywhere(rootDir, chunkName, patchedName);
  }
}

const migrationPath = '/usr/src/app/server/dist/schema/migrations/1786385711807-AlbumOwnerDeleteTrigger.js';
const migrationSource = fs.readFileSync(migrationPath, 'utf8');
const referencingMarker = 'REFERENCING OLD TABLE AS ';
const migrationParts = migrationSource.split(referencingMarker);
if (migrationParts.length !== 3) {
  throw new Error(`Expected 2 album owner trigger clauses, found ${migrationParts.length - 1}`);
}
const correctedMigration = migrationParts
  .map((part, index) => {
    if (index === 0) {
      return part;
    }
    const forEachAt = part.indexOf('FOR EACH ROW');
    if (forEachAt < 0) {
      throw new Error('FOR EACH ROW missing after referencing clause');
    }
    return part.slice(forEachAt);
  })
  .join('');
if (correctedMigration.includes(referencingMarker)) {
  throw new Error('Album owner trigger still references an old table');
}
fs.writeFileSync(migrationPath, correctedMigration);
patchFile(
  '/usr/src/app/server/dist/schema/tables/album-user.table.js',
  "AfterDeleteTrigger({ scope: 'row', function: album_user_delete, referencingOldTableAs: 'old' })",
  "AfterDeleteTrigger({ scope: 'row', function: album_user_delete })",
);

patchFile(
  '/usr/src/app/server/dist/services/metadata.service.js',
  `isMotionPhoto(asset, tags) {\n        return asset.type === AssetType.Image && !!(tags.MotionPhoto || tags.MicroVideo);\n    }`,
  `isMotionPhoto(asset, tags) {\n        return false;\n    }`,
);

const helperAnchor = `import { BaseService } from "./base.service.js";\n`;
const helperCode = `${helperAnchor}const emitExternalImmichEvent = (eventType, payload, logger) => {\n    try {\n        const webhookUrl = process.env.IMMICH_EVENT_WEBHOOK_URL;\n        const webhookSecret = process.env.IMMICH_EVENT_WEBHOOK_SECRET;\n        if (!webhookUrl || !webhookSecret) {\n            return;\n        }\n        const body = JSON.stringify({\n            eventType,\n            timestamp: new Date().toISOString(),\n            correlationId: globalThis.crypto?.randomUUID?.(),\n            ...payload,\n        });\n        void fetch(webhookUrl, {\n            method: 'POST',\n            headers: {\n                'content-type': 'application/json',\n                'x-immich-event-secret': webhookSecret,\n            },\n            body,\n        })\n            .then(async (response) => {\n                if (!response.ok) {\n                    const text = await response.text().catch(() => '');\n                    logger?.warn?.(\`External event webhook \${eventType} failed with \${response.status}\${text ? \`: \${text}\` : ''}\`);\n                }\n            })\n            .catch((error) => {\n                logger?.warn?.(\`External event webhook \${eventType} failed: \${error.message}\`);\n            });\n    }\n    catch (error) {\n        logger?.warn?.(\`External event webhook \${eventType} failed: \${error.message}\`);\n    }\n};\n`;

for (const fileName of ['asset.service.js', 'trash.service.js', 'metadata.service.js', 'person.service.js']) {
  patchFile(`/usr/src/app/server/dist/services/${fileName}`, helperAnchor, helperCode);
}

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
  '/usr/src/app/server/dist/services/metadata.service.js',
  `        await this.eventRepository.emit('AssetMetadataExtracted', {\n            assetId: asset.id,\n            userId: asset.ownerId,\n            source: data.source,\n        });`,
  `        await this.eventRepository.emit('AssetMetadataExtracted', {\n            assetId: asset.id,\n            userId: asset.ownerId,\n            source: data.source,\n        });\n        emitExternalImmichEvent('AssetMetadataExtracted', {\n            assetIds: [asset.id],\n            userId: asset.ownerId,\n            source: data.source,\n        }, this.logger);`,
);

patchFile(
  '/usr/src/app/server/dist/services/person.service.js',
  `        await this.assetRepository.upsertJobStatus({ assetId: asset.id, facesRecognizedAt: new Date() });`,
  `        await this.assetRepository.upsertJobStatus({ assetId: asset.id, facesRecognizedAt: new Date() });\n        emitExternalImmichEvent('AssetFacesRecognized', { assetIds: [asset.id], userId: asset.ownerId }, this.logger);`,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  `    async empty(auth) {\n        const count = await this.trashRepository.empty(auth.user.id);\n        if (count > 0) {\n            await this.jobRepository.queue({ name: JobName.AssetEmptyTrash, data: {} });\n        }\n        return { count };\n    }`,
  `    async empty(auth) {\n        const count = await this.trashRepository.empty(auth.user.id);\n        if (count > 0) {\n            this.emptyTrashUserId = auth.user.id;\n            await this.jobRepository.queue({ name: JobName.AssetEmptyTrash, data: {} });\n        }\n        return { count };\n    }`,
);

patchFile(
  '/usr/src/app/server/dist/services/trash.service.js',
  `    async handleEmptyTrash() {\n        let count = 0;\n        for await (const assets of batched(this.trashRepository.getDeletedIds())) {\n            await this.jobRepository.queueAll(assets.map(({ id }) => ({ name: JobName.AssetDelete, data: { id, deleteOnDisk: true } })));\n            count += assets.length;\n        }\n        this.logger.log(\`Queued \${count} asset(s) for deletion from the trash\`);\n        return JobStatus.Success;\n    }`,
  `    async handleEmptyTrash() {\n        let count = 0;\n        for await (const assets of batched(this.trashRepository.getDeletedIds())) {\n            const ids = assets.map(({ id }) => id);\n            await this.jobRepository.queueAll(ids.map((id) => ({ name: JobName.AssetDelete, data: { id, deleteOnDisk: true } })));\n            count += ids.length;\n            if (ids.length > 0 && this.emptyTrashUserId) {\n                emitExternalImmichEvent('AssetDeleteAll', { assetIds: ids, userId: this.emptyTrashUserId }, this.logger);\n            }\n        }\n        this.emptyTrashUserId = null;\n        this.logger.log(\`Queued \${count} asset(s) for deletion from the trash\`);\n        return JobStatus.Success;\n    }`,
);

patchFile(
  '/usr/src/app/server/dist/app.common.js',
  `    if (existsSync(resourcePaths.web.root)) {\n        app.use(sirv(resourcePaths.web.root, {\n            etag: true,\n            gzip: true,\n            brotli: true,\n            extensions: [],\n            setHeaders: (res, pathname) => {\n                if (pathname.startsWith(\`/_app/immutable\`) && res.statusCode === 200) {\n                    res.setHeader('cache-control', 'public,max-age=31536000,immutable');\n                }\n            },\n        }));\n    }`,
  `    const exifDateFixPagePath = \`\${resourcePaths.web.root}/exifdatefix.html\`;\n    const exifGpsFixPagePath = \`\${resourcePaths.web.root}/exifgpsfix.html\`;\n    const mediaOperationsBaseUrl = (process.env.MEDIA_OPS_INTERNAL_URL || 'http://media-operations:8091').replace(/\\/$/, '');\n    const internalProxySecret = process.env.IMMICH_EVENT_WEBHOOK_SECRET || '';\n    const expressApp = app.getHttpAdapter().getInstance();\n    const utilityProxyDefinitions = [\n        {\n            pagePaths: ['/utilities/exifdatefix', '/utilities/exifdatefix/'],\n            pagePath: exifDateFixPagePath,\n            publicApiPrefix: '/utilities/exifdatefix/api',\n            internalApiPrefix: '/utility/exifdatefix/api',\n            label: 'EXIF date fix',\n        },\n        {\n            pagePaths: ['/utilities/exifgpsfix', '/utilities/exifgpsfix/'],\n            pagePath: exifGpsFixPagePath,\n            publicApiPrefix: '/utilities/exifgpsfix/api',\n            internalApiPrefix: '/utility/exifgpsfix/api',\n            label: 'EXIF GPS fix',\n        },\n    ];\n    for (const utilityDefinition of utilityProxyDefinitions) {\n        expressApp.get(utilityDefinition.pagePaths, (req, res, next) => {\n            if (!existsSync(utilityDefinition.pagePath)) {\n                return next();\n            }\n            res.setHeader('cache-control', 'no-store');\n            return res.sendFile(utilityDefinition.pagePath);\n        });\n        expressApp.use(utilityDefinition.publicApiPrefix, async (req, res, next) => {\n            try {\n                const suffix = req.originalUrl.startsWith(utilityDefinition.publicApiPrefix)\n                    ? req.originalUrl.slice(utilityDefinition.publicApiPrefix.length)\n                    : '';\n                const targetUrl = \`\${mediaOperationsBaseUrl}\${utilityDefinition.internalApiPrefix}\${suffix}\`;\n                const headers = {};\n                for (const [key, value] of Object.entries(req.headers)) {\n                    if (value == null) {\n                        continue;\n                    }\n                    const lowerKey = key.toLowerCase();\n                    if (lowerKey === 'host' || lowerKey === 'content-length' || lowerKey === 'connection') {\n                        continue;\n                    }\n                    headers[key] = Array.isArray(value) ? value.join(', ') : value;\n                }\n                if (req.headers.cookie && internalProxySecret) {\n                    const authResponse = await fetch('http://127.0.0.1:2283/api/users/me', {\n                        method: 'GET',\n                        headers: {\n                            accept: 'application/json',\n                            cookie: Array.isArray(req.headers.cookie) ? req.headers.cookie.join('; ') : req.headers.cookie,\n                        },\n                    });\n                    const authPayload = authResponse.ok ? await authResponse.json().catch(() => null) : null;\n                    if (authPayload?.id) {\n                        headers['x-immich-user-id'] = authPayload.id;\n                        headers['x-media-ops-proxy-secret'] = internalProxySecret;\n                    }\n                }\n                const init = {\n                    method: req.method,\n                    headers,\n                    redirect: 'manual',\n                };\n                if (req.method !== 'GET' && req.method !== 'HEAD') {\n                    init.body = typeof req.body === 'string' ? req.body : JSON.stringify(req.body ?? {});\n                }\n                const response = await fetch(targetUrl, init);\n                res.status(response.status);\n                response.headers.forEach((value, key) => {\n                    const lowerKey = key.toLowerCase();\n                    if (lowerKey === 'content-length' || lowerKey === 'transfer-encoding' || lowerKey === 'connection' || lowerKey === 'content-encoding') {\n                        return;\n                    }\n                    res.setHeader(key, value);\n                });\n                return res.send(Buffer.from(await response.arrayBuffer()));\n            }\n            catch (error) {\n                logger.warn(\`Failed to proxy \${utilityDefinition.label} API request: \${error.message}\`);\n                return next(error);\n            }\n        });\n    }\n    if (existsSync(resourcePaths.web.root)) {\n        app.use(sirv(resourcePaths.web.root, {\n            etag: true,\n            gzip: true,\n            brotli: true,\n            extensions: [],\n            setHeaders: (res, pathname) => {\n                if (pathname === '/service-worker.js' || pathname === '/_app/env.js') {\n                    res.setHeader('cache-control', 'no-store');\n                    return;\n                }\n                if (pathname.startsWith(\`/_app/immutable\`) && res.statusCode === 200) {\n                    res.setHeader('cache-control', 'public,max-age=31536000,immutable');\n                }\n            },\n        }));\n    }`,
);

const offlineNotice = 'This external library asset is no longer available at its original path because it was moved to Nextcloud trash. Restore it from Immich trash to bring it back, or permanently delete it to remove it from Nextcloud trash as well.';
const croatianOfflineNotice = 'Ova vanjska stavka više nije dostupna na izvornom putu jer je premještena u Nextcloud smeće. Vratite je iz Immich smeća ako je želite obnoviti, ili je trajno obrišite kako bi se uklonila i iz Nextcloud smeća.';

replaceEverywhere(
  '/build/www',
  'Ft=`Asset Offline`,It=`This external asset is no longer found on disk. Please contact your Immich administrator for help.`',
  `Ft=\`Moved to Nextcloud trash\`,It=\`${offlineNotice}\``,
);
replaceEverywhere(
  '/build/www',
  'Lt=`Asset Offline`,Rt=`This external asset is no longer found on disk. Please contact your Immich administrator for help.`',
  `Lt=\`Moved to Nextcloud trash\`,Rt=\`${offlineNotice}\``,
);
replaceEverywhere(
  '/build/www',
  'Ft=`Stavka izvan mreže`,It=`Ova vanjska stavka nije pronađena na disku. Za pomoć se obratite Immich administratoru.`',
  `Ft=\`Premješteno u Nextcloud smeće\`,It=\`${croatianOfflineNotice}\``,
);
replaceEverywhere(
  '/build/www',
  't.key.toLowerCase()===e.key.toLowerCase()',
  'typeof t.key=="string"&&typeof e.key=="string"&&t.key.toLowerCase()===e.key.toLowerCase()',
);
replaceEverywhere(
  '/build/www',
  'F=[{href:I.duplicatesUtility(),icon:w,label:i()(`review_duplicates`)},{href:I.largeFileUtility(),icon:O,label:i()(`review_large_files`)},{href:I.geolocationUtility(),icon:S,label:i()(`manage_geolocation`)},{href:I.workflows(),icon:x,label:i()(`workflows`)}];',
  'F=[{href:I.duplicatesUtility(),icon:w,label:i()(`review_duplicates`)},{href:I.largeFileUtility(),icon:O,label:i()(`review_large_files`)},{href:"/utilities/exifdatefix",icon:S,label:"EXIF Date Fix"},{href:"/utilities/exifgpsfix",icon:S,label:"EXIF GPS Fix"},{href:I.geolocationUtility(),icon:S,label:i()(`manage_geolocation`)},{href:I.workflows(),icon:x,label:i()(`workflows`)}];',
);

retargetChunks('/build/www', 'Moved to Nextcloud trash', '.trashnotice.js');
retargetChunks('/build/www', 'Premješteno u Nextcloud smeće', '.trashnotice.js');
retargetChunks('/build/www', 'typeof t.key=="string"&&typeof e.key=="string"&&t.key.toLowerCase()===e.key.toLowerCase()', '.shortcut.js');
retargetChunks('/build/www', 'href:"/utilities/exifdatefix"', '.exifutilityv2.js');

replaceEverywhere(
  '/build/www',
  `  </body>`,
  `    <script>
      (() => {
        const LINK_ID = 'finestar-utility-links';
        const PATHS = new Set(['/utilities', '/utilities/']);
        const ensureLink = () => {
          const existing = document.getElementById(LINK_ID);
          const shouldShow = PATHS.has(window.location.pathname);
          if (!shouldShow) {
            existing?.remove();
            return;
          }
          if (existing) {
            return;
          }
          const container = document.createElement('div');
          container.id = LINK_ID;
          Object.assign(container.style, {
            position: 'fixed',
            right: '24px',
            bottom: '24px',
            zIndex: '9999',
            display: 'flex',
            gap: '12px',
            flexWrap: 'wrap',
            justifyContent: 'flex-end',
          });
          const makeLink = (href, text) => {
            const link = document.createElement('a');
            link.href = href;
            link.textContent = text;
            Object.assign(link.style, {
              padding: '12px 16px',
              borderRadius: '999px',
              background: '#c3562a',
              color: '#fff8f3',
              textDecoration: 'none',
              fontFamily: 'Google Sans, sans-serif',
              fontWeight: '600',
              boxShadow: '0 10px 30px rgba(0,0,0,0.18)',
            });
            return link;
          };
          container.appendChild(makeLink('/utilities/exifdatefix', 'EXIF Date Fix'));
          container.appendChild(makeLink('/utilities/exifgpsfix', 'EXIF GPS Fix'));
          document.body.appendChild(container);
        };
        const wrapHistory = (method) => {
          const original = history[method];
          history[method] = function (...args) {
            const result = original.apply(this, args);
            queueMicrotask(ensureLink);
            return result;
          };
        };
        wrapHistory('pushState');
        wrapHistory('replaceState');
        addEventListener('popstate', ensureLink);
        addEventListener('load', ensureLink);
        setInterval(ensureLink, 1000);
      })();
    </script>
  </body>`,
);
