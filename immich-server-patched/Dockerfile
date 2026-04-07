ARG IMMICH_VERSION=v2
FROM ghcr.io/immich-app/immich-server:${IMMICH_VERSION}

COPY exifdatefix.html /build/www/exifdatefix.html
COPY exifgpsfix.html /build/www/exifgpsfix.html

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
  '/usr/src/app/server/dist/services/metadata.service.js',
  helperAnchor,
  helperCode,
);

patchFile(
  '/usr/src/app/server/dist/services/person.service.js',
  helperAnchor,
  helperCode,
);

patchFile(
  '/usr/src/app/server/dist/app.common.js',
  `    if ((0, node_fs_1.existsSync)(resourcePaths.web.root)) {\n        app.use((0, sirv_1.default)(resourcePaths.web.root, {\n            etag: true,\n            gzip: true,\n            brotli: true,\n            extensions: [],\n            setHeaders: (res, pathname) => {\n                if (pathname.startsWith(\`/_app/immutable\`) && res.statusCode === 200) {\n                    res.setHeader('cache-control', 'public,max-age=31536000,immutable');\n                }\n            },\n        }));\n    }`,
  `    const exifDateFixPagePath = \`\${resourcePaths.web.root}/exifdatefix.html\`;\n    const exifGpsFixPagePath = \`\${resourcePaths.web.root}/exifgpsfix.html\`;\n    const mediaOperationsBaseUrl = (process.env.MEDIA_OPS_INTERNAL_URL || 'http://media-operations:8091').replace(/\\/$/, '');\n    const internalProxySecret = process.env.IMMICH_EVENT_WEBHOOK_SECRET || '';\n    const expressApp = app.getHttpAdapter().getInstance();\n    const utilityProxyDefinitions = [\n        {\n            pagePaths: ['/utilities/exifdatefix', '/utilities/exifdatefix/'],\n            pagePath: exifDateFixPagePath,\n            publicApiPrefix: '/utilities/exifdatefix/api',\n            internalApiPrefix: '/utility/exifdatefix/api',\n            label: 'EXIF date fix',\n        },\n        {\n            pagePaths: ['/utilities/exifgpsfix', '/utilities/exifgpsfix/'],\n            pagePath: exifGpsFixPagePath,\n            publicApiPrefix: '/utilities/exifgpsfix/api',\n            internalApiPrefix: '/utility/exifgpsfix/api',\n            label: 'EXIF GPS fix',\n        },\n    ];\n    for (const utilityDefinition of utilityProxyDefinitions) {\n        expressApp.get(utilityDefinition.pagePaths, (req, res, next) => {\n            if (!(0, node_fs_1.existsSync)(utilityDefinition.pagePath)) {\n                return next();\n            }\n            res.setHeader('cache-control', 'no-store');\n            return res.sendFile(utilityDefinition.pagePath);\n        });\n        expressApp.use(utilityDefinition.publicApiPrefix, async (req, res, next) => {\n            try {\n                const suffix = req.originalUrl.startsWith(utilityDefinition.publicApiPrefix)\n                    ? req.originalUrl.slice(utilityDefinition.publicApiPrefix.length)\n                    : '';\n                const targetUrl = \`\${mediaOperationsBaseUrl}\${utilityDefinition.internalApiPrefix}\${suffix}\`;\n                const headers = {};\n                for (const [key, value] of Object.entries(req.headers)) {\n                    if (value == null) {\n                        continue;\n                    }\n                    const lowerKey = key.toLowerCase();\n                    if (lowerKey === 'host' || lowerKey === 'content-length' || lowerKey === 'connection') {\n                        continue;\n                    }\n                    headers[key] = Array.isArray(value) ? value.join(', ') : value;\n                }\n                if (req.headers.cookie && internalProxySecret) {\n                    const authResponse = await fetch('http://127.0.0.1:2283/api/users/me', {\n                        method: 'GET',\n                        headers: {\n                            accept: 'application/json',\n                            cookie: Array.isArray(req.headers.cookie) ? req.headers.cookie.join('; ') : req.headers.cookie,\n                        },\n                    });\n                    const authPayload = authResponse.ok ? await authResponse.json().catch(() => null) : null;\n                    if (authPayload?.id) {\n                        headers['x-immich-user-id'] = authPayload.id;\n                        headers['x-media-ops-proxy-secret'] = internalProxySecret;\n                    }\n                }\n                const init = {\n                    method: req.method,\n                    headers,\n                    redirect: 'manual',\n                };\n                if (req.method !== 'GET' && req.method !== 'HEAD') {\n                    init.body = typeof req.body === 'string' ? req.body : JSON.stringify(req.body ?? {});\n                }\n                const response = await fetch(targetUrl, init);\n                res.status(response.status);\n                response.headers.forEach((value, key) => {\n                    const lowerKey = key.toLowerCase();\n                    if (lowerKey === 'content-length' || lowerKey === 'transfer-encoding' || lowerKey === 'connection' || lowerKey === 'content-encoding') {\n                        return;\n                    }\n                    res.setHeader(key, value);\n                });\n                return res.send(Buffer.from(await response.arrayBuffer()));\n            }\n            catch (error) {\n                logger.warn(\`Failed to proxy \${utilityDefinition.label} API request: \${error.message}\`);\n                return next(error);\n            }\n        });\n    }\n    if ((0, node_fs_1.existsSync)(resourcePaths.web.root)) {\n        app.use((0, sirv_1.default)(resourcePaths.web.root, {\n            etag: true,\n            gzip: true,\n            brotli: true,\n            extensions: [],\n            setHeaders: (res, pathname) => {\n                if (pathname.startsWith(\`/_app/immutable\`) && res.statusCode === 200) {\n                    res.setHeader('cache-control', 'public,max-age=31536000,immutable');\n                }\n            },\n        }));\n    }`,
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

replaceEverywhere(
  '/build/www',
  'F=[{href:O.duplicatesUtility(),icon:C,label:y()(`review_duplicates`)},{href:O.largeFileUtility(),icon:w,label:y()(`review_large_files`)},{href:O.geolocationUtility(),icon:S,label:y()(`manage_geolocation`)}];',
  'F=[{href:O.duplicatesUtility(),icon:C,label:y()(`review_duplicates`)},{href:O.largeFileUtility(),icon:w,label:y()(`review_large_files`)},{href:"/utilities/exifdatefix",icon:S,label:"EXIF Date Fix"},{href:"/utilities/exifgpsfix",icon:S,label:"EXIF GPS Fix"},{href:O.geolocationUtility(),icon:S,label:y()(`manage_geolocation`)}];',
);

const utilitiesChunkPath = patchFirstMatch(
  '/build/www',
  'F=[{href:O.duplicatesUtility(),icon:C,label:y()(`review_duplicates`)},{href:O.largeFileUtility(),icon:w,label:y()(`review_large_files`)},{href:"/utilities/exifdatefix",icon:S,label:"EXIF Date Fix"},{href:"/utilities/exifgpsfix",icon:S,label:"EXIF GPS Fix"},{href:O.geolocationUtility(),icon:S,label:y()(`manage_geolocation`)}];',
  'F=[{href:O.duplicatesUtility(),icon:C,label:y()(`review_duplicates`)},{href:O.largeFileUtility(),icon:w,label:y()(`review_large_files`)},{href:"/utilities/exifdatefix",icon:S,label:"EXIF Date Fix"},{href:"/utilities/exifgpsfix",icon:S,label:"EXIF GPS Fix"},{href:O.geolocationUtility(),icon:S,label:y()(`manage_geolocation`)}];',
);

const utilitiesChunkName = utilitiesChunkPath.split('/').pop();
const patchedUtilitiesChunkName = utilitiesChunkName.replace('.js', '.exifutilityv2.js');
const patchedUtilitiesChunkPath = utilitiesChunkPath.replace(utilitiesChunkName, patchedUtilitiesChunkName);
fs.copyFileSync(utilitiesChunkPath, patchedUtilitiesChunkPath);
replaceEverywhere('/build/www', utilitiesChunkName, patchedUtilitiesChunkName);

const appEntryPath = patchFirstMatch(
  '/build/www',
  patchedUtilitiesChunkName,
  patchedUtilitiesChunkName,
);

const appEntryName = appEntryPath.split('/').pop();
const patchedAppEntryName = appEntryName.replace('.js', '.exifentryv2.js');
const patchedAppEntryPath = appEntryPath.replace(appEntryName, patchedAppEntryName);
fs.copyFileSync(appEntryPath, patchedAppEntryPath);
replaceEverywhere('/build/www', appEntryName, patchedAppEntryName);

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
EOF
