const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');

const config = {
  port: parseInteger(env('MEDIA_OPS_PORT', '8091'), 8091),
  host: env('MEDIA_OPS_HOST', '0.0.0.0'),
  stateDir: env('MEDIA_OPS_STATE_DIR', '/state'),
  bridgeStateDir: env('MEDIA_OPS_BRIDGE_STATE_DIR', '/bridge-state'),
  sourceRoot: env('MEDIA_OPS_SOURCE_ROOT', '/external-libraries/nextcloud-data'),
  immichApiUrl: stripTrailingSlash(env('IMMICH_API_URL', 'http://immich-server:2283/api')),
  immichAdminApiKey: env('IMMICH_API_KEY', ''),
  dryRun: parseBoolean(env('MEDIA_OPS_DRY_RUN', 'true')),
  writebackEnabled: parseBoolean(env('MEDIA_OPS_WRITEBACK_ENABLED', 'false')),
  deleteEnabled: parseBoolean(env('MEDIA_OPS_DELETE_ENABLED', 'false')),
  folderMoveEnabled: parseBoolean(env('MEDIA_OPS_FOLDER_MOVE_ENABLED', 'false')),
  nextcloudAlbumWritebackEnabled: parseBoolean(env('MEDIA_OPS_NEXTCLOUD_ALBUM_WRITEBACK_ENABLED', 'false')),
  nextcloudContainerName: env('MEDIA_OPS_NEXTCLOUD_CONTAINER_NAME', 'nextcloud'),
  internalEventSecret: env('MEDIA_OPS_INTERNAL_EVENT_SECRET', ''),
  nextcloudTrashSyncEnabled: parseBoolean(env('MEDIA_OPS_NEXTCLOUD_TRASH_SYNC_ENABLED', 'false')),
  nextcloudTrashRestoreEnabled: parseBoolean(env('MEDIA_OPS_NEXTCLOUD_TRASH_RESTORE_ENABLED', 'false')),
  smartAlbumsEnabled: parseBoolean(env('MEDIA_OPS_SMART_ALBUMS_ENABLED', 'false')),
  smartAlbumsDryRun: parseBoolean(env('MEDIA_OPS_SMART_ALBUMS_DRY_RUN', 'true')),
  smartAlbumsDocumentsEnabled: parseBoolean(env('MEDIA_OPS_SMART_ALBUMS_DOCUMENTS_ENABLED', 'true')),
  smartAlbumsScreenshotsEnabled: parseBoolean(env('MEDIA_OPS_SMART_ALBUMS_SCREENSHOTS_ENABLED', 'true')),
  smartAlbumsWhatsAppEnabled: parseBoolean(env('MEDIA_OPS_SMART_ALBUMS_WHATSAPP_ENABLED', 'true')),
  utilityTimezone: env('MEDIA_OPS_UTILITY_TIMEZONE', 'Europe/Zagreb'),
  dbHostname: env('DB_HOSTNAME', 'postgis'),
  dbUsername: env('DB_USERNAME', 'immich'),
  dbPassword: env('DB_PASSWORD', ''),
  dbDatabaseName: env('DB_DATABASE_NAME', 'immich'),
};

const managedStatePath = path.join(config.bridgeStateDir, 'managed-state.json');
const credentialsPath = path.join(config.bridgeStateDir, 'credentials.json');
const operationsStatePath = path.join(config.stateDir, 'operations-state.json');
const lastOperationPath = path.join(config.stateDir, 'last-operation.json');
const auditLogPath = path.join(config.stateDir, 'audit.log');
const trashSyncStatePath = path.join(config.stateDir, 'trash-sync-state.json');
const deleteLookupIndexPath = path.join(config.stateDir, 'delete-lookup-index.json');
const utilityStatePath = path.join(config.stateDir, 'utility-state.json');
const utilitiesHtmlPath = '/app/utilities.html';
const deleteLookupTtlMs = 30 * 24 * 60 * 60 * 1000;
const utilityRowsCacheTtlMs = 5 * 60 * 1000;
const utilityCandidateQueueCacheTtlMs = 5 * 60 * 1000;
const SUPPORTED_UTILITY_EXTENSIONS = new Set(['jpg', 'jpeg', 'png', 'webp', 'gif', 'heic', 'heif']);
const SMART_ALBUM_DEFINITIONS = Object.freeze([
  { name: 'No Faces', matcher: (input) => input.faceCount === 0 },
  { name: 'Screenshots', matcher: (input) => config.smartAlbumsScreenshotsEnabled && isScreenshotCandidate(input.fileName) },
  { name: 'WhatsApp', matcher: (input) => config.smartAlbumsWhatsAppEnabled && isWhatsAppCandidate(input.fileName) },
  {
    name: 'Documents',
    matcher: (input) =>
      config.smartAlbumsDocumentsEnabled && input.faceCount === 0 && isDocumentCandidate(input.fileName),
  },
]);

fs.mkdirSync(config.stateDir, { recursive: true });
const utilityRowsCache = new Map();
const utilityCandidateQueueCache = new Map();
const utilityBackgroundJobs = new Map();
const gpsUtilityRowsCache = new Map();
const gpsUtilityCandidateQueueCache = new Map();
const gpsUtilityBackgroundJobs = new Map();
const gpsUtilityWritebackQueues = new Map();

async function main() {
  const { command, args } = parseCommand(process.argv.slice(2));

  if (command === 'capabilities') {
    process.stdout.write(`${JSON.stringify(getCapabilities(), null, 2)}\n`);
    return;
  }

  if (command === 'serve') {
    await serve();
    return;
  }

  if (command === 'run' && args[0]) {
    const payload = JSON.parse(fs.readFileSync(args[0], 'utf8'));
    const result = await dispatchOperation(payload);
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return;
  }

  throw new Error('Usage: node /app/server.js [serve|capabilities|run <json-file>]');
}

async function serve() {
  const server = http.createServer(async (request, response) => {
    try {
      const requestUrl = new URL(request.url || '/', 'http://media-operations.local');
      const utilityApiPath = requestUrl.pathname.replace(/^\/utilities\/exifdatefix\/api/, '/utility/exifdatefix/api');
      const gpsUtilityApiPath = requestUrl.pathname.replace(/^\/utilities\/exifgpsfix\/api/, '/utility/exifgpsfix/api');

      if (request.method === 'GET' && request.url === '/healthz') {
        return respondJson(response, 200, {
          status: 'ok',
          dryRun: config.dryRun,
          writebackEnabled: config.writebackEnabled,
          deleteEnabled: config.deleteEnabled,
          folderMoveEnabled: config.folderMoveEnabled,
          nextcloudAlbumWritebackEnabled: config.nextcloudAlbumWritebackEnabled,
          nextcloudContainerName: config.nextcloudContainerName,
          nextcloudTrashSyncEnabled: config.nextcloudTrashSyncEnabled,
          nextcloudTrashRestoreEnabled: config.nextcloudTrashRestoreEnabled,
          immichEventWebhookEnabled: Boolean(config.internalEventSecret),
          smartAlbumsEnabled: config.smartAlbumsEnabled,
          smartAlbumsDryRun: config.smartAlbumsDryRun,
        });
      }

      if (request.method === 'GET' && request.url === '/capabilities') {
        return respondJson(response, 200, getCapabilities());
      }

      if (request.method === 'GET' && (requestUrl.pathname === '/utility/exifdatefix' || requestUrl.pathname === '/utility/exifdatefix/')) {
        return respondHtml(response, 200, fs.readFileSync(utilitiesHtmlPath, 'utf8'));
      }

      if (request.method === 'GET' && utilityApiPath === '/utility/exifdatefix/api/date-from-filename/next') {
        const payload = await buildDateFromFilenameResponse(request);
        return respondJson(response, 200, payload);
      }

      if (request.method === 'GET' && utilityApiPath === '/utility/exifdatefix/api/date-from-filename/status') {
        const payload = await buildDateFromFilenameResponse(request, { includeCandidate: false });
        return respondJson(response, 200, payload);
      }

      if (request.method === 'GET' && utilityApiPath === '/utility/exifdatefix/api/date-from-filename/background-status') {
        const payload = await buildDateFromFilenameBackgroundStatusResponse(request);
        return respondJson(response, 200, payload);
      }

      if (request.method === 'POST' && utilityApiPath === '/utility/exifdatefix/api/date-from-filename/skip') {
        verifyUtilityMutationRequest(request);
        const payload = await readJsonBody(request);
        const result = await handleDateFromFilenameSkip(request, payload);
        return respondJson(response, 200, result);
      }

      if (request.method === 'POST' && utilityApiPath === '/utility/exifdatefix/api/date-from-filename/apply') {
        verifyUtilityMutationRequest(request);
        const payload = await readJsonBody(request);
        const result = await handleDateFromFilenameApply(request, payload);
        return respondJson(response, 200, result);
      }

      if (request.method === 'POST' && utilityApiPath === '/utility/exifdatefix/api/date-from-filename/delete') {
        verifyUtilityMutationRequest(request);
        const payload = await readJsonBody(request);
        const result = await handleDateFromFilenameDelete(request, payload);
        return respondJson(response, 200, result);
      }

      if (request.method === 'POST' && utilityApiPath === '/utility/exifdatefix/api/date-from-filename/background-apply') {
        verifyUtilityMutationRequest(request);
        const result = await handleDateFromFilenameBackgroundApply(request);
        return respondJson(response, 200, result);
      }

      if (request.method === 'GET' && gpsUtilityApiPath === '/utility/exifgpsfix/api/next') {
        const payload = await buildGpsFixResponse(request, {
          refresh: requestUrl.searchParams.get('refresh') === '1',
        });
        return respondJson(response, 200, payload);
      }

      if (request.method === 'GET' && gpsUtilityApiPath === '/utility/exifgpsfix/api/status') {
        const payload = await buildGpsFixResponse(request, { includeCandidate: false });
        return respondJson(response, 200, payload);
      }

      if (request.method === 'GET' && gpsUtilityApiPath === '/utility/exifgpsfix/api/background-status') {
        const payload = await buildGpsFixBackgroundStatusResponse(request);
        return respondJson(response, 200, payload);
      }

      if (request.method === 'GET' && gpsUtilityApiPath === '/utility/exifgpsfix/api/refresh') {
        const payload = await buildGpsFixRefreshResponse(request);
        return respondJson(response, 200, payload);
      }

      if (request.method === 'POST' && gpsUtilityApiPath === '/utility/exifgpsfix/api/skip') {
        verifyUtilityMutationRequest(request);
        const payload = await readJsonBody(request);
        const result = await handleGpsFixSkip(request, payload);
        return respondJson(response, 200, result);
      }

      if (request.method === 'POST' && gpsUtilityApiPath === '/utility/exifgpsfix/api/apply') {
        verifyUtilityMutationRequest(request);
        const payload = await readJsonBody(request);
        const result = await handleGpsFixApply(request, payload);
        return respondJson(response, 200, result);
      }

      if (request.method === 'POST' && gpsUtilityApiPath === '/utility/exifgpsfix/api/delete') {
        verifyUtilityMutationRequest(request);
        const payload = await readJsonBody(request);
        const result = await handleGpsFixDelete(request, payload);
        return respondJson(response, 200, result);
      }

      if (request.method === 'POST' && request.url === '/operations') {
        const payload = await readJsonBody(request);
        const result = await dispatchOperation(payload);
        return respondJson(response, 200, result);
      }

      if (request.method === 'POST' && request.url === '/internal/immich-events') {
        verifyInternalEventRequest(request);
        const payload = await readJsonBody(request);
        const result = await handleInternalImmichEvent(payload);
        return respondJson(response, 200, result);
      }

      respondJson(response, 404, { message: 'Not found' });
    } catch (error) {
      const statusCode = error.statusCode || 400;
      respondJson(response, statusCode, {
        message: error.message,
      });
    }
  });

  await new Promise((resolve) => server.listen(config.port, config.host, resolve));
  log(`media-operations listening on ${config.host}:${config.port}`);
}

function getCapabilities() {
  return {
    service: 'nextcloud-immich-media-operations',
    dryRun: config.dryRun,
    writebackEnabled: config.writebackEnabled,
    deleteEnabled: config.deleteEnabled,
    folderMoveEnabled: config.folderMoveEnabled,
    nextcloudAlbumWritebackEnabled: config.nextcloudAlbumWritebackEnabled,
    nextcloudContainerName: config.nextcloudContainerName,
    nextcloudTrashSyncEnabled: config.nextcloudTrashSyncEnabled,
    nextcloudTrashRestoreEnabled: config.nextcloudTrashRestoreEnabled,
    immichEventWebhookEnabled: Boolean(config.internalEventSecret),
    smartAlbumsEnabled: config.smartAlbumsEnabled,
    smartAlbumsDryRun: config.smartAlbumsDryRun,
    supportedSmartAlbums: getManagedSmartAlbumNames(),
    supported: [
      'create-album',
      'update-album',
      'delete-album',
      'add-assets-to-album',
      'remove-assets-from-album',
      'update-asset-metadata',
      'trash-assets',
      'confirm-delete-assets',
      'move-assets-to-folder',
      'immich-trash-webhook',
      'reconcile-smart-albums',
    ],
    destructiveOperationsRequireApply: [
      'confirm-delete-assets',
      'move-assets-to-folder',
    ],
    writebackMode: config.writebackEnabled ? 'opt-in-live' : 'audit-only',
  };
}

async function buildDateFromFilenameResponse(request, options = {}) {
  const includeCandidate = options.includeCandidate !== false;
  const context = await createUtilityContextFromRequest(request);
  const queue = getDateFromFilenameQueue(context, options);

  return {
    status: {
      remaining: queue.remaining,
      total: queue.total,
      applied: queue.applied,
      skipped: queue.skipped,
      deleted: queue.deleted,
    },
    candidate: includeCandidate ? queue.candidate : null,
  };
}

async function handleDateFromFilenameSkip(request, payload) {
  const context = await createUtilityContextFromRequest(request);
  const assetId = requiredString(payload.assetId, 'assetId');
  const queue = getDateFromFilenameQueue(context);

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active utility candidate');
  }

  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  userState.skipped[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
  };
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-date-from-filename-skip',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
  });

  markUtilityQueueAssetProcessed(context, assetId);
  return buildDateFromFilenameResponseFromContext(context);
}

async function handleDateFromFilenameApply(request, payload) {
  const context = await createUtilityContextFromRequest(request, { requireAuthenticatedContext: true });
  const assetId = requiredString(payload.assetId, 'assetId');
  const queue = getDateFromFilenameQueue(context);

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active utility candidate');
  }

  const assetRecord = getUtilityAssetRecord(context, assetId);
  if (!shouldOfferDateCorrection(assetRecord, queue.candidate)) {
    throw httpError(409, 'Asset no longer requires a date correction');
  }

  applyExifDateTime(assetRecord.originalPath, queue.candidate.proposedDateTimeExif, getFileExtension(assetRecord.fileName));

  await immichRequest(context.accessToken, 'PUT', '/assets', {
    ids: [assetId],
    dateTimeOriginal: queue.candidate.proposedDateTimeIso,
  });

  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  userState.applied[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
    originalPath: assetRecord.originalPath,
  };
  delete userState.skipped[assetId];
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-date-from-filename-apply',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
  });

  markUtilityQueueAssetProcessed(context, assetId);
  return buildDateFromFilenameResponseFromContext(context);
}

async function handleDateFromFilenameDelete(request, payload) {
  const context = await createUtilityContextFromRequest(request, { requireAuthenticatedContext: true });
  const assetId = requiredString(payload.assetId, 'assetId');
  const queue = getDateFromFilenameQueue(context);

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active utility candidate');
  }

  const assetRecord = getUtilityAssetRecord(context, assetId);
  await immichRequest(context.accessToken, 'DELETE', '/assets', {
    ids: [assetId],
    force: false,
  });

  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  userState.deleted[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath || null,
    deleteMode: 'trash',
  };
  delete userState.skipped[assetId];
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-date-from-filename-delete',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath || null,
    deleteMode: 'trash',
  });

  markUtilityQueueAssetProcessed(context, assetId);
  return buildDateFromFilenameResponseFromContext(context);
}

async function buildDateFromFilenameBackgroundStatusResponse(request) {
  const context = await createUtilityContextFromRequest(request);
  const job = getUtilityBackgroundJob(context.nextcloudUserId);
  return {
    job: job ? sanitizeUtilityBackgroundJob(job) : null,
  };
}

async function handleDateFromFilenameBackgroundApply(request) {
  const context = await createUtilityContextFromRequest(request, { requireAuthenticatedContext: true });
  const existing = getUtilityBackgroundJob(context.nextcloudUserId);
  if (existing && existing.status === 'running') {
    return {
      started: false,
      job: sanitizeUtilityBackgroundJob(existing),
    };
  }

  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  const queue = getDateFromFilenameQueue(context);
  const job = {
    id: crypto.randomUUID(),
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    status: 'running',
    startedAt: new Date().toISOString(),
    finishedAt: null,
    totalAtStart: queue.remaining,
    processed: 0,
    applied: 0,
    failed: Object.keys(userState.failed || {}).length,
    deleted: Object.keys(userState.deleted || {}).length,
    skipped: Object.keys(userState.skipped || {}).length,
    lastAssetId: null,
    lastFileName: null,
    lastError: null,
  };
  utilityBackgroundJobs.set(context.nextcloudUserId, job);
  void runUtilityBackgroundApplyJob(context.nextcloudUserId, job.id);

  return {
    started: true,
    job: sanitizeUtilityBackgroundJob(job),
  };
}

function buildDateFromFilenameResponseFromContext(context, options = {}) {
  const includeCandidate = options.includeCandidate !== false;
  const queue = getDateFromFilenameQueue(context, options);

  return {
    status: {
      remaining: queue.remaining,
      total: queue.total,
      applied: queue.applied,
      skipped: queue.skipped,
      deleted: queue.deleted,
      failed: queue.failed,
    },
    candidate: includeCandidate ? queue.candidate : null,
  };
}

async function createUtilityContextFromRequest(request, options = {}) {
  const sessionUser = await getImmichSessionUser(request);
  const baseContext = await createUserContextByImmichUserId(sessionUser.id);
  if (!options.requireAuthenticatedContext) {
    return baseContext;
  }

  return {
    ...baseContext,
    accessToken: {
      sessionCookie: request.headers.cookie,
    },
  };
}

async function getImmichSessionUser(request) {
  const proxiedUserIdHeader = request.headers['x-immich-user-id'];
  const proxySecretHeader = request.headers['x-media-ops-proxy-secret'];
  const proxiedUserId = Array.isArray(proxiedUserIdHeader) ? proxiedUserIdHeader[0] : proxiedUserIdHeader;
  const proxySecret = Array.isArray(proxySecretHeader) ? proxySecretHeader[0] : proxySecretHeader;
  if (config.internalEventSecret && proxySecret === config.internalEventSecret && proxiedUserId) {
    return { id: proxiedUserId };
  }

  const cookie = request.headers.cookie;
  if (!cookie) {
    throw httpError(401, 'Authentication required');
  }

  const response = await fetch(`${config.immichApiUrl}/users/me`, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Cookie: cookie,
    },
  });

  const text = await response.text();
  const data = text ? safeJsonParse(text) : null;
  if (response.status === 401) {
    throw httpError(401, 'Authentication required');
  }
  if (!response.ok || !data?.id) {
    throw httpError(403, `Unable to resolve authenticated Immich user: ${text}`);
  }
  return data;
}

function getDateFromFilenameQueue(context, options = {}) {
  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);

  if (!options.rows && !options.excludeAssetIds?.length) {
    const materializedQueue = getMaterializedUtilityCandidateQueue(context, userState);
    return {
      candidate: materializedQueue.candidates[0] || null,
      remaining: materializedQueue.candidates.length,
      total: materializedQueue.total,
      applied: Object.keys(userState.applied || {}).length,
      skipped: Object.keys(userState.skipped || {}).length,
      deleted: Object.keys(userState.deleted || {}).length,
      failed: Object.keys(userState.failed || {}).length,
    };
  }

  const rows = options.rows || getDateFromFilenameRows(context);
  const excludedAssetIds = new Set(options.excludeAssetIds || []);
  const candidates = buildUtilityCandidates(rows, userState, excludedAssetIds);

  return {
    candidate: candidates[0] || null,
    remaining: candidates.length,
    total: rows.length,
    applied: Object.keys(userState.applied || {}).length,
    skipped: Object.keys(userState.skipped || {}).length,
    deleted: Object.keys(userState.deleted || {}).length,
    failed: Object.keys(userState.failed || {}).length,
  };
}

function getMaterializedUtilityCandidateQueue(context, userState) {
  const cacheKey = buildUtilityRowsCacheKey(context);
  const cached = utilityCandidateQueueCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached;
  }

  const rows = getDateFromFilenameRows(context);
  const materialized = {
    total: rows.length,
    candidates: buildUtilityCandidates(rows, userState),
    expiresAt: Date.now() + utilityCandidateQueueCacheTtlMs,
  };
  utilityCandidateQueueCache.set(cacheKey, materialized);
  return materialized;
}

function shouldExcludeFailedUtilityCandidate(userState, assetId) {
  const failure = userState.failed && userState.failed[assetId];
  if (!failure) {
    return false;
  }
  if (failure.permanent) {
    return true;
  }
  if (failure.nextRetryAt) {
    const nextRetryAt = Date.parse(failure.nextRetryAt);
    if (Number.isFinite(nextRetryAt) && nextRetryAt > Date.now()) {
      return true;
    }
  }
  return false;
}

function buildUtilityCandidates(rows, userState, excludedAssetIds = new Set()) {
  const candidates = [];

  for (const row of rows) {
    const candidate = mapUtilityRowToCandidate(row);
    if (!candidate) {
      continue;
    }
    if (excludedAssetIds.has(candidate.assetId)) {
      continue;
    }
    if (
      (userState.applied && userState.applied[candidate.assetId]) ||
      (userState.skipped && userState.skipped[candidate.assetId]) ||
      (userState.deleted && userState.deleted[candidate.assetId]) ||
      shouldExcludeFailedUtilityCandidate(userState, candidate.assetId)
    ) {
      continue;
    }
    candidates.push(candidate);
  }

  return candidates;
}

function markUtilityQueueAssetProcessed(context, assetId) {
  const cacheKey = buildUtilityRowsCacheKey(context);
  const cached = utilityCandidateQueueCache.get(cacheKey);
  if (!cached) {
    return;
  }

  cached.candidates = cached.candidates.filter((candidate) => candidate.assetId !== assetId);
  cached.expiresAt = Date.now() + utilityCandidateQueueCacheTtlMs;
}

function getUtilityBackgroundJob(nextcloudUserId) {
  const job = utilityBackgroundJobs.get(nextcloudUserId);
  if (!job) {
    return null;
  }
  return job;
}

function sanitizeUtilityBackgroundJob(job) {
  return {
    id: job.id,
    status: job.status,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    totalAtStart: job.totalAtStart,
    processed: job.processed,
    applied: job.applied,
    failed: job.failed,
    skipped: job.skipped,
    deleted: job.deleted,
    lastAssetId: job.lastAssetId,
    lastFileName: job.lastFileName,
    lastError: job.lastError,
  };
}

function isRetryableUtilityBackgroundError(error) {
  const message = String(error?.message || error || '').toLowerCase();
  return (
    message.includes('fetch failed') ||
    message.includes('econnreset') ||
    message.includes('econnrefused') ||
    message.includes('etimedout') ||
    message.includes('socket hang up') ||
    message.includes('502') ||
    message.includes('503') ||
    message.includes('504')
  );
}

async function sleepMs(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function runUtilityBackgroundApplyJob(nextcloudUserId, jobId) {
  const job = utilityBackgroundJobs.get(nextcloudUserId);
  if (!job || job.id !== jobId) {
    return;
  }

  try {
    const context = await createUserContext(nextcloudUserId);

    while (true) {
      const queue = getDateFromFilenameQueue(context);
      const candidate = queue.candidate;
      if (!candidate) {
        job.status = 'completed';
        job.finishedAt = new Date().toISOString();
        job.lastError = null;
        return;
      }

      job.lastAssetId = candidate.assetId;
      job.lastFileName = candidate.fileName;
      let shouldKeepCandidateQueued = false;

      try {
        let assetRecord = null;
        let lastAttemptError = null;
        const maxAttempts = 3;

        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
          try {
            assetRecord = getUtilityAssetRecord(context, candidate.assetId);
            if (!shouldOfferDateCorrection(assetRecord, candidate)) {
              throw new Error('Asset no longer requires a date correction');
            }

            applyExifDateTime(assetRecord.originalPath, candidate.proposedDateTimeExif, getFileExtension(assetRecord.fileName));
            await immichRequest(context.accessToken, 'PUT', '/assets', {
              ids: [candidate.assetId],
              dateTimeOriginal: candidate.proposedDateTimeIso,
            });
            lastAttemptError = null;
            break;
          } catch (error) {
            lastAttemptError = error;
            if (attempt >= maxAttempts || !isRetryableUtilityBackgroundError(error)) {
              throw error;
            }
            await sleepMs(750 * attempt);
          }
        }

        const state = loadUtilityState();
        const userState = ensureUtilityUserState(state, context.nextcloudUserId);
        userState.applied[candidate.assetId] = {
          decidedAt: new Date().toISOString(),
          fileName: candidate.fileName,
          proposedDateTimeIso: candidate.proposedDateTimeIso,
          parserReason: candidate.parserReason,
          originalPath: assetRecord.originalPath,
          background: true,
        };
        delete userState.skipped[candidate.assetId];
        delete userState.failed[candidate.assetId];
        saveUtilityState(state);

        writeAuditEntry({
          kind: 'utility-date-from-filename-apply-background',
          nextcloudUserId: context.nextcloudUserId,
          immichUserId: context.immichUserId,
          assetId: candidate.assetId,
          fileName: candidate.fileName,
          originalPath: assetRecord.originalPath,
          proposedDateTimeIso: candidate.proposedDateTimeIso,
          parserReason: candidate.parserReason,
        });

        job.applied += 1;
        job.lastError = null;
      } catch (error) {
        const state = loadUtilityState();
        const userState = ensureUtilityUserState(state, context.nextcloudUserId);
        const previousFailure = userState.failed[candidate.assetId] || {};
        const nextAttemptCount = Number(previousFailure.attempts || 0) + 1;
        const isRetryable = isRetryableUtilityBackgroundError(error);
        const shouldRetryLater = isRetryable && nextAttemptCount < 10;
        const nextRetryAt = shouldRetryLater ? new Date(Date.now() + Math.min(60000, 5000 * nextAttemptCount)).toISOString() : null;
        shouldKeepCandidateQueued = shouldRetryLater;
        userState.failed[candidate.assetId] = {
          decidedAt: new Date().toISOString(),
          fileName: candidate.fileName,
          message: error.message,
          attempts: nextAttemptCount,
          permanent: !shouldRetryLater,
          nextRetryAt,
          background: true,
        };
        saveUtilityState(state);

        writeAuditEntry({
          kind: 'utility-date-from-filename-background-error',
          nextcloudUserId: context.nextcloudUserId,
          immichUserId: context.immichUserId,
          assetId: candidate.assetId,
          fileName: candidate.fileName,
          message: error.message,
          retryScheduled: shouldRetryLater,
          nextRetryAt,
        });

        job.failed += 1;
        job.lastError = error.message;
      }

      job.processed += 1;
      if (!shouldKeepCandidateQueued) {
        markUtilityQueueAssetProcessed(context, candidate.assetId);
      }
    }
  } catch (error) {
    job.status = 'failed';
    job.finishedAt = new Date().toISOString();
    job.lastError = error.message;
  }
}

function getDateFromFilenameRows(context) {
  const cacheKey = buildUtilityRowsCacheKey(context);
  const cached = utilityRowsCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.rows;
  }

  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      a."originalPath",
      a."fileCreatedAt",
      a."localDateTime",
      ae."dateTimeOriginal"
    from asset a
    left join asset_exif ae on ae."assetId" = a.id
    where a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and a.type = 'IMAGE'
      and (${libraryPathCondition})
      and lower(a."originalFileName") ~ '\\.(jpg|jpeg|png|webp|gif|heic|heif)$'
      and lower(a."originalFileName") ~ '(screenshot_|pxl_|signal-|fb_img_\\d{13}|face_sc_\\d{13}|picplus_\\d{13}|img[-_]\\d{8}-wa|vid[-_]\\d{8}-wa|\\d{8}[_ -]\\d{6}|\\d{4}-\\d{2}-\\d{2}[ _. -]\\d{2}[.:_-]\\d{2}[.:_-]\\d{2})'
    order by a."fileCreatedAt" asc, a.id asc;
  `;

  const rows = runPostgresRowsQuery(sql).map((line) => {
    const [assetId, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal] = line.split('\t');
    return { assetId, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal };
  });
  utilityRowsCache.set(cacheKey, {
    rows,
    expiresAt: Date.now() + utilityRowsCacheTtlMs,
  });
  return rows;
}

function buildUtilityRowsCacheKey(context) {
  return `${context.nextcloudUserId}:${context.immichUserId}`;
}

function getUtilityAssetRecord(context, assetId) {
  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      a."originalPath",
      a."fileCreatedAt",
      a."localDateTime",
      ae."dateTimeOriginal"
    from asset a
    left join asset_exif ae on ae."assetId" = a.id
    where a.id = ${sqlString(assetId)}
      and a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and (${libraryPathCondition})
    limit 1;
  `;
  const row = runPostgresQuery(sql);
  if (!row) {
    throw httpError(404, `Asset ${assetId} not found`);
  }
  const [id, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal] = row.split('|');
  return { assetId: id, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal };
}

function mapUtilityRowToCandidate(row) {
  const extension = getFileExtension(row.fileName);
  if (!SUPPORTED_UTILITY_EXTENSIONS.has(extension)) {
    return null;
  }

  const parsed = parseDateFromFilename(row.fileName, row.fileCreatedAt, row.localDateTime);
  if (!parsed) {
    return null;
  }

  const currentDateInfo = normalizeStoredDateTime(row.dateTimeOriginal);
  const candidateKind = determineCandidateKind(parsed, currentDateInfo);
  if (!candidateKind) {
    return null;
  }

  const parserReason = candidateKind === 'missing'
    ? parsed.reason
    : `${parsed.reason} · postojeci datum izgleda sumnjivo`;

  return {
    assetId: row.assetId,
    fileName: row.fileName,
    previewUrl: `/api/assets/${row.assetId}/thumbnail?size=preview`,
    candidateKind,
    currentDateTimeIso: currentDateInfo?.iso || null,
    currentDateTimeLocal: currentDateInfo?.local || 'Nema datuma',
    proposedDateTimeIso: parsed.iso,
    proposedDateTimeExif: parsed.exif,
    proposedDateTimeLocal: parsed.local,
    parserReason,
    confidence: parsed.confidence,
  };
}

function parseDateFromFilename(fileName, fileCreatedAt, localDateTime) {
  const baseName = String(fileName || '').replace(/\.[^.]+$/, '');
  const fallbackTime = extractFallbackTime(fileCreatedAt || localDateTime);
  const patterns = [
    {
      regex: /(?:^|[^0-9])(\d{4})(\d{2})(\d{2})[_ -](\d{2})(\d{2})(\d{2})(?:\d{3})?(?:[^0-9]|$)/,
      reason: 'Prepoznat obrazac YYYYMMDD_HHMMSS',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])(\d{4})-(\d{2})-(\d{2})[ _. -](\d{2})[.:_-](\d{2})[.:_-](\d{2})(?:[^0-9]|$)/,
      reason: 'Prepoznat obrazac YYYY-MM-DD HH.MM.SS',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])PXL_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(?:[^0-9]|$)/i,
      reason: 'Prepoznat PXL obrazac',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])Screenshot_(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})(?:[^0-9]|$)/i,
      reason: 'Prepoznat Screenshot obrazac',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])signal-(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})(\d{2})(?:[^0-9]|$)/i,
      reason: 'Prepoznat Signal obrazac',
      confidence: 'high',
    },
  ];

  for (const pattern of patterns) {
    const match = baseName.match(pattern.regex);
    if (!match) {
      continue;
    }
    return buildParsedDateTime(match.slice(1, 7), pattern.reason, pattern.confidence);
  }

  const whatsappMatch = baseName.match(/(?:IMG|VID)[-_](\d{4})(\d{2})(\d{2})[-_]WA\d+/i);
  if (whatsappMatch && fallbackTime) {
    return buildParsedDateTime(
      [whatsappMatch[1], whatsappMatch[2], whatsappMatch[3], fallbackTime.hour, fallbackTime.minute, fallbackTime.second],
      'Prepoznat WhatsApp datum, vrijeme preuzeto iz fileCreatedAt',
      'medium',
    );
  }

  const epochMatch = baseName.match(/(?:^|[^0-9])(?:FB_IMG|FACE_SC|PicPlus)_(\d{13})(?:[^0-9]|$)/i);
  if (epochMatch) {
    return buildParsedDateTimeFromEpochMilliseconds(
      epochMatch[1],
      'Prepoznat epoch timestamp u nazivu datoteke',
      'high',
    );
  }

  return null;
}

function buildParsedDateTime(parts, reason, confidence) {
  const [year, month, day, hour, minute, second] = parts.map((value) => String(value).padStart(2, '0'));
  const local = `${year}-${month}-${day} ${hour}:${minute}:${second}`;
  const iso = `${year}-${month}-${day}T${hour}:${minute}:${second}${resolveTimezoneOffset(year, month, day)}`;
  const exif = `${year}:${month}:${day} ${hour}:${minute}:${second}`;
  return { iso, exif, local, date: `${year}-${month}-${day}`, reason, confidence };
}

function buildParsedDateTimeFromEpochMilliseconds(epochMilliseconds, reason, confidence) {
  const timestamp = Number(epochMilliseconds);
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: config.utilityTimezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  const parts = Object.fromEntries(
    formatter
      .formatToParts(new Date(timestamp))
      .filter((item) => item.type !== 'literal')
      .map((item) => [item.type, item.value]),
  );

  return buildParsedDateTime(
    [parts.year, parts.month, parts.day, parts.hour, parts.minute, parts.second],
    reason,
    confidence,
  );
}

function buildParsedDateTimeFromManualInput(dateValue, timeValue) {
  if (!dateValue && !timeValue) {
    return null;
  }
  if (!dateValue || !timeValue) {
    throw httpError(400, 'Date and time must both be provided');
  }

  const dateMatch = String(dateValue).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  const timeMatch = String(timeValue).match(/^(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (!dateMatch || !timeMatch) {
    throw httpError(400, 'Invalid date or time format');
  }

  return buildParsedDateTime(
    [dateMatch[1], dateMatch[2], dateMatch[3], timeMatch[1], timeMatch[2], timeMatch[3] || '00'],
    'Rucno odabran datum i vrijeme',
    'manual',
  );
}

function extractFallbackTime(value) {
  if (!value) {
    return null;
  }
  const match = String(value).match(/T?(\d{2}):(\d{2}):(\d{2})/);
  if (!match) {
    return null;
  }
  return { hour: match[1], minute: match[2], second: match[3] };
}

function normalizeStoredDateTime(value) {
  if (!value) {
    return null;
  }
  const normalized = String(value).replace('T', ' ');
  const iso = normalized.includes('T') ? normalized : normalized.replace(' ', 'T');
  const dateMatch = normalized.match(/^(\d{4}-\d{2}-\d{2})/);
  return {
    iso,
    local: normalized,
    date: dateMatch ? dateMatch[1] : null,
  };
}

function determineCandidateKind(parsed, currentDateInfo) {
  if (!currentDateInfo?.date) {
    return 'missing';
  }
  return currentDateInfo.date !== parsed.date ? 'mismatch' : null;
}

function shouldOfferDateCorrection(assetRecord, candidate) {
  const currentDateInfo = normalizeStoredDateTime(assetRecord.dateTimeOriginal);
  return determineCandidateKind({
    date: candidate.proposedDateTimeIso.slice(0, 10),
  }, currentDateInfo) !== null;
}

function buildLibraryPathSqlCondition(context) {
  const libraryPaths = getManagedLibraryPaths(context);
  return libraryPaths.map((libraryPath) => `a."originalPath" like ${sqlString(`${libraryPath}%`)}`).join(' or ');
}

function getManagedLibraryPaths(context) {
  const paths = new Set();
  if (context.libraryPath) {
    paths.add(context.libraryPath);
  }
  const libraries = context.stateEntry?.libraries && typeof context.stateEntry.libraries === 'object'
    ? Object.values(context.stateEntry.libraries)
    : [];
  for (const library of libraries) {
    if (library?.libraryPath) {
      paths.add(library.libraryPath);
    }
  }
  if (paths.size === 0) {
    throw httpError(400, `Managed user is missing library paths: ${context.nextcloudUserId}`);
  }
  return Array.from(paths);
}

function resolveTimezoneOffset(year, month, day) {
  const sample = new Date(`${year}-${month}-${day}T12:00:00Z`);
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: config.utilityTimezone,
    timeZoneName: 'longOffset',
  });
  const part = formatter.formatToParts(sample).find((item) => item.type === 'timeZoneName');
  const offset = part?.value?.replace('GMT', '') || '+00:00';
  return offset === '' ? '+00:00' : offset;
}

function applyExifDateTime(originalPath, exifDateTime, extension) {
  const command = [
    '-overwrite_original',
    '-P',
    `-DateTimeOriginal=${exifDateTime}`,
    `-CreateDate=${exifDateTime}`,
    `-ModifyDate=${exifDateTime}`,
  ];

  if (extension === 'heic' || extension === 'heif') {
    command.push(`-QuickTime:CreateDate=${exifDateTime}`, `-QuickTime:ModifyDate=${exifDateTime}`);
  }

  command.push(originalPath);

  try {
    execFileSync('exiftool', command, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } catch (error) {
    const output = `${error.stdout || ''}\n${error.stderr || ''}`.trim();
    throw httpError(500, `EXIF writeback failed: ${output || error.message}`);
  }
}

function applyExifGps(originalPath, latitude, longitude, extension) {
  const normalizedLatitude = Number(latitude);
  const normalizedLongitude = Number(longitude);
  if (!Number.isFinite(normalizedLatitude) || !Number.isFinite(normalizedLongitude)) {
    throw httpError(400, 'Invalid GPS coordinates');
  }

  const command = [
    '-overwrite_original',
    '-P',
    `-GPSLatitude=${Math.abs(normalizedLatitude)}`,
    `-GPSLatitudeRef=${normalizedLatitude < 0 ? 'S' : 'N'}`,
    `-GPSLongitude=${Math.abs(normalizedLongitude)}`,
    `-GPSLongitudeRef=${normalizedLongitude < 0 ? 'W' : 'E'}`,
  ];

  if (extension === 'heic' || extension === 'heif') {
    command.push(`-QuickTime:GPSCoordinates=${normalizedLatitude} ${normalizedLongitude}`);
  }

  command.push(originalPath);

  try {
    execFileSync('exiftool', command, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } catch (error) {
    const output = `${error.stdout || ''}\n${error.stderr || ''}`.trim();
    throw httpError(500, `EXIF GPS writeback failed: ${output || error.message}`);
  }
}

function loadUtilityState() {
  const state = loadJson(utilityStatePath, {
    dateFromFilename: { users: {} },
    gpsFix: { users: {} },
  });
  const users = state.dateFromFilename && typeof state.dateFromFilename === 'object' ? state.dateFromFilename.users : {};
  state.dateFromFilename = {
    users: users && typeof users === 'object' ? users : {},
  };
  const gpsUsers = state.gpsFix && typeof state.gpsFix === 'object' ? state.gpsFix.users : {};
  state.gpsFix = {
    users: gpsUsers && typeof gpsUsers === 'object' ? gpsUsers : {},
  };
  return state;
}

function saveUtilityState(state) {
  fs.writeFileSync(utilityStatePath, JSON.stringify(state, null, 2));
}

function ensureUtilityUserState(state, nextcloudUserId) {
  const users = state.dateFromFilename.users;
  users[nextcloudUserId] = users[nextcloudUserId] || { applied: {}, skipped: {}, deleted: {}, failed: {} };
  users[nextcloudUserId].applied = users[nextcloudUserId].applied && typeof users[nextcloudUserId].applied === 'object' ? users[nextcloudUserId].applied : {};
  users[nextcloudUserId].skipped = users[nextcloudUserId].skipped && typeof users[nextcloudUserId].skipped === 'object' ? users[nextcloudUserId].skipped : {};
  users[nextcloudUserId].deleted = users[nextcloudUserId].deleted && typeof users[nextcloudUserId].deleted === 'object' ? users[nextcloudUserId].deleted : {};
  users[nextcloudUserId].failed = users[nextcloudUserId].failed && typeof users[nextcloudUserId].failed === 'object' ? users[nextcloudUserId].failed : {};
  return users[nextcloudUserId];
}

function ensureGpsUtilityUserState(state, nextcloudUserId) {
  const users = state.gpsFix.users;
  users[nextcloudUserId] = users[nextcloudUserId] || { applied: {}, skipped: {}, failed: {} };
  users[nextcloudUserId].applied = users[nextcloudUserId].applied && typeof users[nextcloudUserId].applied === 'object' ? users[nextcloudUserId].applied : {};
  users[nextcloudUserId].skipped = users[nextcloudUserId].skipped && typeof users[nextcloudUserId].skipped === 'object' ? users[nextcloudUserId].skipped : {};
  users[nextcloudUserId].failed = users[nextcloudUserId].failed && typeof users[nextcloudUserId].failed === 'object' ? users[nextcloudUserId].failed : {};
  return users[nextcloudUserId];
}

async function buildGpsFixResponse(request, options = {}) {
  const includeCandidate = options.includeCandidate !== false;
  const context = await createUtilityContextFromRequest(request);
  if (options.refresh) {
    invalidateGpsFixCaches(context);
  }
  const queue = getGpsFixQueue(context, options);

  return {
    status: {
      remaining: queue.remaining,
      total: queue.total,
      applied: queue.applied,
      skipped: queue.skipped,
      failed: queue.failed,
    },
    candidate: includeCandidate ? queue.candidate : null,
    job: sanitizeGpsUtilityBackgroundJob(getGpsUtilityBackgroundJob(context.nextcloudUserId)),
  };
}

async function buildGpsFixRefreshResponse(request) {
  const requestUrl = new URL(request.url || '/', 'http://media-operations.local');
  const assetId = requiredString(requestUrl.searchParams.get('assetId'), 'assetId');
  const context = await createUtilityContextFromRequest(request);
  invalidateGpsFixCaches(context);

  const rows = getGpsFixRows(context);
  const row = rows.find((entry) => entry.assetId === assetId);
  const assetRecord = row || getGpsUtilityAssetRecord(context, assetId);

  const state = loadUtilityState();
  const userState = ensureGpsUtilityUserState(state, context.nextcloudUserId);

  return {
    status: {
      remaining: getGpsFixQueue(context, { rows }).remaining,
      total: rows.length,
      applied: Object.keys(userState.applied || {}).length,
      skipped: Object.keys(userState.skipped || {}).length,
      failed: Object.keys(userState.failed || {}).length,
    },
    candidate: buildGpsFixCandidate(context, {
      assetId: assetRecord.assetId,
      fileName: assetRecord.fileName,
      originalPath: assetRecord.originalPath,
      fileCreatedAt: assetRecord.fileCreatedAt,
      localDateTime: assetRecord.localDateTime,
      dateTimeOriginal: assetRecord.dateTimeOriginal,
      latitude: assetRecord.latitude,
      longitude: assetRecord.longitude,
      sortDateTime: assetRecord.dateTimeOriginal || assetRecord.localDateTime || assetRecord.fileCreatedAt || null,
    }),
    job: sanitizeGpsUtilityBackgroundJob(getGpsUtilityBackgroundJob(context.nextcloudUserId)),
  };
}

async function buildGpsFixBackgroundStatusResponse(request) {
  const context = await createUtilityContextFromRequest(request);
  return {
    job: sanitizeGpsUtilityBackgroundJob(getGpsUtilityBackgroundJob(context.nextcloudUserId)),
  };
}

async function handleGpsFixSkip(request, payload) {
  const context = await createUtilityContextFromRequest(request);
  const assetId = requiredString(payload.assetId, 'assetId');
  const rows = getGpsFixRows(context);
  const queue = getGpsFixQueue(context, { rows });

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active GPS utility candidate');
  }

  const state = loadUtilityState();
  const userState = ensureGpsUtilityUserState(state, context.nextcloudUserId);
  userState.skipped[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    currentDateTimeIso: queue.candidate.currentDateTimeIso,
  };
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-gps-fix-skip',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
  });

  markGpsUtilityQueueAssetProcessed(context, assetId);
  return buildGpsFixResponseFromContext(context, { rows, excludeAssetIds: [assetId] });
}

async function handleGpsFixApply(request, payload) {
  const context = await createUtilityContextFromRequest(request, { requireAuthenticatedContext: true });
  const assetId = requiredString(payload.assetId, 'assetId');
  const latitude = optionalNumber(payload.latitude);
  const longitude = optionalNumber(payload.longitude);
  const manualDateTime = buildParsedDateTimeFromManualInput(optionalString(payload.manualDate), optionalString(payload.manualTime));
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    throw httpError(400, 'Apply requires latitude and longitude');
  }

  const rows = getGpsFixRows(context);
  const queue = getGpsFixQueue(context, { rows });
  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active GPS utility candidate');
  }

  const assetRecord = getGpsUtilityAssetRecord(context, assetId);
  if (!shouldOfferGpsFix(assetRecord)) {
    throw httpError(409, 'Asset no longer requires a GPS correction');
  }

  await immichRequest(context.accessToken, 'PUT', '/assets', {
    ids: [assetId],
    latitude,
    longitude,
    ...(manualDateTime ? { dateTimeOriginal: manualDateTime.iso } : {}),
  });

  const selectedSource = resolveGpsSelectionSource(queue.candidate, latitude, longitude, optionalString(payload.source));
  const state = loadUtilityState();
  const userState = ensureGpsUtilityUserState(state, context.nextcloudUserId);
  userState.applied[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath,
    latitude,
    longitude,
    source: selectedSource,
    dateTimeOriginal: manualDateTime?.iso || null,
  };
  delete userState.skipped[assetId];
  delete userState.failed[assetId];
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-gps-fix-apply',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath,
    latitude,
    longitude,
    source: selectedSource,
    dateTimeOriginal: manualDateTime?.iso || null,
  });

  enqueueGpsExifWriteback(context, {
    assetId,
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath,
    latitude,
    longitude,
    dateTimeOriginal: manualDateTime?.iso || null,
    exifDateTime: manualDateTime?.exif || null,
    extension: getFileExtension(assetRecord.fileName),
  });

  markGpsUtilityQueueAssetProcessed(context, assetId);
  return buildGpsFixResponseFromContext(context, { rows, excludeAssetIds: [assetId] });
}

async function handleGpsFixDelete(request, payload) {
  const context = await createUtilityContextFromRequest(request, { requireAuthenticatedContext: true });
  const assetId = requiredString(payload.assetId, 'assetId');
  const rows = getGpsFixRows(context);
  const queue = getGpsFixQueue(context, { rows });

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active GPS utility candidate');
  }

  const assetRecord = getGpsUtilityAssetRecord(context, assetId);
  await immichRequest(context.accessToken, 'DELETE', '/assets', {
    ids: [assetId],
    force: false,
  });

  const state = loadUtilityState();
  const userState = ensureGpsUtilityUserState(state, context.nextcloudUserId);
  userState.skipped[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    deleted: true,
    originalPath: assetRecord.originalPath,
  };
  delete userState.failed[assetId];
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-gps-fix-delete',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath,
    deleteMode: 'trash',
  });

  markGpsUtilityQueueAssetProcessed(context, assetId);
  return buildGpsFixResponseFromContext(context, { rows, excludeAssetIds: [assetId] });
}

function buildGpsFixResponseFromContext(context, options = {}) {
  const includeCandidate = options.includeCandidate !== false;
  const queue = getGpsFixQueue(context, options);

  return {
    status: {
      remaining: queue.remaining,
      total: queue.total,
      applied: queue.applied,
      skipped: queue.skipped,
      failed: queue.failed,
    },
    candidate: includeCandidate ? queue.candidate : null,
    job: sanitizeGpsUtilityBackgroundJob(getGpsUtilityBackgroundJob(context.nextcloudUserId)),
  };
}

function getGpsFixQueue(context, options = {}) {
  const state = loadUtilityState();
  const userState = ensureGpsUtilityUserState(state, context.nextcloudUserId);

  if (!options.rows && !options.excludeAssetIds?.length) {
    const materializedQueue = getMaterializedGpsUtilityCandidateQueue(context, userState);
    return {
      candidate: materializedQueue.candidates[0] ? buildGpsFixCandidate(context, materializedQueue.candidates[0]) : null,
      remaining: materializedQueue.candidates.length,
      total: materializedQueue.total,
      applied: Object.keys(userState.applied || {}).length,
      skipped: Object.keys(userState.skipped || {}).length,
      failed: Object.keys(userState.failed || {}).length,
    };
  }

  const rows = options.rows || getGpsFixRows(context);
  const excludedAssetIds = new Set(options.excludeAssetIds || []);
  const candidates = buildGpsUtilityCandidateRows(rows, userState, excludedAssetIds);

  return {
    candidate: candidates[0] ? buildGpsFixCandidate(context, candidates[0]) : null,
    remaining: candidates.length,
    total: rows.length,
    applied: Object.keys(userState.applied || {}).length,
    skipped: Object.keys(userState.skipped || {}).length,
    failed: Object.keys(userState.failed || {}).length,
  };
}

function getMaterializedGpsUtilityCandidateQueue(context, userState) {
  const cacheKey = buildGpsUtilityRowsCacheKey(context);
  const cached = gpsUtilityCandidateQueueCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached;
  }

  const rows = getGpsFixRows(context);
  const materialized = {
    total: rows.length,
    candidates: buildGpsUtilityCandidateRows(rows, userState),
    expiresAt: Date.now() + utilityCandidateQueueCacheTtlMs,
  };
  gpsUtilityCandidateQueueCache.set(cacheKey, materialized);
  return materialized;
}

function buildGpsUtilityCandidateRows(rows, userState, excludedAssetIds = new Set()) {
  const candidates = [];
  for (const row of rows) {
    if (excludedAssetIds.has(row.assetId)) {
      continue;
    }
    if (
      (userState.applied && userState.applied[row.assetId]) ||
      (userState.skipped && userState.skipped[row.assetId]) ||
      shouldExcludeFailedUtilityCandidate(userState, row.assetId)
    ) {
      continue;
    }
    candidates.push(row);
  }
  return candidates;
}

function markGpsUtilityQueueAssetProcessed(context, assetId) {
  const cacheKey = buildGpsUtilityRowsCacheKey(context);
  const cached = gpsUtilityCandidateQueueCache.get(cacheKey);
  if (!cached) {
    return;
  }

  cached.candidates = cached.candidates.filter((candidate) => candidate.assetId !== assetId);
  cached.expiresAt = Date.now() + utilityCandidateQueueCacheTtlMs;
}

function getGpsFixRows(context) {
  const cacheKey = buildGpsUtilityRowsCacheKey(context);
  const cached = gpsUtilityRowsCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.rows;
  }

  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      a."originalPath",
      a."fileCreatedAt",
      a."localDateTime",
      ae."dateTimeOriginal",
      ae.latitude,
      ae.longitude,
      coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") as "sortDateTime"
    from asset a
    left join asset_exif ae on ae."assetId" = a.id
    where a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and a.type = 'IMAGE'
      and (${libraryPathCondition})
      and lower(a."originalFileName") ~ '\\.(jpg|jpeg|png|webp|gif|heic|heif)$'
      and (ae.latitude is null or ae.longitude is null)
    order by coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") asc nulls last, a.id asc;
  `;

  const rows = runPostgresRowsQuery(sql).map((line) => {
    const [assetId, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal, latitude, longitude, sortDateTime] = line.split('\t');
    return {
      assetId,
      fileName,
      originalPath,
      fileCreatedAt,
      localDateTime,
      dateTimeOriginal,
      latitude: parseOptionalFloat(latitude),
      longitude: parseOptionalFloat(longitude),
      sortDateTime,
    };
  });
  gpsUtilityRowsCache.set(cacheKey, {
    rows,
    expiresAt: Date.now() + utilityRowsCacheTtlMs,
  });
  return rows;
}

function buildGpsUtilityRowsCacheKey(context) {
  return `gps:${context.nextcloudUserId}:${context.immichUserId}`;
}

function invalidateGpsFixCaches(context) {
  const cacheKey = buildGpsUtilityRowsCacheKey(context);
  gpsUtilityRowsCache.delete(cacheKey);
  gpsUtilityCandidateQueueCache.delete(cacheKey);
}

function getGpsUtilityAssetRecord(context, assetId) {
  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      a."originalPath",
      a."fileCreatedAt",
      a."localDateTime",
      ae."dateTimeOriginal",
      ae.latitude,
      ae.longitude
    from asset a
    left join asset_exif ae on ae."assetId" = a.id
    where a.id = ${sqlString(assetId)}
      and a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and (${libraryPathCondition})
    limit 1;
  `;
  const row = runPostgresRowsQuery(sql)[0];
  if (!row) {
    throw httpError(404, `Asset ${assetId} not found`);
  }

  const [id, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal, latitude, longitude] = row.split('\t');
  return {
    assetId: id,
    fileName,
    originalPath,
    fileCreatedAt,
    localDateTime,
    dateTimeOriginal,
    latitude: parseOptionalFloat(latitude),
    longitude: parseOptionalFloat(longitude),
  };
}

function shouldOfferGpsFix(assetRecord) {
  return !Number.isFinite(assetRecord.latitude) || !Number.isFinite(assetRecord.longitude);
}

function buildGpsFixCandidate(context, row) {
  const siblingSuggestion = getGpsSiblingSuggestion(context, row);
  const previousSuggestion = siblingSuggestion || getGpsNeighborSuggestion(context, row, 'previous');
  const nextSuggestion = getGpsNeighborSuggestion(context, row, 'next');
  const defaultSuggestion = selectDefaultGpsSuggestion(previousSuggestion, nextSuggestion);
  const pickerDateTime = row.dateTimeOriginal || row.sortDateTime || null;
  return {
    assetId: row.assetId,
    fileName: row.fileName,
    previewUrl: `/api/assets/${row.assetId}/thumbnail?size=preview`,
    currentDateTimeIso: row.dateTimeOriginal || row.sortDateTime || null,
    currentDateTimeLocal: formatUtilityDateTime(row.dateTimeOriginal || row.sortDateTime),
    currentDatePickerDate: formatUtilityPickerDate(pickerDateTime),
    currentDatePickerTime: formatUtilityPickerTime(pickerDateTime),
    currentLatitude: Number.isFinite(row.latitude) ? row.latitude : null,
    currentLongitude: Number.isFinite(row.longitude) ? row.longitude : null,
    previousSuggestion,
    nextSuggestion,
    defaultSuggestion,
  };
}

function getGpsSiblingSuggestion(context, row) {
  const rowSortDateTime = row.dateTimeOriginal || row.sortDateTime || row.localDateTime || row.fileCreatedAt || null;
  const referenceGroupKey = buildReferenceGroupKey(row.fileName);
  if (!rowSortDateTime || !referenceGroupKey) {
    return null;
  }

  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") as "sortDateTime",
      ae.latitude,
      ae.longitude
    from asset a
    join asset_exif ae on ae."assetId" = a.id
    where a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and a.type = 'IMAGE'
      and (${libraryPathCondition})
      and a.id <> ${sqlString(row.assetId)}
      and ae.latitude is not null
      and ae.longitude is not null
      and coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") is not null
      and ${buildReferenceGroupKeySql('a."originalFileName"')} = ${sqlString(referenceGroupKey)}
    order by abs(extract(epoch from (coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") - ${sqlString(rowSortDateTime)}::timestamptz))) asc,
      length(a."originalFileName") asc,
      a.id asc
    limit 1;
  `;
  const result = runPostgresRowsQuery(sql)[0];
  if (!result) {
    return null;
  }

  const [assetId, fileName, sortDateTime, latitude, longitude] = result.split('\t');
  const timeDeltaMs = Math.abs(Date.parse(rowSortDateTime) - Date.parse(sortDateTime));
  return {
    source: 'previous',
    assetId,
    fileName,
    previewUrl: `/api/assets/${assetId}/thumbnail?size=preview`,
    dateTimeOriginal: sortDateTime,
    dateTimeLocal: formatUtilityDateTime(sortDateTime),
    latitude: Number(latitude),
    longitude: Number(longitude),
    timeDeltaMs,
    timeDeltaLabel: formatRelativeDuration(timeDeltaMs),
  };
}

function getGpsNeighborSuggestion(context, row, direction) {
  const rowSortDateTime = row.dateTimeOriginal || row.sortDateTime || row.localDateTime || row.fileCreatedAt || null;
  if (!rowSortDateTime) {
    return null;
  }

  const comparisonOperator = direction === 'previous' ? '<' : '>';
  const sortDirection = direction === 'previous' ? 'desc' : 'asc';
  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") as "sortDateTime",
      ae.latitude,
      ae.longitude
    from asset a
    join asset_exif ae on ae."assetId" = a.id
    where a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and a.type = 'IMAGE'
      and (${libraryPathCondition})
      and ae.latitude is not null
      and ae.longitude is not null
      and coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") is not null
      and coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") ${comparisonOperator} ${sqlString(rowSortDateTime)}
    order by coalesce(ae."dateTimeOriginal", a."localDateTime", a."fileCreatedAt") ${sortDirection}, a.id ${sortDirection}
    limit 1;
  `;
  const result = runPostgresRowsQuery(sql)[0];
  if (!result) {
    return null;
  }

  const [assetId, fileName, sortDateTime, latitude, longitude] = result.split('\t');
  const timeDeltaMs = Math.abs(Date.parse(rowSortDateTime) - Date.parse(sortDateTime));
  return {
    source: direction,
    assetId,
    fileName,
    previewUrl: `/api/assets/${assetId}/thumbnail?size=preview`,
    dateTimeOriginal: sortDateTime,
    dateTimeLocal: formatUtilityDateTime(sortDateTime),
    latitude: Number(latitude),
    longitude: Number(longitude),
    timeDeltaMs,
    timeDeltaLabel: formatRelativeDuration(timeDeltaMs),
  };
}

function buildReferenceGroupKey(fileName) {
  const baseName = String(fileName || '')
    .replace(/\.[^.]+$/, '')
    .toLowerCase()
    .trim();
  if (!baseName) {
    return null;
  }
  return baseName.replace(/(?:[ _-](?:editado|edited|edit|copy|copia|copie|final))+$/i, '');
}

function buildReferenceGroupKeySql(columnSql) {
  return `lower(regexp_replace(regexp_replace(${columnSql}, '\\.[^.]+$', '', 'g'), '(?:[ _-](?:editado|edited|edit|copy|copia|copie|final))+$', '', 'i'))`;
}

function selectDefaultGpsSuggestion(previousSuggestion, nextSuggestion) {
  if (previousSuggestion && nextSuggestion) {
    return previousSuggestion.timeDeltaMs <= nextSuggestion.timeDeltaMs ? previousSuggestion : nextSuggestion;
  }
  return previousSuggestion || nextSuggestion || null;
}

function resolveGpsSelectionSource(candidate, latitude, longitude, requestedSource) {
  if (requestedSource === 'previous' || requestedSource === 'next' || requestedSource === 'manual') {
    return requestedSource;
  }

  const previous = candidate.previousSuggestion;
  if (previous && approximatelyEqualCoordinate(previous.latitude, latitude) && approximatelyEqualCoordinate(previous.longitude, longitude)) {
    return 'previous';
  }

  const next = candidate.nextSuggestion;
  if (next && approximatelyEqualCoordinate(next.latitude, latitude) && approximatelyEqualCoordinate(next.longitude, longitude)) {
    return 'next';
  }

  return 'manual';
}

function approximatelyEqualCoordinate(left, right) {
  return Math.abs(Number(left) - Number(right)) < 0.000001;
}

function enqueueGpsExifWriteback(context, task) {
  const queue = gpsUtilityWritebackQueues.get(context.nextcloudUserId) || [];
  queue.push({
    ...task,
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
  });
  gpsUtilityWritebackQueues.set(context.nextcloudUserId, queue);

  let job = gpsUtilityBackgroundJobs.get(context.nextcloudUserId);
  if (!job || job.status === 'completed' || job.status === 'failed') {
    job = {
      id: crypto.randomUUID(),
      nextcloudUserId: context.nextcloudUserId,
      immichUserId: context.immichUserId,
      status: 'running',
      startedAt: new Date().toISOString(),
      finishedAt: null,
      pending: queue.length,
      processed: 0,
      applied: 0,
      failed: 0,
      lastAssetId: null,
      lastFileName: null,
      lastError: null,
    };
    gpsUtilityBackgroundJobs.set(context.nextcloudUserId, job);
    void runGpsUtilityBackgroundJob(context.nextcloudUserId, job.id);
  } else {
    job.status = 'running';
    job.finishedAt = null;
    job.pending = queue.length;
  }
}

function getGpsUtilityBackgroundJob(nextcloudUserId) {
  return gpsUtilityBackgroundJobs.get(nextcloudUserId) || null;
}

function sanitizeGpsUtilityBackgroundJob(job) {
  if (!job) {
    return null;
  }
  return {
    id: job.id,
    status: job.status,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    pending: job.pending,
    processed: job.processed,
    applied: job.applied,
    failed: job.failed,
    lastAssetId: job.lastAssetId,
    lastFileName: job.lastFileName,
    lastError: job.lastError,
  };
}

async function runGpsUtilityBackgroundJob(nextcloudUserId, jobId) {
  const job = gpsUtilityBackgroundJobs.get(nextcloudUserId);
  if (!job || job.id !== jobId) {
    return;
  }

  try {
    while (true) {
      const queue = gpsUtilityWritebackQueues.get(nextcloudUserId) || [];
      const task = queue.shift();
      gpsUtilityWritebackQueues.set(nextcloudUserId, queue);
      job.pending = queue.length;

      if (!task) {
        job.status = 'completed';
        job.finishedAt = new Date().toISOString();
        job.lastError = null;
        return;
      }

      job.lastAssetId = task.assetId;
      job.lastFileName = task.fileName;

      try {
        applyExifGps(task.originalPath, task.latitude, task.longitude, task.extension);
        if (task.exifDateTime) {
          applyExifDateTime(task.originalPath, task.exifDateTime, task.extension);
        }
        const state = loadUtilityState();
        const userState = ensureGpsUtilityUserState(state, nextcloudUserId);
        delete userState.failed[task.assetId];
        saveUtilityState(state);

        writeAuditEntry({
          kind: 'utility-gps-fix-writeback-applied',
          nextcloudUserId,
          immichUserId: task.immichUserId,
          assetId: task.assetId,
          fileName: task.fileName,
          originalPath: task.originalPath,
          latitude: task.latitude,
          longitude: task.longitude,
          dateTimeOriginal: task.dateTimeOriginal,
        });
        job.applied += 1;
        job.lastError = null;
      } catch (error) {
        const state = loadUtilityState();
        const userState = ensureGpsUtilityUserState(state, nextcloudUserId);
        userState.failed[task.assetId] = {
          decidedAt: new Date().toISOString(),
          fileName: task.fileName,
          message: error.message,
          latitude: task.latitude,
          longitude: task.longitude,
          dateTimeOriginal: task.dateTimeOriginal,
          background: true,
          permanent: true,
        };
        saveUtilityState(state);

        writeAuditEntry({
          kind: 'utility-gps-fix-writeback-error',
          nextcloudUserId,
          immichUserId: task.immichUserId,
          assetId: task.assetId,
          fileName: task.fileName,
          originalPath: task.originalPath,
          message: error.message,
        });
        job.failed += 1;
        job.lastError = error.message;
      }

      job.processed += 1;
    }
  } catch (error) {
    job.status = 'failed';
    job.finishedAt = new Date().toISOString();
    job.lastError = error.message;
  }
}

function formatUtilityDateTime(value) {
  if (!value) {
    return 'Nema datuma';
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return String(value);
  }
  return new Intl.DateTimeFormat('hr-HR', {
    dateStyle: 'medium',
    timeStyle: 'medium',
    timeZone: config.utilityTimezone,
  }).format(new Date(timestamp));
}

function formatUtilityPickerDate(value) {
  if (!value) {
    return '';
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return '';
  }
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-CA', {
      timeZone: config.utilityTimezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    })
      .formatToParts(new Date(timestamp))
      .filter((item) => item.type !== 'literal')
      .map((item) => [item.type, item.value]),
  );
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function formatUtilityPickerTime(value) {
  if (!value) {
    return '';
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return '';
  }
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-GB', {
      timeZone: config.utilityTimezone,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    })
      .formatToParts(new Date(timestamp))
      .filter((item) => item.type !== 'literal')
      .map((item) => [item.type, item.value]),
  );
  return `${parts.hour}:${parts.minute}:${parts.second}`;
}

function formatRelativeDuration(milliseconds) {
  if (!Number.isFinite(milliseconds)) {
    return '-';
  }
  const totalMinutes = Math.round(milliseconds / 60000);
  if (totalMinutes < 60) {
    return `${totalMinutes} min`;
  }
  const totalHours = Math.round(totalMinutes / 60);
  if (totalHours < 48) {
    return `${totalHours} h`;
  }
  const totalDays = Math.round(totalHours / 24);
  return `${totalDays} d`;
}

function parseOptionalFloat(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function buildDateFromFilenameResponse(request, options = {}) {
  const includeCandidate = options.includeCandidate !== false;
  const context = await createUtilityContextFromRequest(request);
  const queue = getDateFromFilenameQueue(context, options);

  return {
    status: {
      remaining: queue.remaining,
      total: queue.total,
      applied: queue.applied,
      skipped: queue.skipped,
      deleted: queue.deleted,
    },
    candidate: includeCandidate ? queue.candidate : null,
  };
}

async function handleDateFromFilenameSkip(request, payload) {
  const context = await createUtilityContextFromRequest(request);
  const assetId = requiredString(payload.assetId, 'assetId');
  const rows = getDateFromFilenameRows(context);
  const queue = getDateFromFilenameQueue(context, { rows });

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active utility candidate');
  }

  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  userState.skipped[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
  };
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-date-from-filename-skip',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
  });

  return buildDateFromFilenameResponseFromContext(context, { rows, excludeAssetIds: [assetId] });
}

async function handleDateFromFilenameApply(request, payload) {
  const context = await createUtilityContextFromRequest(request, { requireAuthenticatedContext: true });
  const assetId = requiredString(payload.assetId, 'assetId');
  const rows = getDateFromFilenameRows(context);
  const queue = getDateFromFilenameQueue(context, { rows });

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active utility candidate');
  }

  const validation = await validateAssetsOwned(context, [assetId]);
  const assetRecord = getUtilityAssetRecord(context, assetId);
  if (!shouldOfferDateCorrection(assetRecord, queue.candidate)) {
    throw httpError(409, 'Asset no longer requires a date correction');
  }

  applyExifDateTime(assetRecord.originalPath, queue.candidate.proposedDateTimeExif, getFileExtension(assetRecord.fileName));

  await immichRequest(context.accessToken, 'PUT', '/assets', {
    ids: [assetId],
    dateTimeOriginal: queue.candidate.proposedDateTimeIso,
  });

  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  userState.applied[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
    originalPath: validation.assets[0]?.originalPath || assetRecord.originalPath,
  };
  delete userState.skipped[assetId];
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-date-from-filename-apply',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    originalPath: assetRecord.originalPath,
    proposedDateTimeIso: queue.candidate.proposedDateTimeIso,
    parserReason: queue.candidate.parserReason,
  });

  return buildDateFromFilenameResponseFromContext(context, { rows, excludeAssetIds: [assetId] });
}

async function handleDateFromFilenameDelete(request, payload) {
  const context = await createUtilityContextFromRequest(request, { requireAuthenticatedContext: true });
  const assetId = requiredString(payload.assetId, 'assetId');
  const rows = getDateFromFilenameRows(context);
  const queue = getDateFromFilenameQueue(context, { rows });

  if (!queue.candidate || queue.candidate.assetId !== assetId) {
    throw httpError(409, 'Asset is no longer the active utility candidate');
  }

  const validation = await validateAssetsOwned(context, [assetId]);
  await immichRequest(context.accessToken, 'DELETE', '/assets', {
    ids: [assetId],
    force: false,
  });

  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  userState.deleted[assetId] = {
    decidedAt: new Date().toISOString(),
    fileName: queue.candidate.fileName,
    originalPath: validation.assets[0]?.originalPath || null,
    deleteMode: 'trash',
  };
  delete userState.skipped[assetId];
  saveUtilityState(state);

  writeAuditEntry({
    kind: 'utility-date-from-filename-delete',
    nextcloudUserId: context.nextcloudUserId,
    immichUserId: context.immichUserId,
    assetId,
    fileName: queue.candidate.fileName,
    originalPath: validation.assets[0]?.originalPath || null,
    deleteMode: 'trash',
  });

  return buildDateFromFilenameResponseFromContext(context, { rows, excludeAssetIds: [assetId] });
}

function buildDateFromFilenameResponseFromContext(context, options = {}) {
  const includeCandidate = options.includeCandidate !== false;
  const queue = getDateFromFilenameQueue(context, options);

  return {
    status: {
      remaining: queue.remaining,
      total: queue.total,
      applied: queue.applied,
      skipped: queue.skipped,
      deleted: queue.deleted,
    },
    candidate: includeCandidate ? queue.candidate : null,
  };
}

async function createUtilityContextFromRequest(request, options = {}) {
  const sessionUser = await getImmichSessionUser(request);
  const baseContext = await createUserContextByImmichUserId(sessionUser.id);
  if (!options.requireAuthenticatedContext) {
    return baseContext;
  }

  return {
    ...baseContext,
    accessToken: {
      sessionCookie: request.headers.cookie,
    },
  };
}

async function getImmichSessionUser(request) {
  const proxiedUserIdHeader = request.headers['x-immich-user-id'];
  const proxySecretHeader = request.headers['x-media-ops-proxy-secret'];
  const proxiedUserId = Array.isArray(proxiedUserIdHeader) ? proxiedUserIdHeader[0] : proxiedUserIdHeader;
  const proxySecret = Array.isArray(proxySecretHeader) ? proxySecretHeader[0] : proxySecretHeader;
  if (config.internalEventSecret && proxySecret === config.internalEventSecret && proxiedUserId) {
    return { id: proxiedUserId };
  }

  const cookie = request.headers.cookie;
  if (!cookie) {
    throw httpError(401, 'Authentication required');
  }

  const response = await fetch(`${config.immichApiUrl}/users/me`, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Cookie: cookie,
    },
  });

  const text = await response.text();
  const data = text ? safeJsonParse(text) : null;
  if (response.status === 401) {
    throw httpError(401, 'Authentication required');
  }
  if (!response.ok || !data?.id) {
    throw httpError(403, `Unable to resolve authenticated Immich user: ${text}`);
  }
  return data;
}

function getDateFromFilenameQueue(context, options = {}) {
  const state = loadUtilityState();
  const userState = ensureUtilityUserState(state, context.nextcloudUserId);
  const rows = options.rows || getDateFromFilenameRows(context);
  const excludedAssetIds = new Set(options.excludeAssetIds || []);
  const candidates = [];

  for (const row of rows) {
    const candidate = mapUtilityRowToCandidate(row);
    if (!candidate) {
      continue;
    }
    if (excludedAssetIds.has(candidate.assetId)) {
      continue;
    }
    if (
      (userState.applied && userState.applied[candidate.assetId]) ||
      (userState.skipped && userState.skipped[candidate.assetId]) ||
      shouldExcludeFailedUtilityCandidate(userState, candidate.assetId)
    ) {
      continue;
    }
    if (userState.deleted && userState.deleted[candidate.assetId]) {
      continue;
    }
    candidates.push(candidate);
  }

  return {
    candidate: candidates[0] || null,
    remaining: candidates.length,
    total: rows.length,
    applied: Object.keys(userState.applied || {}).length,
    skipped: Object.keys(userState.skipped || {}).length,
    deleted: Object.keys(userState.deleted || {}).length,
  };
}

function getDateFromFilenameRows(context) {
  const cacheKey = buildUtilityRowsCacheKey(context);
  const cached = utilityRowsCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.rows;
  }

  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      a."originalPath",
      a."fileCreatedAt",
      a."localDateTime",
      ae."dateTimeOriginal"
    from asset a
    left join asset_exif ae on ae."assetId" = a.id
    where a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and a.type = 'IMAGE'
      and (${libraryPathCondition})
      and lower(a."originalFileName") ~ '\\.(jpg|jpeg|png|webp|gif|heic|heif)$'
      and lower(a."originalFileName") ~ '(screenshot_|pxl_|signal-|fb_img_\\d{13}|face_sc_\\d{13}|picplus_\\d{13}|img[-_]\\d{8}-wa|vid[-_]\\d{8}-wa|\\d{8}[_ -]\\d{6}|\\d{4}-\\d{2}-\\d{2}[ _. -]\\d{2}[.:_-]\\d{2}[.:_-]\\d{2})'
    order by a."fileCreatedAt" asc, a.id asc;
  `;

  const rows = runPostgresRowsQuery(sql).map((line) => {
    const [assetId, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal] = line.split('\t');
    return { assetId, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal };
  });
  utilityRowsCache.set(cacheKey, {
    rows,
    expiresAt: Date.now() + utilityRowsCacheTtlMs,
  });
  return rows;
}

function getUtilityAssetRecord(context, assetId) {
  const libraryPathCondition = buildLibraryPathSqlCondition(context);
  const sql = `
    select
      a.id,
      a."originalFileName",
      a."originalPath",
      a."fileCreatedAt",
      a."localDateTime",
      ae."dateTimeOriginal"
    from asset a
    left join asset_exif ae on ae."assetId" = a.id
    where a.id = ${sqlString(assetId)}
      and a."ownerId" = ${sqlString(context.immichUserId)}
      and a."deletedAt" is null
      and (${libraryPathCondition})
    limit 1;
  `;
  const row = runPostgresQuery(sql);
  if (!row) {
    throw httpError(404, `Asset ${assetId} not found`);
  }
  const [id, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal] = row.split('|');
  return { assetId: id, fileName, originalPath, fileCreatedAt, localDateTime, dateTimeOriginal };
}

function mapUtilityRowToCandidate(row) {
  const extension = getFileExtension(row.fileName);
  if (!SUPPORTED_UTILITY_EXTENSIONS.has(extension)) {
    return null;
  }

  const parsed = parseDateFromFilename(row.fileName, row.fileCreatedAt, row.localDateTime);
  if (!parsed) {
    return null;
  }

  const currentDateInfo = normalizeStoredDateTime(row.dateTimeOriginal);
  const candidateKind = determineCandidateKind(parsed, currentDateInfo);
  if (!candidateKind) {
    return null;
  }

  const parserReason = candidateKind === 'missing'
    ? parsed.reason
    : `${parsed.reason} · postojeci datum izgleda sumnjivo`;

  return {
    assetId: row.assetId,
    fileName: row.fileName,
    previewUrl: `/api/assets/${row.assetId}/thumbnail?size=preview`,
    candidateKind,
    currentDateTimeIso: currentDateInfo?.iso || null,
    currentDateTimeLocal: currentDateInfo?.local || 'Nema datuma',
    proposedDateTimeIso: parsed.iso,
    proposedDateTimeExif: parsed.exif,
    proposedDateTimeLocal: parsed.local,
    parserReason,
    confidence: parsed.confidence,
  };
}

function parseDateFromFilename(fileName, fileCreatedAt, localDateTime) {
  const baseName = String(fileName || '').replace(/\.[^.]+$/, '');
  const fallbackTime = extractFallbackTime(fileCreatedAt || localDateTime);
  const patterns = [
    {
      regex: /(?:^|[^0-9])(\d{4})(\d{2})(\d{2})[_ -](\d{2})(\d{2})(\d{2})(?:[^0-9]|$)/,
      reason: 'Prepoznat obrazac YYYYMMDD_HHMMSS',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])(\d{4})-(\d{2})-(\d{2})[ _. -](\d{2})[.:_-](\d{2})[.:_-](\d{2})(?:[^0-9]|$)/,
      reason: 'Prepoznat obrazac YYYY-MM-DD HH.MM.SS',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])PXL_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(?:[^0-9]|$)/i,
      reason: 'Prepoznat PXL obrazac',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])Screenshot_(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})(?:[^0-9]|$)/i,
      reason: 'Prepoznat Screenshot obrazac',
      confidence: 'high',
    },
    {
      regex: /(?:^|[^0-9])signal-(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})(\d{2})(?:[^0-9]|$)/i,
      reason: 'Prepoznat Signal obrazac',
      confidence: 'high',
    },
  ];

  for (const pattern of patterns) {
    const match = baseName.match(pattern.regex);
    if (!match) {
      continue;
    }
    return buildParsedDateTime(match.slice(1, 7), pattern.reason, pattern.confidence);
  }

  const whatsappMatch = baseName.match(/(?:IMG|VID)[-_](\d{4})(\d{2})(\d{2})[-_]WA\d+/i);
  if (whatsappMatch && fallbackTime) {
    return buildParsedDateTime(
      [whatsappMatch[1], whatsappMatch[2], whatsappMatch[3], fallbackTime.hour, fallbackTime.minute, fallbackTime.second],
      'Prepoznat WhatsApp datum, vrijeme preuzeto iz fileCreatedAt',
      'medium',
    );
  }

  const epochMatch = baseName.match(/(?:^|[^0-9])(?:FB_IMG|FACE_SC|PicPlus)_(\d{13})(?:[^0-9]|$)/i);
  if (epochMatch) {
    return buildParsedDateTimeFromEpochMilliseconds(
      epochMatch[1],
      'Prepoznat epoch timestamp u nazivu datoteke',
      'high',
    );
  }

  return null;
}

function buildParsedDateTime(parts, reason, confidence) {
  const [year, month, day, hour, minute, second] = parts.map((value) => String(value).padStart(2, '0'));
  const local = `${year}-${month}-${day} ${hour}:${minute}:${second}`;
  const iso = `${year}-${month}-${day}T${hour}:${minute}:${second}${resolveTimezoneOffset(year, month, day)}`;
  const exif = `${year}:${month}:${day} ${hour}:${minute}:${second}`;
  return { iso, exif, local, date: `${year}-${month}-${day}`, reason, confidence };
}

function extractFallbackTime(value) {
  if (!value) {
    return null;
  }
  const match = String(value).match(/T?(\d{2}):(\d{2}):(\d{2})/);
  if (!match) {
    return null;
  }
  return { hour: match[1], minute: match[2], second: match[3] };
}

function normalizeStoredDateTime(value) {
  if (!value) {
    return null;
  }
  const normalized = String(value).replace('T', ' ');
  const iso = normalized.includes('T') ? normalized : normalized.replace(' ', 'T');
  const dateMatch = normalized.match(/^(\d{4}-\d{2}-\d{2})/);
  return {
    iso,
    local: normalized,
    date: dateMatch ? dateMatch[1] : null,
  };
}

function determineCandidateKind(parsed, currentDateInfo) {
  if (!currentDateInfo?.date) {
    return 'missing';
  }
  return currentDateInfo.date !== parsed.date ? 'mismatch' : null;
}

function shouldOfferDateCorrection(assetRecord, candidate) {
  const currentDateInfo = normalizeStoredDateTime(assetRecord.dateTimeOriginal);
  return determineCandidateKind({
    date: candidate.proposedDateTimeIso.slice(0, 10),
  }, currentDateInfo) !== null;
}

function buildLibraryPathSqlCondition(context) {
  const libraryPaths = getManagedLibraryPaths(context);
  return libraryPaths.map((libraryPath) => `a."originalPath" like ${sqlString(`${libraryPath}%`)}`).join(' or ');
}

function getManagedLibraryPaths(context) {
  const paths = new Set();
  if (context.libraryPath) {
    paths.add(context.libraryPath);
  }
  const libraries = context.stateEntry?.libraries && typeof context.stateEntry.libraries === 'object'
    ? Object.values(context.stateEntry.libraries)
    : [];
  for (const library of libraries) {
    if (library?.libraryPath) {
      paths.add(library.libraryPath);
    }
  }
  if (paths.size === 0) {
    throw httpError(400, `Managed user is missing library paths: ${context.nextcloudUserId}`);
  }
  return Array.from(paths);
}

function resolveTimezoneOffset(year, month, day) {
  const sample = new Date(`${year}-${month}-${day}T12:00:00Z`);
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: config.utilityTimezone,
    timeZoneName: 'longOffset',
  });
  const part = formatter.formatToParts(sample).find((item) => item.type === 'timeZoneName');
  const offset = part?.value?.replace('GMT', '') || '+00:00';
  return offset === '' ? '+00:00' : offset;
}

function applyExifDateTime(originalPath, exifDateTime, extension) {
  const command = [
    '-overwrite_original',
    '-P',
    `-DateTimeOriginal=${exifDateTime}`,
    `-CreateDate=${exifDateTime}`,
    `-ModifyDate=${exifDateTime}`,
  ];

  if (extension === 'heic' || extension === 'heif') {
    command.push(`-QuickTime:CreateDate=${exifDateTime}`, `-QuickTime:ModifyDate=${exifDateTime}`);
  }

  command.push(originalPath);

  try {
    execFileSync('exiftool', command, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } catch (error) {
    const output = `${error.stdout || ''}\n${error.stderr || ''}`.trim();
    throw httpError(500, `EXIF writeback failed: ${output || error.message}`);
  }
}

function loadUtilityState() {
  const state = loadJson(utilityStatePath, {
    dateFromFilename: { users: {} },
    gpsFix: { users: {} },
  });
  const users = state.dateFromFilename && typeof state.dateFromFilename === 'object' ? state.dateFromFilename.users : {};
  state.dateFromFilename = {
    users: users && typeof users === 'object' ? users : {},
  };
  const gpsUsers = state.gpsFix && typeof state.gpsFix === 'object' ? state.gpsFix.users : {};
  state.gpsFix = {
    users: gpsUsers && typeof gpsUsers === 'object' ? gpsUsers : {},
  };
  return state;
}

function saveUtilityState(state) {
  fs.writeFileSync(utilityStatePath, JSON.stringify(state, null, 2));
}

function ensureUtilityUserState(state, nextcloudUserId) {
  const users = state.dateFromFilename.users;
  users[nextcloudUserId] = users[nextcloudUserId] || { applied: {}, skipped: {}, deleted: {}, failed: {} };
  users[nextcloudUserId].applied = users[nextcloudUserId].applied && typeof users[nextcloudUserId].applied === 'object' ? users[nextcloudUserId].applied : {};
  users[nextcloudUserId].skipped = users[nextcloudUserId].skipped && typeof users[nextcloudUserId].skipped === 'object' ? users[nextcloudUserId].skipped : {};
  users[nextcloudUserId].deleted = users[nextcloudUserId].deleted && typeof users[nextcloudUserId].deleted === 'object' ? users[nextcloudUserId].deleted : {};
  users[nextcloudUserId].failed = users[nextcloudUserId].failed && typeof users[nextcloudUserId].failed === 'object' ? users[nextcloudUserId].failed : {};
  return users[nextcloudUserId];
}

function ensureGpsUtilityUserState(state, nextcloudUserId) {
  const users = state.gpsFix.users;
  users[nextcloudUserId] = users[nextcloudUserId] || { applied: {}, skipped: {}, failed: {} };
  users[nextcloudUserId].applied = users[nextcloudUserId].applied && typeof users[nextcloudUserId].applied === 'object' ? users[nextcloudUserId].applied : {};
  users[nextcloudUserId].skipped = users[nextcloudUserId].skipped && typeof users[nextcloudUserId].skipped === 'object' ? users[nextcloudUserId].skipped : {};
  users[nextcloudUserId].failed = users[nextcloudUserId].failed && typeof users[nextcloudUserId].failed === 'object' ? users[nextcloudUserId].failed : {};
  return users[nextcloudUserId];
}

async function handleInternalImmichEvent(payload) {
  const eventType = requiredString(payload.eventType, 'eventType');
  const userId = requiredString(payload.userId, 'userId');
  const assetIds = requiredStringArray(payload.assetIds, 'assetIds');
  const eventRecord = {
    id: optionalString(payload.correlationId) || crypto.randomUUID(),
    eventType,
    userId,
    assetIds,
    receivedAt: new Date().toISOString(),
    sourceTimestamp: optionalString(payload.timestamp) || null,
  };

  writeAuditEntry({
    kind: 'immich-event-received',
    ...eventRecord,
  });

  const result = await processImmichEvent(eventRecord);
  return {
    accepted: true,
    eventId: eventRecord.id,
    eventType,
    assetCount: assetIds.length,
    result,
  };
}

async function dispatchOperation(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Operation payload must be a JSON object');
  }

  const operation = String(payload.operation || '');
  if (!operation) {
    throw new Error('Operation payload requires "operation"');
  }

  const handlers = {
    'create-album': handleCreateAlbum,
    'update-album': handleUpdateAlbum,
    'delete-album': handleDeleteAlbum,
    'add-assets-to-album': handleAddAssetsToAlbum,
    'remove-assets-from-album': handleRemoveAssetsFromAlbum,
    'update-asset-metadata': handleUpdateAssetMetadata,
    'trash-assets': handleTrashAssets,
    'confirm-delete-assets': handleConfirmDeleteAssets,
    'move-assets-to-folder': handleMoveAssetsToFolder,
    'reconcile-smart-albums': handleReconcileSmartAlbums,
  };

  const handler = handlers[operation];
  if (!handler) {
    throw new Error(`Unsupported operation: ${operation}`);
  }

  const result = await handler(payload);
  persistOperation(result);
  return result;
}

async function handleCreateAlbum(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const dto = {
    albumName: requiredString(payload.albumName, 'albumName'),
    description: optionalString(payload.description),
    assetIds: optionalStringArray(payload.assetIds),
  };

  if (config.dryRun) {
    return finalizeResult('create-album', payload, context, {
      applied: false,
      planned: true,
      dryRunReason: 'MEDIA_OPS_DRY_RUN=true',
      plannedRequest: {
        method: 'POST',
        path: '/albums',
        body: dto,
      },
    });
  }

  const created = await immichRequest(context.accessToken, 'POST', '/albums', dto);
  const nextcloudWriteback = await syncNextcloudAlbumWriteback(context, dto.albumName, dto.assetIds);
  return finalizeResult('create-album', payload, context, {
    albumId: created.id,
    applied: true,
    immichApplied: true,
    immichResponse: created,
    nextcloudWritebackApplied: nextcloudWriteback.applied,
    nextcloudWritebackStatus: nextcloudWriteback.status,
    nextcloudWritebackErrors: nextcloudWriteback.errors,
    nextcloudWriteback,
  });
}

async function handleUpdateAlbum(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const albumId = requiredString(payload.albumId, 'albumId');
  const dto = compactObject({
    albumName: optionalString(payload.albumName),
    description: optionalString(payload.description),
    albumThumbnailAssetId: optionalString(payload.albumThumbnailAssetId),
    isActivityEnabled: optionalBoolean(payload.isActivityEnabled),
    order: optionalString(payload.order),
  });

  if (Object.keys(dto).length === 0) {
    throw new Error('update-album requires at least one mutable field');
  }

  if (config.dryRun) {
    return finalizeResult('update-album', payload, context, {
      albumId,
      applied: false,
      planned: true,
      dryRunReason: 'MEDIA_OPS_DRY_RUN=true',
      plannedRequest: {
        method: 'PATCH',
        path: `/albums/${albumId}`,
        body: dto,
      },
    });
  }

  const updated = await immichRequest(context.accessToken, 'PATCH', `/albums/${albumId}`, dto);
  return finalizeResult('update-album', payload, context, {
    albumId,
    applied: true,
    immichResponse: updated,
  });
}

async function handleDeleteAlbum(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const albumId = requiredString(payload.albumId, 'albumId');

  if (config.dryRun) {
    return finalizeResult('delete-album', payload, context, {
      albumId,
      applied: false,
      planned: true,
      dryRunReason: 'MEDIA_OPS_DRY_RUN=true',
      plannedRequest: {
        method: 'DELETE',
        path: `/albums/${albumId}`,
      },
    });
  }

  await immichRequest(context.accessToken, 'DELETE', `/albums/${albumId}`);
  return finalizeResult('delete-album', payload, context, {
    albumId,
    applied: true,
  });
}

async function handleAddAssetsToAlbum(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const albumId = requiredString(payload.albumId, 'albumId');
  const assetIds = requiredStringArray(payload.assetIds, 'assetIds');
  const validation = await validateAssetsOwned(context, assetIds);

  if (config.dryRun) {
    return finalizeResult('add-assets-to-album', payload, context, {
      albumId,
      assetIds,
      validation,
      applied: false,
      planned: true,
      dryRunReason: 'MEDIA_OPS_DRY_RUN=true',
      plannedRequest: {
        method: 'PUT',
        path: `/albums/${albumId}/assets`,
        body: { ids: assetIds },
      },
    });
  }

  const updated = await immichRequest(context.accessToken, 'PUT', `/albums/${albumId}/assets`, { ids: assetIds });
  const album = await immichRequest(context.accessToken, 'GET', `/albums/${albumId}`);
  const nextcloudWriteback = await syncNextcloudAlbumWriteback(
    context,
    resolveAlbumName(album, albumId),
    assetIds,
    validation,
  );
  return finalizeResult('add-assets-to-album', payload, context, {
    albumId,
    assetIds,
    validation,
    applied: true,
    immichApplied: true,
    immichResponse: updated,
    nextcloudWritebackApplied: nextcloudWriteback.applied,
    nextcloudWritebackStatus: nextcloudWriteback.status,
    nextcloudWritebackErrors: nextcloudWriteback.errors,
    nextcloudWriteback,
  });
}

async function handleRemoveAssetsFromAlbum(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const albumId = requiredString(payload.albumId, 'albumId');
  const assetIds = requiredStringArray(payload.assetIds, 'assetIds');
  const validation = await validateAssetsOwned(context, assetIds);

  if (config.dryRun) {
    return finalizeResult('remove-assets-from-album', payload, context, {
      albumId,
      assetIds,
      validation,
      applied: false,
      planned: true,
      dryRunReason: 'MEDIA_OPS_DRY_RUN=true',
      plannedRequest: {
        method: 'DELETE',
        path: `/albums/${albumId}/assets`,
        body: { ids: assetIds },
      },
    });
  }

  const updated = await immichRequest(context.accessToken, 'DELETE', `/albums/${albumId}/assets`, { ids: assetIds });
  return finalizeResult('remove-assets-from-album', payload, context, {
    albumId,
    assetIds,
    validation,
    applied: true,
    immichResponse: updated,
  });
}

async function handleUpdateAssetMetadata(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const assetIds = requiredStringArray(payload.assetIds, 'assetIds');
  const validation = await validateAssetsOwned(context, assetIds);
  const metadata = compactObject({
    dateTimeOriginal: optionalString(payload.dateTimeOriginal),
    latitude: optionalNumber(payload.latitude),
    longitude: optionalNumber(payload.longitude),
    description: optionalString(payload.description),
    rating: optionalNullableNumber(payload.rating),
    isFavorite: optionalBoolean(payload.isFavorite),
    visibility: optionalString(payload.visibility),
  });

  if (Object.keys(metadata).length === 0) {
    throw new Error('update-asset-metadata requires at least one metadata field');
  }

  let immichResponse = null;
  let applied = false;
  if (!config.dryRun) {
    immichResponse = await immichRequest(context.accessToken, 'PUT', '/assets', {
      ids: assetIds,
      ...metadata,
    });
    applied = true;
  }

  const writebackPlan = validation.assets.map((asset) => ({
    assetId: asset.id,
    originalPath: asset.originalPath,
    writeback: config.writebackEnabled && !config.dryRun ? 'pending-live-writeback' : 'planned-sidecar-or-exif-writeback',
    metadata,
  }));

  return finalizeResult('update-asset-metadata', payload, context, {
    assetIds,
    validation,
    metadata,
    applied,
    planned: config.dryRun,
    dryRunReason: config.dryRun ? 'MEDIA_OPS_DRY_RUN=true' : undefined,
    immichResponse,
    writebackPlan,
    writebackMode: config.writebackEnabled && !config.dryRun ? 'live-enabled' : 'audit-only',
  });
}

async function handleTrashAssets(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const assetIds = requiredStringArray(payload.assetIds, 'assetIds');
  const validation = await validateAssetsOwned(context, assetIds);
  const state = loadOperationsState();
  const batchId = crypto.randomUUID();
  const batch = {
    id: batchId,
    type: 'trash-assets',
    nextcloudUserId: context.nextcloudUserId,
    immichEmail: context.immichEmail,
    status: 'pending_delete_confirmation',
    createdAt: new Date().toISOString(),
    assetIds,
    assets: validation.assets.map((asset) => ({
      assetId: asset.id,
      originalPath: asset.originalPath,
      trashStrategy: 'staging-or-user-trash',
    })),
    reason: optionalString(payload.reason) || '',
  };
  state.trashBatches.push(batch);
  saveOperationsState(state);

  return finalizeResult('trash-assets', payload, context, {
    batchId,
    assetIds,
    validation,
    applied: false,
    staged: true,
    nextAction: 'confirm-delete-assets',
  });
}

async function handleConfirmDeleteAssets(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const batchId = requiredString(payload.batchId, 'batchId');
  const apply = parseBoolean(payload.apply);
  const state = loadOperationsState();
  const batch = state.trashBatches.find((candidate) => candidate.id === batchId && candidate.nextcloudUserId === context.nextcloudUserId);

  if (!batch) {
    throw new Error(`Trash batch not found for ${context.nextcloudUserId}: ${batchId}`);
  }

  batch.confirmedAt = new Date().toISOString();
  batch.status = apply ? 'confirmed_pending_delete' : 'confirmed_dry_run';

  let immichResponse = null;
  if (apply && config.deleteEnabled && !config.dryRun) {
    immichResponse = await immichRequest(context.accessToken, 'DELETE', '/assets', {
      ids: batch.assetIds,
      force: false,
    });
    batch.status = 'delete_requested_in_immich';
  }

  saveOperationsState(state);

  return finalizeResult('confirm-delete-assets', payload, context, {
    batchId,
    assetIds: batch.assetIds,
    applied: Boolean(apply && config.deleteEnabled && !config.dryRun),
    immichResponse,
    deleteMode: config.deleteEnabled && !config.dryRun ? 'live-delete-requested' : 'audit-only',
  });
}

async function handleMoveAssetsToFolder(payload) {
  const context = await createUserContext(payload.nextcloudUserId);
  const assetIds = requiredStringArray(payload.assetIds, 'assetIds');
  const destinationRelative = requiredString(payload.destinationRelativePath, 'destinationRelativePath');
  const validation = await validateAssetsOwned(context, assetIds);
  const destinationAbsolute = buildUserDestinationPath(context, destinationRelative);
  const plannedMoves = validation.assets.map((asset) => ({
    assetId: asset.id,
    sourcePath: asset.originalPath,
    destinationPath: path.posix.join(destinationAbsolute, path.posix.basename(asset.originalPath)),
  }));

  return finalizeResult('move-assets-to-folder', payload, context, {
    assetIds,
    validation,
    destinationRelativePath: destinationRelative,
    destinationAbsolutePath: destinationAbsolute,
    plannedMoves,
    applied: false,
    moveMode: config.folderMoveEnabled && !config.dryRun ? 'ready-for-live-worker' : 'dry-run-only',
  });
}

async function handleReconcileSmartAlbums(payload) {
  if (!config.smartAlbumsEnabled) {
    throw new Error('MEDIA_OPS_SMART_ALBUMS_ENABLED=false');
  }

  const context = await createUserContext(payload.nextcloudUserId);
  const assetIds = requiredStringArray(payload.assetIds, 'assetIds');
  const runtime = createSmartAlbumRuntime();
  const results = [];

  for (const assetId of assetIds) {
    results.push(await reconcileSmartAlbumsForAsset(context, runtime, assetId, null));
  }

  return finalizeResult('reconcile-smart-albums', payload, context, {
    assetIds,
    smartAlbumsDryRun: config.smartAlbumsDryRun,
    smartAlbumsEnabled: config.smartAlbumsEnabled,
    status: summarizePerAssetStatus(results),
    assets: results,
  });
}

async function createUserContext(nextcloudUserId) {
  const managedState = loadJson(managedStatePath, { users: {} });
  const credentials = loadJson(credentialsPath, {});
  const [managedEmail, stateEntry] = findManagedUserByNextcloudUserId(managedState, nextcloudUserId);
  const credentialEntry = managedEmail ? credentials[managedEmail] : null;

  if (!stateEntry || !credentialEntry) {
    throw new Error(`Unknown managed user: ${nextcloudUserId}`);
  }

  const preferredLibrary = resolvePreferredLibraryState(stateEntry);
  if (!preferredLibrary?.libraryPath || !stateEntry.immichUserId || !stateEntry.email) {
    throw new Error(`Managed user is missing bridge identity data: ${nextcloudUserId}`);
  }

  const accessToken = await loginImmichUser(stateEntry.email, credentialEntry.password);
  return {
    nextcloudUserId,
    immichEmail: stateEntry.email,
    immichUserId: stateEntry.immichUserId,
    libraryPath: preferredLibrary.libraryPath,
    libraryName: preferredLibrary.libraryName || null,
    stateEntry,
    accessToken,
  };
}

async function createUserContextByImmichUserId(immichUserId) {
  const managedState = loadJson(managedStatePath, { users: {} });
  const users = managedState.users && typeof managedState.users === 'object' ? managedState.users : {};
  const managedEmail = Object.keys(users).find((candidate) => users[candidate]?.immichUserId === immichUserId);

  if (!managedEmail) {
    throw new Error(`Unknown managed Immich user: ${immichUserId}`);
  }

  return createManagedContext(users[managedEmail].nextcloudUserId || managedEmail, users[managedEmail]);
}

async function createAuthenticatedUserContextByImmichUserId(immichUserId) {
  const managedState = loadJson(managedStatePath, { users: {} });
  const users = managedState.users && typeof managedState.users === 'object' ? managedState.users : {};
  const managedEmail = Object.keys(users).find((candidate) => users[candidate]?.immichUserId === immichUserId);

  if (!managedEmail) {
    throw new Error(`Unknown managed Immich user: ${immichUserId}`);
  }

  return createUserContext(users[managedEmail].nextcloudUserId || managedEmail);
}

function createManagedContext(nextcloudUserId, stateEntry) {
  if (!stateEntry) {
    throw new Error(`Unknown managed user: ${nextcloudUserId}`);
  }

  const preferredLibrary = resolvePreferredLibraryState(stateEntry);
  if (!preferredLibrary?.libraryPath || !stateEntry.immichUserId || !stateEntry.email) {
    throw new Error(`Managed user is missing bridge identity data: ${nextcloudUserId}`);
  }

  return {
    nextcloudUserId: stateEntry.nextcloudUserId || nextcloudUserId,
    immichEmail: stateEntry.email,
    immichUserId: stateEntry.immichUserId,
    libraryPath: preferredLibrary.libraryPath,
    libraryName: preferredLibrary.libraryName || null,
    stateEntry,
  };
}

function findManagedUserByNextcloudUserId(managedState, nextcloudUserId) {
  const users = managedState.users && typeof managedState.users === 'object' ? managedState.users : {};
  for (const [email, entry] of Object.entries(users)) {
    if (entry?.nextcloudUserId === nextcloudUserId) {
      return [email, entry];
    }
  }

  return [null, null];
}

function resolvePreferredLibraryState(stateEntry) {
  if (stateEntry.libraryPath) {
    return {
      libraryPath: stateEntry.libraryPath,
      libraryName: stateEntry.libraryName || null,
    };
  }

  const libraries = stateEntry.libraries && typeof stateEntry.libraries === 'object' ? stateEntry.libraries : {};
  return libraries.photos || Object.values(libraries)[0] || null;
}

async function validateAssetsOwned(context, assetIds) {
  const assets = [];

  for (const assetId of assetIds) {
    const asset = await immichRequest(context.accessToken, 'GET', `/assets/${assetId}`);
    const originalPath = extractOriginalPath(asset);
    const ownerId = asset.ownerId || asset.owner?.id || null;

    if (!originalPath) {
      throw new Error(`Unable to resolve original path for asset ${assetId}`);
    }

    const ownedByPath = originalPath.startsWith(context.libraryPath);
    const ownedByUser = ownerId === context.immichUserId;
    if (!ownedByPath && !ownedByUser) {
      throw new Error(`Asset ${assetId} does not belong to ${context.nextcloudUserId}`);
    }

    assets.push({
      id: asset.id,
      ownerId,
      originalPath,
      fileName: path.posix.basename(originalPath),
    });
  }

  return {
    assetCount: assets.length,
    assets,
  };
}

async function processImmichEvent(eventRecord) {
  const isTrashEvent = eventRecord.eventType === 'AssetTrashAll';
  const isRestoreEvent = eventRecord.eventType === 'AssetRestoreAll';
  const isDeleteEvent = eventRecord.eventType === 'AssetDeleteAll';
  const isSmartAlbumEvent = ['AssetMetadataExtracted', 'AssetFacesRecognized'].includes(eventRecord.eventType);

  if (!isTrashEvent && !isRestoreEvent && !isDeleteEvent && !isSmartAlbumEvent) {
    const result = {
      eventId: eventRecord.id,
      status: 'ignored',
      reason: `Unsupported event type: ${eventRecord.eventType}`,
      assets: [],
    };
    writeAuditEntry({
      kind: 'nextcloud-trash-skipped',
      eventId: eventRecord.id,
      eventType: eventRecord.eventType,
      reason: result.reason,
    });
    return result;
  }

  if (isSmartAlbumEvent) {
    return processSmartAlbumEvent(eventRecord);
  }

  if (isTrashEvent && !config.nextcloudTrashSyncEnabled) {
    return {
      eventId: eventRecord.id,
      status: 'disabled',
      reason: 'MEDIA_OPS_NEXTCLOUD_TRASH_SYNC_ENABLED=false',
      assets: [],
    };
  }

  if (isRestoreEvent && !config.nextcloudTrashRestoreEnabled) {
    return {
      eventId: eventRecord.id,
      status: 'disabled',
      reason: 'MEDIA_OPS_NEXTCLOUD_TRASH_RESTORE_ENABLED=false',
      assets: [],
    };
  }

  if (isDeleteEvent && !config.nextcloudTrashSyncEnabled) {
    return {
      eventId: eventRecord.id,
      status: 'disabled',
      reason: 'MEDIA_OPS_NEXTCLOUD_TRASH_SYNC_ENABLED=false',
      assets: [],
    };
  }

  const context = await createUserContextByImmichUserId(eventRecord.userId);
  const state = loadTrashSyncState();
  const deleteLookupIndex = pruneDeleteLookupIndex(loadDeleteLookupIndex());
  const results = [];

  for (const assetId of eventRecord.assetIds) {
    try {
      const assetResult = isTrashEvent
        ? await applyNextcloudTrashEvent(context, state, deleteLookupIndex, eventRecord, assetId)
        : isRestoreEvent
          ? await applyNextcloudRestoreEvent(context, state, deleteLookupIndex, eventRecord, assetId)
          : await applyNextcloudDeleteEvent(context, state, deleteLookupIndex, eventRecord, assetId);
      results.push(assetResult);
    } catch (error) {
      const failure = {
        assetId,
        status: 'error',
        message: String(error.message || error),
      };
      results.push(failure);
      writeAuditEntry({
        kind: isDeleteEvent ? 'nextcloud-delete-error' : isRestoreEvent ? 'nextcloud-restore-error' : 'nextcloud-trash-error',
        eventId: eventRecord.id,
        assetId,
        nextcloudUserId: context.nextcloudUserId,
        status: 'error',
        message: failure.message,
      });
      recordTrashSyncEntry(state, {
        eventId: eventRecord.id,
        eventType: eventRecord.eventType,
        assetId,
        nextcloudUserId: context.nextcloudUserId,
        status: 'error',
        lastError: failure.message,
      });
    }
  }

  saveTrashSyncState(state);
  saveDeleteLookupIndex(deleteLookupIndex);

  return {
    eventId: eventRecord.id,
    status: results.some((item) => item.status === 'error') ? 'partial_failure' : 'success',
    nextcloudUserId: context.nextcloudUserId,
    assets: results,
  };
}

async function processSmartAlbumEvent(eventRecord) {
  if (!config.smartAlbumsEnabled) {
    return {
      eventId: eventRecord.id,
      status: 'disabled',
      reason: 'MEDIA_OPS_SMART_ALBUMS_ENABLED=false',
      assets: [],
    };
  }

  const context = await createAuthenticatedUserContextByImmichUserId(eventRecord.userId);
  const runtime = createSmartAlbumRuntime();
  const results = [];

  for (const assetId of eventRecord.assetIds) {
    try {
      results.push(await reconcileSmartAlbumsForAsset(context, runtime, assetId, eventRecord));
    } catch (error) {
      const failure = {
        assetId,
        status: 'error',
        message: String(error.message || error),
      };
      results.push(failure);
      writeAuditEntry({
        kind: 'smart-album-error',
        eventId: eventRecord.id,
        eventType: eventRecord.eventType,
        assetId,
        nextcloudUserId: context.nextcloudUserId,
        message: failure.message,
      });
    }
  }

  return {
    eventId: eventRecord.id,
    eventType: eventRecord.eventType,
    nextcloudUserId: context.nextcloudUserId,
    smartAlbumsDryRun: config.smartAlbumsDryRun,
    status: summarizePerAssetStatus(results),
    assets: results,
  };
}

async function applyNextcloudTrashEvent(context, state, deleteLookupIndex, eventRecord, assetId) {
  const asset = await lookupAssetForEvent(assetId);
  const originalPath = asset.originalPath || null;
  const ownerId = asset.ownerId || null;
  const library = resolveManagedLibraryForPath(context.stateEntry, originalPath);
  const relativePath = buildManagedNextcloudRelativePath(context.nextcloudUserId, originalPath);

  if (!originalPath || !library || !relativePath || ownerId !== context.immichUserId) {
    const skipped = {
      assetId,
      status: 'skipped_unmanaged',
      originalPath: originalPath || null,
      relativePath: relativePath || null,
    };
    recordTrashSyncEntry(state, {
      eventId: eventRecord.id,
      eventType: eventRecord.eventType,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      originalPath: originalPath || null,
      relativePath: relativePath || null,
      librarySourceKey: library?.sourceKey || null,
      status: skipped.status,
      lastError: null,
    });
    writeAuditEntry({
      kind: 'nextcloud-trash-skipped',
      eventId: eventRecord.id,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      reason: 'unmanaged_or_unmappable_path',
      originalPath: originalPath || null,
    });
    return skipped;
  }

  const deleteResult = runNextcloudFileTrashDelete(context.nextcloudUserId, relativePath);
  const status = deleteResult.status === 'trashed' ? 'trashed' : deleteResult.status === 'missing_source' ? 'already_trashed_or_missing' : deleteResult.status;

  recordTrashSyncEntry(state, {
    eventId: eventRecord.id,
    eventType: eventRecord.eventType,
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    librarySourceKey: library.sourceKey,
    status,
    appliedAt: status === 'trashed' ? new Date().toISOString() : undefined,
    restoredAt: null,
    lastError: deleteResult.status === 'error' ? deleteResult.message : null,
  });
  refreshDeleteLookupEntry(deleteLookupIndex, {
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    librarySourceKey: library.sourceKey,
    lastSeenEventType: eventRecord.eventType,
  });

  writeAuditEntry({
    kind: deleteResult.status === 'error' ? 'nextcloud-trash-error' : 'nextcloud-trash-applied',
    eventId: eventRecord.id,
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    status,
    message: deleteResult.message,
  });

  return {
    assetId,
    librarySourceKey: library.sourceKey,
    originalPath,
    relativePath,
    status,
    message: deleteResult.message,
  };
}

async function applyNextcloudRestoreEvent(context, state, deleteLookupIndex, eventRecord, assetId) {
  const asset = await lookupAssetForEvent(assetId);
  const originalPath = asset.originalPath || null;
  const ownerId = asset.ownerId || null;
  const library = resolveManagedLibraryForPath(context.stateEntry, originalPath);
  const relativePath = buildManagedNextcloudRelativePath(context.nextcloudUserId, originalPath);

  if (!originalPath || !library || !relativePath || ownerId !== context.immichUserId) {
    const skipped = {
      assetId,
      status: 'skipped_unmanaged',
      originalPath: originalPath || null,
      relativePath: relativePath || null,
    };
    recordTrashSyncEntry(state, {
      eventId: eventRecord.id,
      eventType: eventRecord.eventType,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      originalPath: originalPath || null,
      relativePath: relativePath || null,
      librarySourceKey: library?.sourceKey || null,
      status: skipped.status,
      lastError: null,
    });
    writeAuditEntry({
      kind: 'nextcloud-restore-skipped',
      eventId: eventRecord.id,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      reason: 'unmanaged_or_unmappable_path',
      originalPath: originalPath || null,
    });
    return skipped;
  }

  const restoreResult = runNextcloudFileTrashRestore(context.nextcloudUserId, relativePath);

  recordTrashSyncEntry(state, {
    eventId: eventRecord.id,
    eventType: eventRecord.eventType,
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    librarySourceKey: library.sourceKey,
    status: restoreResult.status,
    restoredAt: restoreResult.status === 'restored' || restoreResult.status === 'already_restored' ? new Date().toISOString() : undefined,
    lastError: restoreResult.status === 'error' ? restoreResult.message : null,
  });
  refreshDeleteLookupEntry(deleteLookupIndex, {
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    librarySourceKey: library.sourceKey,
    lastSeenEventType: eventRecord.eventType,
  });

  writeAuditEntry({
    kind: restoreResult.status === 'error' ? 'nextcloud-restore-error' : 'nextcloud-restore-applied',
    eventId: eventRecord.id,
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    status: restoreResult.status,
    message: restoreResult.message,
  });

  return {
    assetId,
    librarySourceKey: library.sourceKey,
    originalPath,
    relativePath,
    status: restoreResult.status,
    message: restoreResult.message,
  };
}

async function applyNextcloudDeleteEvent(context, state, deleteLookupIndex, eventRecord, assetId) {
  const target = await resolveDeleteTargetForEvent(context, state, deleteLookupIndex, assetId);
  const originalPath = target.originalPath || null;
  const ownerId = target.ownerId || null;
  const library = resolveManagedLibraryForPath(context.stateEntry, originalPath)
    || (target.librarySourceKey ? { sourceKey: target.librarySourceKey } : null);
  const relativePath = target.relativePath || buildManagedNextcloudRelativePath(context.nextcloudUserId, originalPath);

  if (!originalPath || !library || !relativePath || ownerId !== context.immichUserId) {
    const skipped = {
      assetId,
      status: 'skipped_unmanaged',
      originalPath: originalPath || null,
      relativePath: relativePath || null,
    };
    recordTrashSyncEntry(state, {
      eventId: eventRecord.id,
      eventType: eventRecord.eventType,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      originalPath: originalPath || null,
      relativePath: relativePath || null,
      librarySourceKey: library?.sourceKey || null,
      status: skipped.status,
      lastError: null,
    });
    writeAuditEntry({
      kind: 'nextcloud-delete-skipped',
      eventId: eventRecord.id,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      reason: 'unmanaged_or_unmappable_path',
      originalPath: originalPath || null,
    });
    return skipped;
  }

  if (['trash-sync-state', 'delete-lookup-index', 'audit-log'].includes(target.source)) {
    writeAuditEntry({
      kind:
        target.source === 'audit-log'
          ? 'resolved_from_audit_log'
          : target.source === 'delete-lookup-index'
            ? 'resolved_from_delete_lookup_index'
            : 'resolved_from_trash_sync_state',
      eventId: eventRecord.id,
      eventType: eventRecord.eventType,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      originalPath,
      relativePath,
    });
  }

  const deleteResult = runNextcloudFileTrashDeletePermanent(context.nextcloudUserId, relativePath);
  const status =
    deleteResult.status === 'deleted'
      ? 'deleted_from_nextcloud_trash'
      : deleteResult.status === 'not_found'
        ? 'already_deleted_or_missing'
        : deleteResult.status;

  recordTrashSyncEntry(state, {
    eventId: eventRecord.id,
    eventType: eventRecord.eventType,
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    librarySourceKey: library.sourceKey,
    status,
    deletedAt: status === 'deleted_from_nextcloud_trash' ? new Date().toISOString() : undefined,
    lastError: deleteResult.status === 'error' ? deleteResult.message : null,
  });
  refreshDeleteLookupEntry(deleteLookupIndex, {
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    librarySourceKey: library.sourceKey,
    lastSeenEventType: eventRecord.eventType,
  });

  writeAuditEntry({
    kind: deleteResult.status === 'error' ? 'nextcloud-delete-error' : 'nextcloud-delete-applied',
    eventId: eventRecord.id,
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    relativePath,
    status,
    message: deleteResult.message,
  });

  return {
    assetId,
    librarySourceKey: library.sourceKey,
    originalPath,
    relativePath,
    status,
    message: deleteResult.message,
  };
}

async function resolveDeleteTargetForEvent(context, state, deleteLookupIndex, assetId) {
  try {
    const asset = await lookupAssetForEvent(assetId);
    return {
      ownerId: asset.ownerId || null,
      originalPath: asset.originalPath || null,
      relativePath: buildManagedNextcloudRelativePath(context.nextcloudUserId, asset.originalPath || null),
      librarySourceKey: resolveManagedLibraryForPath(context.stateEntry, asset.originalPath || null)?.sourceKey || null,
      source: 'asset',
    };
  } catch (error) {
    if (!String(error.message || error).includes(`Asset ${assetId} not found in database`)) {
      throw error;
    }
  }

  const fallback = state.assets?.[assetId];
  if (fallback) {
    return {
      ownerId: context.immichUserId,
      originalPath: fallback.originalPath || null,
      relativePath: fallback.relativePath || buildManagedNextcloudRelativePath(context.nextcloudUserId, fallback.originalPath || null),
      librarySourceKey: fallback.librarySourceKey || null,
      source: 'trash-sync-state',
    };
  }

  const deleteLookupFallback = deleteLookupIndex.assets?.[assetId];
  if (deleteLookupFallback && deleteLookupFallback.nextcloudUserId === context.nextcloudUserId) {
    return {
      ownerId: context.immichUserId,
      originalPath: deleteLookupFallback.originalPath || null,
      relativePath: deleteLookupFallback.relativePath || buildManagedNextcloudRelativePath(context.nextcloudUserId, deleteLookupFallback.originalPath || null),
      librarySourceKey: deleteLookupFallback.librarySourceKey || null,
      source: 'delete-lookup-index',
    };
  }

  const auditFallback = lookupDeleteTargetFromAuditLog(assetId, context.nextcloudUserId);
  if (!auditFallback) {
    throw new Error(`Asset ${assetId} not found in database, trash sync state, delete lookup index, or audit log`);
  }

  return {
    ownerId: context.immichUserId,
    originalPath: auditFallback.originalPath || null,
    relativePath: auditFallback.relativePath || buildManagedNextcloudRelativePath(context.nextcloudUserId, auditFallback.originalPath || null),
    librarySourceKey: auditFallback.librarySourceKey || null,
    source: 'audit-log',
  };
}

function lookupDeleteTargetFromAuditLog(assetId, nextcloudUserId) {
  if (!fs.existsSync(auditLogPath)) {
    return null;
  }

  const lines = fs.readFileSync(auditLogPath, 'utf8').trim().split('\n').filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    let entry;
    try {
      entry = JSON.parse(lines[index]);
    } catch {
      continue;
    }

    if (entry?.assetId !== assetId) {
      continue;
    }

    if (nextcloudUserId && entry?.nextcloudUserId && entry.nextcloudUserId !== nextcloudUserId) {
      continue;
    }

    if (!['nextcloud-trash-applied', 'nextcloud-restore-applied', 'nextcloud-delete-applied'].includes(entry.kind)) {
      continue;
    }

    return {
      originalPath: entry.originalPath || null,
      relativePath: entry.relativePath || null,
      librarySourceKey: entry.librarySourceKey || null,
      kind: entry.kind,
      eventId: entry.eventId || null,
    };
  }

  return null;
}

function buildUserDestinationPath(context, destinationRelative) {
  const cleanRelative = destinationRelative.replace(/^\/+/, '').replace(/\.\./g, '');
  const basePath = context.libraryPath;
  const destinationPath = path.posix.join(basePath, cleanRelative);
  if (!destinationPath.startsWith(basePath)) {
    throw new Error('Destination path escapes the user library root');
  }
  return destinationPath;
}

function createSmartAlbumRuntime() {
  return {
    albumsByName: new Map(),
  };
}

async function reconcileSmartAlbumsForAsset(context, runtime, assetId, eventRecord) {
  const asset = await lookupAssetForEvent(assetId);
  const originalPath = asset.originalPath || null;

  if (!originalPath || asset.ownerId !== context.immichUserId) {
    const skipped = {
      assetId,
      eventType: eventRecord?.eventType || null,
      status: 'skipped_unmanaged',
      originalPath,
    };
    writeAuditEntry({
      kind: 'smart-album-skipped',
      eventId: eventRecord?.id || null,
      eventType: eventRecord?.eventType || null,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      reason: 'unmanaged_or_missing_path',
      originalPath,
    });
    return skipped;
  }

  if (asset.isTrashed) {
    const skipped = {
      assetId,
      eventType: eventRecord?.eventType || null,
      status: 'skipped_trashed',
      originalPath,
    };
    writeAuditEntry({
      kind: 'smart-album-skipped',
      eventId: eventRecord?.id || null,
      eventType: eventRecord?.eventType || null,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      reason: 'asset_trashed',
      originalPath,
    });
    return skipped;
  }

  const fileName = path.posix.basename(originalPath);
  const faceCount = lookupAssetFaceCount(assetId);
  const desiredAlbumNames = classifySmartAlbums({ faceCount, fileName }).map((album) => album.name);
  const existingMemberships = lookupManagedSmartAlbumMemberships(context.immichUserId, assetId);
  const existingAlbumNames = new Set(existingMemberships.map((album) => album.albumName));
  const desiredAlbumNameSet = new Set(desiredAlbumNames);
  const additions = desiredAlbumNames.filter((albumName) => !existingAlbumNames.has(albumName));
  const removals = existingMemberships.filter((album) => !desiredAlbumNameSet.has(album.albumName));
  const createdAlbums = [];
  const plannedAlbumCreates = [];

  for (const albumName of additions) {
    const albumRef = await ensureManagedSmartAlbum(context, runtime, albumName, assetId);
    if (albumRef.created) {
      createdAlbums.push(albumName);
    }
    if (albumRef.plannedCreate) {
      plannedAlbumCreates.push(albumName);
    }
    if (config.smartAlbumsDryRun || !albumRef.id) {
      continue;
    }
    await immichRequest(context.accessToken, 'PUT', `/albums/${albumRef.id}/assets`, { ids: [assetId] });
    writeAuditEntry({
      kind: 'smart-album-membership-added',
      eventId: eventRecord?.id || null,
      eventType: eventRecord?.eventType || null,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      albumId: albumRef.id,
      albumName,
      originalPath,
    });
  }

  for (const album of removals) {
    if (!config.smartAlbumsDryRun) {
      await immichRequest(context.accessToken, 'DELETE', `/albums/${album.id}/assets`, { ids: [assetId] });
    }
    writeAuditEntry({
      kind: config.smartAlbumsDryRun ? 'smart-album-membership-remove-planned' : 'smart-album-membership-removed',
      eventId: eventRecord?.id || null,
      eventType: eventRecord?.eventType || null,
      assetId,
      nextcloudUserId: context.nextcloudUserId,
      albumId: album.id,
      albumName: album.albumName,
      originalPath,
    });
  }

  writeAuditEntry({
    kind: config.smartAlbumsDryRun ? 'smart-album-reconcile-planned' : 'smart-album-reconciled',
    eventId: eventRecord?.id || null,
    eventType: eventRecord?.eventType || null,
    assetId,
    nextcloudUserId: context.nextcloudUserId,
    originalPath,
    fileName,
    faceCount,
    desiredAlbumNames,
    existingAlbumNames: existingMemberships.map((album) => album.albumName),
    additions,
    removals: removals.map((album) => album.albumName),
    createdAlbums,
    plannedAlbumCreates,
  });

  return {
    assetId,
    eventType: eventRecord?.eventType || null,
    status:
      additions.length === 0 && removals.length === 0 && createdAlbums.length === 0
        ? 'unchanged'
        : config.smartAlbumsDryRun
          ? 'planned'
          : 'applied',
    originalPath,
    fileName,
    faceCount,
    desiredAlbumNames,
    existingAlbumNames: existingMemberships.map((album) => album.albumName),
    additions,
    removals: removals.map((album) => album.albumName),
    createdAlbums,
    plannedAlbumCreates,
    smartAlbumsDryRun: config.smartAlbumsDryRun,
  };
}

function classifySmartAlbums(input) {
  return SMART_ALBUM_DEFINITIONS.filter((definition) => definition.matcher(input));
}

async function ensureManagedSmartAlbum(context, runtime, albumName, seedAssetId) {
  const cached = runtime.albumsByName.get(albumName);
  if (cached) {
    return { ...cached, created: false, plannedCreate: false };
  }

  const existing = lookupOwnedAlbumByName(context.immichUserId, albumName);
  if (existing) {
    runtime.albumsByName.set(albumName, existing);
    return existing;
  }

  if (config.smartAlbumsDryRun) {
    const planned = { id: null, albumName, created: false, plannedCreate: true };
    runtime.albumsByName.set(albumName, { id: null, albumName, created: false, plannedCreate: false });
    writeAuditEntry({
      kind: 'smart-album-create-planned',
      nextcloudUserId: context.nextcloudUserId,
      albumName,
      seedAssetId,
    });
    return planned;
  }

  const created = await immichRequest(context.accessToken, 'POST', '/albums', {
    albumName,
    assetIds: [],
  });
  const createdAlbum = {
    id: created.id,
    albumName,
    created: true,
  };
  runtime.albumsByName.set(albumName, { id: created.id, albumName, created: false, plannedCreate: false });
  writeAuditEntry({
    kind: 'smart-album-created',
    nextcloudUserId: context.nextcloudUserId,
    albumId: created.id,
    albumName,
    seedAssetId,
  });
  return createdAlbum;
}

function lookupAssetFaceCount(assetId) {
  const row = runPostgresQuery(
    `select count(*) from asset_face where "assetId" = ${sqlString(assetId)} and "deletedAt" is null and "isVisible" is true;`,
  );
  return Number.parseInt(row || '0', 10) || 0;
}

function lookupManagedSmartAlbumMemberships(ownerId, assetId) {
  const rows = runPostgresRowsQuery(`
    select album.id, album."albumName"
    from album
    join album_asset on album_asset."albumId" = album.id
    where album."ownerId" = ${sqlString(ownerId)}
      and album_asset."assetId" = ${sqlString(assetId)}
      and album."deletedAt" is null
      and album."albumName" in (${sqlStringList(getManagedSmartAlbumNames())});
  `);

  return rows.map((line) => {
    const [id, albumName] = line.split('\t');
    return { id, albumName };
  });
}

function lookupOwnedAlbumByName(ownerId, albumName) {
  const row = runPostgresQuery(`
    select id || E'\\t' || "albumName"
    from album
    where "ownerId" = ${sqlString(ownerId)}
      and "albumName" = ${sqlString(albumName)}
      and "deletedAt" is null
    order by "createdAt" desc
    limit 1;
  `);

  if (!row) {
    return null;
  }

  const [id, resolvedAlbumName] = row.split('\t');
  return { id, albumName: resolvedAlbumName, created: false };
}

function getManagedSmartAlbumNames() {
  return SMART_ALBUM_DEFINITIONS.map((definition) => definition.name);
}

function summarizePerAssetStatus(results) {
  if (results.some((item) => item.status === 'error')) {
    return 'partial_failure';
  }
  if (results.some((item) => item.status === 'applied')) {
    return 'success';
  }
  if (results.some((item) => item.status === 'planned')) {
    return 'planned';
  }
  return 'success';
}

function isScreenshotCandidate(fileName) {
  const value = String(fileName || '').toLowerCase();
  return /(screenshot|screen shot|screen[_ -]?shot)/i.test(value);
}

function isWhatsAppCandidate(fileName) {
  const value = String(fileName || '').toLowerCase();
  return (
    /^img-\d{8}-wa\d+\.(jpe?g|png|heic|webp)$/i.test(value) ||
    /^vid-\d{8}-wa\d+\.(mp4|mov|3gp)$/i.test(value) ||
    /^whatsapp image \d{4}-\d{2}-\d{2} at .+\.(jpe?g|png|heic|webp)$/i.test(value) ||
    /^whatsapp video \d{4}-\d{2}-\d{2} at .+\.(mp4|mov|3gp)$/i.test(value)
  );
}

function isDocumentCandidate(fileName) {
  const value = String(fileName || '').toLowerCase();
  if (!/\.(jpe?g)$/.test(value)) {
    return false;
  }
  return /(^|[^a-z])(scan|document|doc|receipt|invoice)([^a-z]|$)/.test(value);
}

async function syncNextcloudAlbumWriteback(context, albumName, assetIds = [], validation = null) {
  if (!config.nextcloudAlbumWritebackEnabled) {
    return {
      attempted: false,
      applied: false,
      status: 'disabled',
      errors: [],
      albumName,
      containerName: config.nextcloudContainerName,
    };
  }

  const result = {
    attempted: true,
    applied: false,
    status: 'success',
    errors: [],
    albumName,
    containerName: config.nextcloudContainerName,
    createAlbum: null,
    assetAdds: [],
  };

  result.createAlbum = runNextcloudAlbumCreate(context.nextcloudUserId, albumName);

  if (result.createAlbum.status === 'error') {
    result.status = 'failed';
    result.errors.push(result.createAlbum.message);
    return result;
  }

  const assetValidation = validation || (assetIds.length > 0 ? await validateAssetsOwned(context, assetIds) : { assets: [] });

  for (const asset of assetValidation.assets) {
    const relativePath = buildNextcloudRelativeAssetPath(context, asset.originalPath);

    if (!relativePath) {
      result.status = 'partial_failure';
      result.errors.push(`Asset ${asset.id} is not within the user's Nextcloud Photos path: ${asset.originalPath}`);
      result.assetAdds.push({
        assetId: asset.id,
        relativePath: null,
        status: 'unmappable',
      });
      continue;
    }

    const addResult = runNextcloudAlbumAdd(context.nextcloudUserId, albumName, relativePath);
    result.assetAdds.push({
      assetId: asset.id,
      relativePath,
      status: addResult.status,
      message: addResult.message,
    });

    if (addResult.status === 'error') {
      result.status = 'partial_failure';
      result.errors.push(addResult.message);
    }
  }

  result.applied = result.status === 'success' || result.status === 'partial_failure';
  return result;
}

function buildNextcloudRelativeAssetPath(context, originalPath) {
  const filesRoot = `${path.posix.join(config.sourceRoot, context.nextcloudUserId, 'files')}/`;
  if (!originalPath.startsWith(filesRoot)) {
    return null;
  }

  const relativePath = originalPath.slice(filesRoot.length);
  if (
    !relativePath ||
    relativePath.startsWith('/') ||
    relativePath.includes('/../') ||
    relativePath === '..' ||
    !(relativePath === 'Photos' || relativePath.startsWith('Photos/'))
  ) {
    return null;
  }

  return relativePath;
}

function buildManagedNextcloudRelativePath(nextcloudUserId, originalPath) {
  if (!originalPath) {
    return null;
  }

  const filesRoot = `${path.posix.join(config.sourceRoot, nextcloudUserId, 'files')}/`;
  if (!originalPath.startsWith(filesRoot)) {
    return null;
  }

  const relativePath = originalPath.slice(filesRoot.length);
  if (!relativePath || relativePath.startsWith('/') || relativePath.includes('/../') || relativePath === '..') {
    return null;
  }

  return relativePath;
}

function resolveManagedLibraryForPath(stateEntry, originalPath) {
  if (!stateEntry || !originalPath) {
    return null;
  }

  const libraries = stateEntry.libraries && typeof stateEntry.libraries === 'object' ? stateEntry.libraries : {};
  for (const library of Object.values(libraries)) {
    if (!library?.libraryPath) {
      continue;
    }

    if (originalPath === library.libraryPath || originalPath.startsWith(`${library.libraryPath}/`)) {
      return library;
    }
  }

  return null;
}

function runNextcloudAlbumCreate(nextcloudUserId, albumName) {
  const command = ['exec', '-u', 'www-data', config.nextcloudContainerName, 'php', 'occ', 'photos:albums:create', nextcloudUserId, albumName];
  return runNextcloudOcc(command, {
    successStatus: 'created',
    idempotentPatterns: [/already exists/i],
    idempotentStatus: 'already_exists',
  });
}

function runNextcloudAlbumAdd(nextcloudUserId, albumName, relativePath) {
  const command = ['exec', '-u', 'www-data', config.nextcloudContainerName, 'php', 'occ', 'photos:albums:add', nextcloudUserId, albumName, relativePath];
  return runNextcloudOcc(command, {
    successStatus: 'added',
    idempotentPatterns: [/already.*album/i, /already in album/i, /unique violation/i, /duplicate key value/i, /paf_album_file/i],
    idempotentStatus: 'already_present',
  });
}

function runNextcloudFileTrashDelete(nextcloudUserId, relativePath) {
  return runNextcloudPhpJson(`
require '/var/www/html/lib/base.php';
$userId = $argv[1] ?? '';
$relativePath = ltrim($argv[2] ?? '', '/');
if ($userId === '' || $relativePath === '') {
  echo json_encode(['status' => 'error', 'message' => 'Missing user or path']);
  exit(1);
}
try {
  $userFolder = \\OC::$server->getUserFolder($userId);
  if (!$userFolder->nodeExists($relativePath)) {
    echo json_encode(['status' => 'missing_source', 'message' => 'File is already missing from active storage']);
    exit(0);
  }
  $node = $userFolder->get($relativePath);
  if (!$node->isDeletable()) {
    echo json_encode(['status' => 'error', 'message' => 'File cannot be deleted, insufficient permissions']);
    exit(0);
  }
  $node->delete();
  echo json_encode(['status' => 'trashed', 'message' => 'Moved to Nextcloud trash']);
} catch (\\Throwable $error) {
  echo json_encode(['status' => 'error', 'message' => $error->getMessage()]);
  exit(1);
}
`, [nextcloudUserId, relativePath], {
    successStatus: 'trashed',
  });
}

function runNextcloudFileTrashRestore(nextcloudUserId, relativePath) {
  return runNextcloudPhpJson(`
require '/var/www/html/lib/base.php';
$userId = $argv[1] ?? '';
$relativePath = ltrim($argv[2] ?? '', '/');
if ($userId === '' || $relativePath === '') {
  echo json_encode(['status' => 'error', 'message' => 'Missing user or path']);
  exit(1);
}
try {
  \\OC_Util::tearDownFS();
  \\OC_Util::setupFS($userId);
  \\OC_User::setUserId($userId);
  $userFolder = \\OC::$server->getUserFolder($userId);
  if ($userFolder->nodeExists($relativePath)) {
    echo json_encode(['status' => 'already_restored', 'message' => 'File already exists at active path']);
    exit(0);
  }
  $userManager = \\OC::$server->get(\\OCP\\IUserManager::class);
  $trashManager = \\OC::$server->get(\\OCA\\Files_Trashbin\\Trash\\ITrashManager::class);
  $user = $userManager->get($userId);
  if (!$user) {
    echo json_encode(['status' => 'error', 'message' => 'Unknown Nextcloud user']);
    exit(1);
  }
  $match = null;
  foreach ($trashManager->listTrashRoot($user) as $item) {
    if (ltrim($item->getOriginalLocation(), '/') !== $relativePath) {
      continue;
    }
    if ($match === null || $item->getDeletedTime() > $match->getDeletedTime()) {
      $match = $item;
    }
  }
  if ($match === null) {
    echo json_encode(['status' => 'not_found', 'message' => 'Matching trash item not found']);
    exit(0);
  }
  $trashManager->restoreItem($match);
  echo json_encode(['status' => 'restored', 'message' => 'Restored from Nextcloud trash']);
} catch (\\Throwable $error) {
  echo json_encode(['status' => 'error', 'message' => $error->getMessage()]);
  exit(1);
}
`, [nextcloudUserId, relativePath], {
    successStatus: 'restored',
  });
}

function runNextcloudFileTrashDeletePermanent(nextcloudUserId, relativePath) {
  return runNextcloudPhpJson(`
require '/var/www/html/lib/base.php';
$userId = $argv[1] ?? '';
$relativePath = ltrim($argv[2] ?? '', '/');
if ($userId === '' || $relativePath === '') {
  echo json_encode(['status' => 'error', 'message' => 'Missing user or path']);
  exit(1);
}
try {
  \\OC_Util::tearDownFS();
  \\OC_Util::setupFS($userId);
  \\OC_User::setUserId($userId);
  $userManager = \\OC::$server->get(\\OCP\\IUserManager::class);
  $trashManager = \\OC::$server->get(\\OCA\\Files_Trashbin\\Trash\\ITrashManager::class);
  $user = $userManager->get($userId);
  if (!$user) {
    echo json_encode(['status' => 'error', 'message' => 'Unknown Nextcloud user']);
    exit(1);
  }
  $match = null;
  foreach ($trashManager->listTrashRoot($user) as $item) {
    if (ltrim($item->getOriginalLocation(), '/') !== $relativePath) {
      continue;
    }
    if ($match === null || $item->getDeletedTime() > $match->getDeletedTime()) {
      $match = $item;
    }
  }
  if ($match === null) {
    echo json_encode(['status' => 'not_found', 'message' => 'Matching trash item already deleted or missing']);
    exit(0);
  }
  $trashManager->removeItem($match);
  echo json_encode(['status' => 'deleted', 'message' => 'Removed from Nextcloud trash']);
} catch (\\Throwable $error) {
  echo json_encode(['status' => 'error', 'message' => $error->getMessage()]);
  exit(1);
}
`, [nextcloudUserId, relativePath], {
    successStatus: 'deleted',
  });
}

function runNextcloudOcc(command, options) {
  const successStatus = options.successStatus;
  const idempotentPatterns = options.idempotentPatterns || [];
  const idempotentStatus = options.idempotentStatus || 'already_exists';

  try {
    const stdout = execFileSync('docker', command, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return {
      status: successStatus,
      message: sanitizeOccOutput(stdout) || successStatus,
    };
  } catch (error) {
    const combinedOutput = sanitizeOccOutput(`${error.stdout || ''}\n${error.stderr || ''}`);
    if (idempotentPatterns.some((pattern) => pattern.test(combinedOutput))) {
      return {
        status: idempotentStatus,
        message: combinedOutput || idempotentStatus,
      };
    }

    return {
      status: 'error',
      message: combinedOutput || String(error.message || error),
    };
  }
}

function runNextcloudPhpJson(script, args, options = {}) {
  try {
    const stdout = execFileSync('docker', [
      'exec',
      '-i',
      '-u',
      'www-data',
      config.nextcloudContainerName,
      'php',
      '-d',
      'display_errors=stderr',
      '-r',
      script,
      '--',
      ...args,
    ], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    return parseNextcloudPhpJson(stdout, options.successStatus || 'success');
  } catch (error) {
    const stdout = `${error.stdout || ''}\n${error.stderr || ''}`;
    return parseNextcloudPhpJson(stdout, 'error', error.message);
  }
}

function parseNextcloudPhpJson(output, fallbackStatus, fallbackMessage = null) {
  const sanitized = sanitizeOccOutput(output);
  const lines = sanitized ? sanitized.split('\n') : [];
  const rawJson = [...lines].reverse().find((line) => line.startsWith('{') && line.endsWith('}'));
  if (rawJson) {
    const parsed = safeJsonParse(rawJson);
    if (parsed && typeof parsed === 'object') {
      return {
        status: parsed.status || fallbackStatus,
        message: parsed.message || fallbackMessage || parsed.status || fallbackStatus,
      };
    }
  }

  return {
    status: fallbackStatus,
    message: fallbackMessage || sanitized || fallbackStatus,
  };
}

function runPostgresQuery(sql) {
  if (!config.dbPassword) {
    throw new Error('DB_PASSWORD is required for event asset lookup');
  }

  try {
    const stdout = execFileSync('docker', [
      'exec',
      '-e',
      `PGPASSWORD=${config.dbPassword}`,
      config.dbHostname,
      'psql',
      '-U',
      config.dbUsername,
      '-d',
      config.dbDatabaseName,
      '-Atc',
      sql,
    ], {
      encoding: 'utf8',
      maxBuffer: 20 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return stdout.trim() || null;
  } catch (error) {
    const output = sanitizeOccOutput(`${error.stdout || ''}\n${error.stderr || ''}`);
    throw new Error(`Postgres lookup failed: ${output || error.message}`);
  }
}

function runPostgresRowsQuery(sql) {
  if (!config.dbPassword) {
    throw new Error('DB_PASSWORD is required for Postgres queries');
  }

  try {
    const stdout = execFileSync('docker', [
      'exec',
      '-e',
      `PGPASSWORD=${config.dbPassword}`,
      config.dbHostname,
      'psql',
      '-U',
      config.dbUsername,
      '-d',
      config.dbDatabaseName,
      '-F',
      '\t',
      '-Atc',
      sql,
    ], {
      encoding: 'utf8',
      maxBuffer: 20 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return stdout
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean);
  } catch (error) {
    const output = sanitizeOccOutput(`${error.stdout || ''}\n${error.stderr || ''}`);
    throw new Error(`Postgres lookup failed: ${output || error.message}`);
  }
}

function sanitizeOccOutput(rawOutput) {
  return String(rawOutput || '')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => {
      if (!line) {
        return false;
      }
      return !(
        line.startsWith('WARNING:') ||
        line.startsWith('DETAIL:') ||
        line.startsWith('HINT:')
      );
    })
    .join('\n')
    .trim();
}

function sqlString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function sqlStringList(values) {
  return values.map((value) => sqlString(value)).join(', ');
}

function resolveAlbumName(album, albumId) {
  const albumName = album?.albumName || album?.name || null;
  if (!albumName) {
    throw new Error(`Unable to resolve album name for ${albumId}`);
  }
  return albumName;
}

function persistOperation(result) {
  fs.writeFileSync(lastOperationPath, JSON.stringify(result, null, 2));
  writeAuditEntry(result);
}

function loadOperationsState() {
  const state = loadJson(operationsStatePath, {
    trashBatches: [],
  });
  state.trashBatches = Array.isArray(state.trashBatches) ? state.trashBatches : [];
  return state;
}

function saveOperationsState(state) {
  fs.writeFileSync(operationsStatePath, JSON.stringify(state, null, 2));
}

function loadTrashSyncState() {
  const state = loadJson(trashSyncStatePath, {
    assets: {},
  });
  state.assets = state.assets && typeof state.assets === 'object' ? state.assets : {};
  return state;
}

function saveTrashSyncState(state) {
  fs.writeFileSync(trashSyncStatePath, JSON.stringify(state, null, 2));
}

function loadDeleteLookupIndex() {
  const state = loadJson(deleteLookupIndexPath, {
    assets: {},
  });
  state.assets = state.assets && typeof state.assets === 'object' ? state.assets : {};
  return state;
}

function saveDeleteLookupIndex(state) {
  fs.writeFileSync(deleteLookupIndexPath, JSON.stringify(state, null, 2));
}

function pruneDeleteLookupIndex(state) {
  const cutoff = Date.now() - deleteLookupTtlMs;
  for (const [assetId, entry] of Object.entries(state.assets || {})) {
    const timestamp = Date.parse(entry?.updatedAt || '');
    if (Number.isNaN(timestamp) || timestamp < cutoff) {
      delete state.assets[assetId];
    }
  }
  return state;
}

function refreshDeleteLookupEntry(state, entry) {
  if (!entry?.assetId || !entry?.nextcloudUserId || !entry?.originalPath) {
    return;
  }

  const existing = state.assets[entry.assetId] || {};
  state.assets[entry.assetId] = {
    ...existing,
    assetId: entry.assetId,
    nextcloudUserId: entry.nextcloudUserId,
    originalPath: entry.originalPath,
    relativePath: entry.relativePath || existing.relativePath || null,
    librarySourceKey: entry.librarySourceKey || existing.librarySourceKey || null,
    lastSeenEventType: entry.lastSeenEventType || existing.lastSeenEventType || null,
    updatedAt: new Date().toISOString(),
  };
}

function recordTrashSyncEntry(state, entry) {
  const existing = state.assets[entry.assetId] || {};
  state.assets[entry.assetId] = {
    ...existing,
    ...entry,
    updatedAt: new Date().toISOString(),
  };
}

function finalizeResult(operation, payload, context, details) {
  return {
    id: crypto.randomUUID(),
    operation,
    requestedAt: new Date().toISOString(),
    requestedBy: context.nextcloudUserId,
    immichEmail: context.immichEmail,
    dryRun: config.dryRun,
    writebackEnabled: config.writebackEnabled,
    deleteEnabled: config.deleteEnabled,
    folderMoveEnabled: config.folderMoveEnabled,
    request: sanitizePayload(payload),
    details,
  };
}

async function loginImmichUser(email, password) {
  const response = await fetch(`${config.immichApiUrl}/auth/login`, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ email, password }),
  });
  const text = await response.text();
  const data = text ? safeJsonParse(text) : null;
  if (!response.ok) {
    throw new Error(`Immich login failed for ${email}: ${response.status} ${text}`);
  }
  if (!data?.accessToken) {
    throw new Error(`Immich login did not return access token for ${email}`);
  }
  return data.accessToken;
}

async function immichRequest(accessToken, method, pathname, body) {
  const authHeaders = {};
  if (typeof accessToken === 'string' && accessToken) {
    authHeaders.Authorization = `Bearer ${accessToken}`;
  } else if (accessToken && typeof accessToken === 'object' && accessToken.sessionCookie) {
    authHeaders.Cookie = accessToken.sessionCookie;
  } else {
    throw new Error(`Missing authentication for ${method} ${pathname}`);
  }

  const response = await fetch(`${config.immichApiUrl}${pathname}`, {
    method,
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      ...authHeaders,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  const text = await response.text();
  const data = text ? safeJsonParse(text) : null;
  if (!response.ok) {
    throw new Error(`${method} ${pathname} failed with ${response.status}: ${text}`);
  }
  return data;
}

async function immichAdminRequest(method, pathname, body) {
  if (!config.immichAdminApiKey) {
    throw new Error('IMMICH_API_KEY is required for admin event processing');
  }

  const response = await fetch(`${config.immichApiUrl}${pathname}`, {
    method,
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      'x-api-key': config.immichAdminApiKey,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  const text = await response.text();
  const data = text ? safeJsonParse(text) : null;
  if (!response.ok) {
    throw new Error(`${method} ${pathname} failed with ${response.status}: ${text}`);
  }
  return data;
}

async function lookupAssetForEvent(assetId) {
  try {
    const asset = await immichAdminRequest('GET', `/assets/${assetId}`);
    return {
      id: asset.id,
      ownerId: asset.ownerId || asset.owner?.id || null,
      originalPath: extractOriginalPath(asset),
      isTrashed: Boolean(asset.isTrashed),
    };
  } catch (error) {
    if (!String(error.message || error).includes('403')) {
      throw error;
    }
  }

  return lookupAssetForEventFromDatabase(assetId);
}

function lookupAssetForEventFromDatabase(assetId) {
  const sql = `select id, "ownerId", "originalPath", ("deletedAt" is not null) as "isTrashed" from asset where id = '${String(assetId).replace(/'/g, "''")}' limit 1;`;
  const row = runPostgresQuery(sql);

  if (!row) {
    throw new Error(`Asset ${assetId} not found in database`);
  }

  const [id, ownerId, originalPath, isTrashed] = row.split('|');
  return {
    id,
    ownerId: ownerId || null,
    originalPath: originalPath || null,
    isTrashed: isTrashed === 't',
  };
}

function extractOriginalPath(asset) {
  return (
    asset.originalPath ||
    asset.originalFilePath ||
    asset.path ||
    asset.exifInfo?.filePath ||
    null
  );
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    request.on('data', (chunk) => chunks.push(chunk));
    request.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch (error) {
        reject(new Error(`Invalid JSON body: ${error.message}`));
      }
    });
    request.on('error', reject);
  });
}

function verifyInternalEventRequest(request) {
  if (!config.internalEventSecret) {
    throw new Error('MEDIA_OPS_INTERNAL_EVENT_SECRET is not configured');
  }

  const providedSecret = request.headers['x-immich-event-secret'];
  if (providedSecret !== config.internalEventSecret) {
    throw new Error('Unauthorized internal event request');
  }
}

function verifyUtilityMutationRequest(request) {
  const origin = request.headers.origin;
  if (origin && origin !== 'https://media.finestar.hr') {
    throw httpError(403, 'Unauthorized origin');
  }
}

function respondJson(response, statusCode, body) {
  response.statusCode = statusCode;
  response.setHeader('Content-Type', 'application/json');
  response.setHeader('Connection', 'close');
  response.shouldKeepAlive = false;
  response.end(JSON.stringify(body));
}

function respondHtml(response, statusCode, body) {
  response.statusCode = statusCode;
  response.setHeader('Content-Type', 'text/html; charset=utf-8');
  response.setHeader('Connection', 'close');
  response.shouldKeepAlive = false;
  response.end(body);
}

function writeAuditEntry(entry) {
  fs.appendFileSync(auditLogPath, `${JSON.stringify(entry)}\n`);
}

function parseCommand(argv) {
  const [command, ...args] = argv;
  if (!command) {
    return { command: 'serve', args: [] };
  }
  return { command, args };
}

function sanitizePayload(payload) {
  return JSON.parse(JSON.stringify(payload));
}

function compactObject(object) {
  return Object.fromEntries(Object.entries(object).filter(([, value]) => value !== undefined));
}

function httpError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function requiredString(value, name) {
  if (typeof value !== 'string' || !value.trim()) {
    throw new Error(`Missing required string field: ${name}`);
  }
  return value.trim();
}

function optionalString(value) {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

function requiredStringArray(value, name) {
  if (!Array.isArray(value) || value.length === 0 || value.some((item) => typeof item !== 'string' || !item.trim())) {
    throw new Error(`Missing required string array field: ${name}`);
  }
  return value.map((item) => item.trim());
}

function optionalStringArray(value) {
  if (value === undefined) {
    return undefined;
  }
  return requiredStringArray(value, 'assetIds');
}

function optionalBoolean(value) {
  return typeof value === 'boolean' ? value : undefined;
}

function optionalNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function optionalNullableNumber(value) {
  if (value === null) {
    return null;
  }
  return optionalNumber(value);
}

function parseBoolean(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getFileExtension(fileName) {
  return String(fileName || '').split('.').pop().toLowerCase();
}

function env(name, fallback) {
  return process.env[name] ?? fallback;
}

function stripTrailingSlash(value) {
  return value.endsWith('/') ? value.slice(0, -1) : value;
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function loadJson(filePath, fallback) {
  if (!fs.existsSync(filePath)) {
    return fallback;
  }

  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function log(message) {
  console.log(`[media-operations] ${new Date().toISOString()} ${message}`);
}

main().catch((error) => {
  log(`fatal: ${error.stack || error.message}`);
  process.exit(1);
});
