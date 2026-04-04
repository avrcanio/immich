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
};

const managedStatePath = path.join(config.bridgeStateDir, 'managed-state.json');
const credentialsPath = path.join(config.bridgeStateDir, 'credentials.json');
const operationsStatePath = path.join(config.stateDir, 'operations-state.json');
const lastOperationPath = path.join(config.stateDir, 'last-operation.json');
const auditLogPath = path.join(config.stateDir, 'audit.log');

fs.mkdirSync(config.stateDir, { recursive: true });

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
      if (request.method === 'GET' && request.url === '/healthz') {
        return respondJson(response, 200, {
          status: 'ok',
          dryRun: config.dryRun,
          writebackEnabled: config.writebackEnabled,
          deleteEnabled: config.deleteEnabled,
          folderMoveEnabled: config.folderMoveEnabled,
          nextcloudAlbumWritebackEnabled: config.nextcloudAlbumWritebackEnabled,
          nextcloudContainerName: config.nextcloudContainerName,
        });
      }

      if (request.method === 'GET' && request.url === '/capabilities') {
        return respondJson(response, 200, getCapabilities());
      }

      if (request.method === 'POST' && request.url === '/operations') {
        const payload = await readJsonBody(request);
        const result = await dispatchOperation(payload);
        return respondJson(response, 200, result);
      }

      respondJson(response, 404, { message: 'Not found' });
    } catch (error) {
      respondJson(response, 400, {
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
    ],
    destructiveOperationsRequireApply: [
      'confirm-delete-assets',
      'move-assets-to-folder',
    ],
    writebackMode: config.writebackEnabled ? 'opt-in-live' : 'audit-only',
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

async function createUserContext(nextcloudUserId) {
  const managedState = loadJson(managedStatePath, { users: {} });
  const credentials = loadJson(credentialsPath, {});
  const stateEntry = managedState.users?.[nextcloudUserId];
  const credentialEntry = credentials[nextcloudUserId];

  if (!stateEntry || !credentialEntry) {
    throw new Error(`Unknown managed user: ${nextcloudUserId}`);
  }

  if (!stateEntry.libraryPath || !stateEntry.immichUserId || !stateEntry.email) {
    throw new Error(`Managed user is missing bridge identity data: ${nextcloudUserId}`);
  }

  const accessToken = await loginImmichUser(stateEntry.email, credentialEntry.password);
  return {
    nextcloudUserId,
    immichEmail: stateEntry.email,
    immichUserId: stateEntry.immichUserId,
    libraryPath: stateEntry.libraryPath,
    libraryName: stateEntry.libraryName || null,
    accessToken,
  };
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

function buildUserDestinationPath(context, destinationRelative) {
  const cleanRelative = destinationRelative.replace(/^\/+/, '').replace(/\.\./g, '');
  const basePath = context.libraryPath;
  const destinationPath = path.posix.join(basePath, cleanRelative);
  if (!destinationPath.startsWith(basePath)) {
    throw new Error('Destination path escapes the user library root');
  }
  return destinationPath;
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

function resolveAlbumName(album, albumId) {
  const albumName = album?.albumName || album?.name || null;
  if (!albumName) {
    throw new Error(`Unable to resolve album name for ${albumId}`);
  }
  return albumName;
}

function persistOperation(result) {
  fs.writeFileSync(lastOperationPath, JSON.stringify(result, null, 2));
  fs.appendFileSync(auditLogPath, `${JSON.stringify(result)}\n`);
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
  const response = await fetch(`${config.immichApiUrl}${pathname}`, {
    method,
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      Authorization: `Bearer ${accessToken}`,
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

function respondJson(response, statusCode, body) {
  response.statusCode = statusCode;
  response.setHeader('Content-Type', 'application/json');
  response.end(JSON.stringify(body));
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
