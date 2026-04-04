const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const config = {
  apiUrl: stripTrailingSlash(env('IMMICH_API_URL', 'http://immich-server:2283/api')),
  apiKey: env('IMMICH_API_KEY', ''),
  sourceRoot: env('BRIDGE_SOURCE_ROOT', '/external-libraries/nextcloud-data'),
  libraryRoot: env('BRIDGE_LIBRARY_ROOT', '/external-libraries/nextcloud-data'),
  stateDir: env('BRIDGE_STATE_DIR', '/state'),
  intervalSeconds: parseInteger(env('BRIDGE_INTERVAL_SECONDS', '900'), 900),
  runOnce: parseBoolean(env('BRIDGE_RUN_ONCE', 'false')),
  dryRun: parseBoolean(env('BRIDGE_DRY_RUN', 'true')),
  defaultEmailDomain: env('BRIDGE_DEFAULT_EMAIL_DOMAIN', 'local.invalid'),
  passwordLength: parseInteger(env('BRIDGE_PASSWORD_LENGTH', '24'), 24),
  libraryNamePrefix: env('BRIDGE_LIBRARY_NAME_PREFIX', 'Nextcloud Photos -'),
  disableCandidateThreshold: parseInteger(env('BRIDGE_DISABLE_CANDIDATE_THRESHOLD', '3'), 3),
};

const credentialsPath = path.join(config.stateDir, 'credentials.json');
const reportPath = path.join(config.stateDir, 'last-run.json');
const managedStatePath = path.join(config.stateDir, 'managed-state.json');
const deprovisionReportPath = path.join(config.stateDir, 'last-deprovision.json');
const placeholderApiKey = 'REPLACE_WITH_IMMICH_ADMIN_API_KEY';

fs.mkdirSync(config.stateDir, { recursive: true });

async function main() {
  const { command, args } = parseCommand(process.argv.slice(2));

  if (command === 'deprovision') {
    await runDeprovision(args);
    return;
  }

  const runOnce = command === 'sync-once' ? true : command === 'sync-loop' ? false : config.runOnce;

  log(`bridge starting in ${runOnce ? 'sync-once' : 'sync-loop'} mode`);

  do {
    const startedAt = new Date().toISOString();
    const report = createReport(startedAt, runOnce ? 'sync-once' : 'sync-loop');

    try {
      await runSync(report);
    } catch (error) {
      report.errors.push(asError(error));
      log(`cycle failed: ${error.message}`);
    }

    finalizeReport(report);
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
    log(
      `cycle finished: processed=${report.usersProcessed}, createdUsers=${report.usersCreated.length}, createdLibraries=${report.librariesCreated.length}, mismatches=${report.mismatches.length}, errors=${report.errors.length}`,
    );

    if (runOnce) {
      break;
    }

    await sleep(config.intervalSeconds * 1000);
  } while (true);
}

function createReport(startedAt, mode) {
  return {
    startedAt,
    finishedAt: null,
    mode,
    dryRun: shouldDryRun(),
    sourceRoot: config.sourceRoot,
    disableCandidateThreshold: config.disableCandidateThreshold,
    summary: {
      nextcloudDirectories: 0,
      managedUsers: 0,
      eligible: 0,
      appdataSkipped: 0,
      missingPhotos: 0,
      dryRunCandidates: 0,
      usersCreated: 0,
      usersUpdated: 0,
      librariesCreated: 0,
      librariesUpdated: 0,
      librariesScanned: 0,
      mismatches: 0,
      disabledCandidates: 0,
      skipped: 0,
      errors: 0,
    },
    usersDiscovered: 0,
    usersProcessed: 0,
    nextcloudDirectories: [],
    eligibleUsers: [],
    appdataSkipped: [],
    missingPhotos: [],
    dryRunCandidates: [],
    usersCreated: [],
    usersUpdated: [],
    librariesCreated: [],
    librariesUpdated: [],
    librariesScanned: [],
    mismatches: [],
    disabledCandidates: [],
    lifecycle: [],
    skipped: [],
    errors: [],
  };
}

function finalizeReport(report) {
  report.finishedAt = new Date().toISOString();
  report.summary.usersCreated = report.usersCreated.length;
  report.summary.usersUpdated = report.usersUpdated.length;
  report.summary.librariesCreated = report.librariesCreated.length;
  report.summary.librariesUpdated = report.librariesUpdated.length;
  report.summary.librariesScanned = report.librariesScanned.length;
  report.summary.mismatches = report.mismatches.length;
  report.summary.disabledCandidates = report.disabledCandidates.length;
  report.summary.skipped = report.skipped.length;
  report.summary.errors = report.errors.length;
}

async function runSync(report) {
  const discovery = discoverUsers(report);
  const discoveredUsers = discovery.eligibleUsers;
  const directoryUserIds = new Set(discovery.directoryUserIds);
  const eligibleUserIds = new Set(discoveredUsers.map((user) => user.nextcloudUserId));
  const missingPhotosIds = new Set(report.missingPhotos.map((user) => user.nextcloudUserId));

  report.usersDiscovered = discoveredUsers.length;

  const credentials = normalizeCredentials(loadJson(credentialsPath, {}));
  const managedState = normalizeManagedState(loadJson(managedStatePath, { users: {} }));
  report.summary.managedUsers = countManagedUsers(credentials, managedState);

  if (shouldDryRun()) {
    for (const user of discoveredUsers) {
      report.usersProcessed += 1;
      report.dryRunCandidates.push({
        nextcloudUserId: user.nextcloudUserId,
        status: 'dry-run-candidate',
        displayName: user.displayName,
        libraryPath: user.libraryPath,
        immichEmail: user.immichEmail,
      });
    }

    report.summary.dryRunCandidates = report.dryRunCandidates.length;
    updateMismatchLifecycle({
      report,
      credentials,
      managedState,
      directoryUserIds,
      eligibleUserIds,
      missingPhotosIds,
      users: [],
      libraries: [],
      now: new Date().toISOString(),
    });
    fs.writeFileSync(managedStatePath, JSON.stringify(managedState, null, 2));
    return;
  }

  const users = await immichGet('/admin/users');
  const libraries = await immichGet('/libraries');
  const now = new Date().toISOString();

  for (const discovered of discoveredUsers) {
    report.usersProcessed += 1;

    const stateEntry = ensureManagedStateEntry(managedState, discovered.nextcloudUserId);
    if (stateEntry.status === 'deprovisioned') {
      report.skipped.push({
        type: 'manual_deprovision',
        nextcloudUserId: discovered.nextcloudUserId,
        immichEmail: discovered.immichEmail,
        libraryPath: discovered.libraryPath,
      });
      report.lifecycle.push(buildLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'manual_deprovision'));
      continue;
    }

    let existingUser = users.find((user) => user.email === discovered.immichEmail);
    let userId = existingUser?.id;

    if (!existingUser) {
      report.mismatches.push({
        type: 'missing_immich_user',
        nextcloudUserId: discovered.nextcloudUserId,
        immichEmail: discovered.immichEmail,
        resolution: 'create-user',
      });

      const generatedPassword = generatePassword(config.passwordLength);
      credentials[discovered.nextcloudUserId] = {
        email: discovered.immichEmail,
        password: generatedPassword,
        createdAt: now,
      };

      const createdUser = await immichPost('/admin/users', {
        email: discovered.immichEmail,
        password: generatedPassword,
        name: discovered.displayName,
        shouldChangePassword: true,
        notify: false,
        isAdmin: false,
      });

      userId = createdUser.id;
      existingUser = createdUser;
      users.push(createdUser);
      report.usersCreated.push({
        nextcloudUserId: discovered.nextcloudUserId,
        immichUserId: createdUser.id,
        immichEmail: discovered.immichEmail,
      });
    } else if (existingUser.name !== discovered.displayName) {
      const updatedUser = await immichPut(`/admin/users/${existingUser.id}`, {
        name: discovered.displayName,
      });
      const index = users.findIndex((candidate) => candidate.id === existingUser.id);
      if (index !== -1) {
        users[index] = updatedUser;
      }
      existingUser = updatedUser;
      report.usersUpdated.push({
        nextcloudUserId: discovered.nextcloudUserId,
        immichUserId: existingUser.id,
        fields: ['name'],
      });
      userId = updatedUser.id;
    }

    const desiredLibraryName = `${config.libraryNamePrefix} ${discovered.nextcloudUserId}`;
    const desiredImportPath = discovered.libraryPath;
    const stateLibrary = stateEntry.libraryId ? libraries.find((library) => library.id === stateEntry.libraryId) : null;
    const existingLibrary =
      stateLibrary ||
      libraries.find((library) => {
        const sameOwner = library.ownerId === userId;
        const importPaths = Array.isArray(library.importPaths) ? library.importPaths : [];
        return sameOwner && importPaths.includes(desiredImportPath);
      });

    let libraryId = existingLibrary?.id;

    if (!existingLibrary) {
      report.mismatches.push({
        type: 'missing_library',
        nextcloudUserId: discovered.nextcloudUserId,
        immichEmail: discovered.immichEmail,
        libraryPath: desiredImportPath,
        resolution: 'create-library',
      });

      const createdLibrary = await immichPost('/libraries', {
        ownerId: userId,
        name: desiredLibraryName,
        importPaths: [desiredImportPath],
        exclusionPatterns: [],
      });
      libraryId = createdLibrary.id;
      libraries.push(createdLibrary);
      report.librariesCreated.push({
        nextcloudUserId: discovered.nextcloudUserId,
        libraryId: createdLibrary.id,
        libraryPath: desiredImportPath,
      });
    } else {
      const desiredPayload = {
        name: desiredLibraryName,
        importPaths: [desiredImportPath],
        exclusionPatterns: [],
      };
      const currentPaths = JSON.stringify(existingLibrary.importPaths || []);
      const desiredPaths = JSON.stringify(desiredPayload.importPaths);
      const currentExclusions = JSON.stringify(existingLibrary.exclusionPatterns || []);
      const desiredExclusions = JSON.stringify(desiredPayload.exclusionPatterns);
      const needsUpdate =
        existingLibrary.name !== desiredPayload.name ||
        currentPaths !== desiredPaths ||
        currentExclusions !== desiredExclusions;

      if (needsUpdate) {
        const updatedLibrary = await immichPut(`/libraries/${existingLibrary.id}`, desiredPayload);
        const index = libraries.findIndex((candidate) => candidate.id === existingLibrary.id);
        if (index !== -1) {
          libraries[index] = updatedLibrary;
        }
        libraryId = updatedLibrary.id;
        report.librariesUpdated.push({
          nextcloudUserId: discovered.nextcloudUserId,
          libraryId: updatedLibrary.id,
          fields: ['name', 'importPaths', 'exclusionPatterns'],
        });
      }
    }

    stateEntry.email = discovered.immichEmail;
    stateEntry.immichUserId = userId;
    stateEntry.libraryId = libraryId || stateEntry.libraryId || null;
    stateEntry.libraryPath = desiredImportPath;
    stateEntry.libraryName = desiredLibraryName;
    stateEntry.lastSeenAt = now;
    stateEntry.lastSyncedAt = now;
    stateEntry.lastHealthyAt = now;
    stateEntry.lastMismatchAt = null;
    stateEntry.lastMismatchType = null;
    stateEntry.consecutiveMissingCycles = 0;
    stateEntry.disabledCandidateSince = null;
    stateEntry.scanPolicy = 'enabled';
    stateEntry.status = 'active';

    if (stateEntry.libraryId) {
      await immichPost(`/libraries/${stateEntry.libraryId}/scan`, {});
      report.librariesScanned.push({
        nextcloudUserId: discovered.nextcloudUserId,
        libraryId: stateEntry.libraryId,
      });
    }

    report.lifecycle.push(buildLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'synced'));
  }

  updateMismatchLifecycle({
    report,
    credentials,
    managedState,
    directoryUserIds,
    eligibleUserIds,
    missingPhotosIds,
    users,
    libraries,
    now,
  });

  report.summary.managedUsers = countManagedUsers(credentials, managedState);
  fs.writeFileSync(credentialsPath, JSON.stringify(credentials, null, 2));
  fs.writeFileSync(managedStatePath, JSON.stringify(managedState, null, 2));
}

function updateMismatchLifecycle({
  report,
  credentials,
  managedState,
  directoryUserIds,
  eligibleUserIds,
  missingPhotosIds,
  users,
  libraries,
  now,
}) {
  const managedUserIds = new Set([
    ...Object.keys(credentials),
    ...Object.keys(managedState.users || {}),
  ]);

  for (const nextcloudUserId of managedUserIds) {
    const stateEntry = ensureManagedStateEntry(managedState, nextcloudUserId);
    if (stateEntry.status === 'deprovisioned') {
      report.lifecycle.push(buildLifecycleRecord(nextcloudUserId, stateEntry, 'deprovisioned'));
      continue;
    }

    if (eligibleUserIds.has(nextcloudUserId)) {
      continue;
    }

    const mismatchType = directoryUserIds.has(nextcloudUserId) ? 'missing_photos_path' : 'missing_nextcloud_user';
    const library = stateEntry.libraryId ? libraries.find((candidate) => candidate.id === stateEntry.libraryId) : null;
    const immichUser = stateEntry.immichUserId ? users.find((candidate) => candidate.id === stateEntry.immichUserId) : null;
    registerManagedMismatch(report, stateEntry, {
      nextcloudUserId,
      mismatchType,
      now,
      immichEmail: stateEntry.email || credentials[nextcloudUserId]?.email || null,
      immichUserId: stateEntry.immichUserId || null,
      libraryId: stateEntry.libraryId || null,
      libraryPath: stateEntry.libraryPath || null,
      directoryPresent: directoryUserIds.has(nextcloudUserId),
      photosPathMissing: missingPhotosIds.has(nextcloudUserId),
      immichUserMissing: !immichUser && Boolean(stateEntry.immichUserId),
      libraryMissing: !library && Boolean(stateEntry.libraryId),
    });
  }

  const expectedEmails = new Set(
    Array.from(managedUserIds)
      .map((nextcloudUserId) => credentials[nextcloudUserId]?.email || managedState.users?.[nextcloudUserId]?.email)
      .filter(Boolean),
  );

  for (const user of users) {
    if (!expectedEmails.has(user.email) && !user.isAdmin) {
      report.mismatches.push({
        type: 'orphan_immich_user',
        immichUserId: user.id,
        immichEmail: user.email,
      });
    }
  }
}

function registerManagedMismatch(report, stateEntry, details) {
  const mismatchChanged = stateEntry.lastMismatchType !== details.mismatchType;
  stateEntry.consecutiveMissingCycles = mismatchChanged ? 1 : stateEntry.consecutiveMissingCycles + 1;
  stateEntry.lastMismatchAt = details.now;
  stateEntry.lastMismatchType = details.mismatchType;
  stateEntry.status = 'mismatch';

  report.mismatches.push({
    type: details.mismatchType,
    nextcloudUserId: details.nextcloudUserId,
    immichEmail: details.immichEmail,
    immichUserId: details.immichUserId,
    libraryId: details.libraryId,
    libraryPath: details.libraryPath,
    consecutiveMissingCycles: stateEntry.consecutiveMissingCycles,
    directoryPresent: details.directoryPresent,
    photosPathMissing: details.photosPathMissing,
    immichUserMissing: details.immichUserMissing,
    libraryMissing: details.libraryMissing,
  });

  if (stateEntry.consecutiveMissingCycles >= config.disableCandidateThreshold) {
    stateEntry.status = 'disabled_candidate';
    stateEntry.scanPolicy = 'disabled';
    stateEntry.disabledCandidateSince = stateEntry.disabledCandidateSince || details.now;
    report.disabledCandidates.push({
      type: 'disabled_candidate',
      nextcloudUserId: details.nextcloudUserId,
      mismatchType: details.mismatchType,
      immichEmail: details.immichEmail,
      libraryId: details.libraryId,
      libraryPath: details.libraryPath,
      consecutiveMissingCycles: stateEntry.consecutiveMissingCycles,
      scanPolicy: stateEntry.scanPolicy,
    });
  }

  report.lifecycle.push(buildLifecycleRecord(details.nextcloudUserId, stateEntry, details.mismatchType));
}

function discoverUsers(report) {
  const entries = fs.readdirSync(config.sourceRoot, { withFileTypes: true });
  const directoryUserIds = [];
  const eligibleUsers = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }

    if (entry.name.startsWith('appdata_')) {
      report.appdataSkipped.push({
        nextcloudUserId: entry.name,
        status: 'appdata-skipped',
      });
      continue;
    }

    directoryUserIds.push(entry.name);
    report.nextcloudDirectories.push({
      nextcloudUserId: entry.name,
      status: 'directory-discovered',
    });

    const hostPhotosPath = path.join(config.sourceRoot, entry.name, 'files', 'Photos');
    if (!fs.existsSync(hostPhotosPath) || !fs.statSync(hostPhotosPath).isDirectory()) {
      report.missingPhotos.push({
        nextcloudUserId: entry.name,
        status: 'missing-photos-directory',
        expectedLibraryPath: path.posix.join(config.libraryRoot, entry.name, 'files', 'Photos'),
      });
      continue;
    }

    const mappedUser = {
      nextcloudUserId: entry.name,
      displayName: entry.name,
      immichEmail: toImmichEmail(entry.name),
      libraryPath: path.posix.join(config.libraryRoot, entry.name, 'files', 'Photos'),
      status: 'eligible',
    };
    eligibleUsers.push(mappedUser);
    report.eligibleUsers.push(mappedUser);
  }

  report.summary.nextcloudDirectories = directoryUserIds.length;
  report.summary.eligible = report.eligibleUsers.length;
  report.summary.appdataSkipped = report.appdataSkipped.length;
  report.summary.missingPhotos = report.missingPhotos.length;

  return {
    eligibleUsers: eligibleUsers.sort((left, right) => left.nextcloudUserId.localeCompare(right.nextcloudUserId)),
    directoryUserIds: directoryUserIds.sort((left, right) => left.localeCompare(right)),
  };
}

async function runDeprovision(args) {
  const options = parseDeprovisionArgs(args);
  const credentials = normalizeCredentials(loadJson(credentialsPath, {}));
  const managedState = normalizeManagedState(loadJson(managedStatePath, { users: {} }));
  const stateEntry = ensureManagedStateEntry(managedState, options.user);
  const credentialEntry = credentials[options.user] || null;
  const now = new Date().toISOString();
  const beforeState = JSON.parse(JSON.stringify(stateEntry));
  const beforeCredentials = credentialEntry ? JSON.parse(JSON.stringify(credentialEntry)) : null;

  const result = {
    startedAt: now,
    finishedAt: null,
    mode: 'deprovision',
    apply: options.apply,
    nextcloudUserId: options.user,
    foundInCredentials: Boolean(credentialEntry),
    before: {
      state: beforeState,
      credentials: beforeCredentials,
    },
    actions: options.apply
      ? [
          'mark-state-as-deprovisioned',
          'disable-future-bridge-rescans-for-user',
          'keep-existing-immich-user-and-library-intact',
        ]
      : [
          'preview-only',
          'no-remote-changes',
          'no-local-state-written',
        ],
    warnings: [
      'This command does not delete Immich assets.',
      'This command does not delete Nextcloud files.',
      'This command does not delete Immich users or libraries.',
    ],
  };

  if (options.apply) {
    stateEntry.status = 'deprovisioned';
    stateEntry.scanPolicy = 'disabled';
    stateEntry.deprovisionedAt = now;
    stateEntry.lastMismatchAt = now;
    stateEntry.lastMismatchType = 'manual_deprovision';
    fs.writeFileSync(managedStatePath, JSON.stringify(managedState, null, 2));
  }

  result.finishedAt = new Date().toISOString();
  result.after = options.apply ? managedState.users[options.user] : stateEntry;
  fs.writeFileSync(deprovisionReportPath, JSON.stringify(result, null, 2));
  log(
    `${options.apply ? 'applied' : 'previewed'} deprovision for ${options.user}; audit written to ${deprovisionReportPath}`,
  );
}

function buildLifecycleRecord(nextcloudUserId, stateEntry, status) {
  return {
    nextcloudUserId,
    status,
    immichEmail: stateEntry.email || null,
    immichUserId: stateEntry.immichUserId || null,
    libraryId: stateEntry.libraryId || null,
    libraryPath: stateEntry.libraryPath || null,
    scanPolicy: stateEntry.scanPolicy || 'enabled',
    consecutiveMissingCycles: stateEntry.consecutiveMissingCycles || 0,
    disabledCandidateSince: stateEntry.disabledCandidateSince || null,
    deprovisionedAt: stateEntry.deprovisionedAt || null,
  };
}

function ensureManagedStateEntry(managedState, nextcloudUserId) {
  if (!managedState.users[nextcloudUserId]) {
    managedState.users[nextcloudUserId] = {
      email: null,
      immichUserId: null,
      libraryId: null,
      libraryPath: null,
      libraryName: null,
      status: 'new',
      scanPolicy: 'enabled',
      consecutiveMissingCycles: 0,
      disabledCandidateSince: null,
      deprovisionedAt: null,
      lastSeenAt: null,
      lastSyncedAt: null,
      lastHealthyAt: null,
      lastMismatchAt: null,
      lastMismatchType: null,
    };
  }

  return managedState.users[nextcloudUserId];
}

function normalizeManagedState(value) {
  const users = value && typeof value === 'object' && value.users && typeof value.users === 'object' ? value.users : {};
  return { users };
}

function normalizeCredentials(value) {
  return value && typeof value === 'object' ? value : {};
}

function countManagedUsers(credentials, managedState) {
  return new Set([...Object.keys(credentials), ...Object.keys(managedState.users || {})]).size;
}

function parseCommand(argv) {
  if (argv[0] === 'sync-once' || argv[0] === 'sync-loop' || argv[0] === 'deprovision') {
    return { command: argv[0], args: argv.slice(1) };
  }

  return { command: null, args: argv };
}

function parseDeprovisionArgs(args) {
  let user = null;
  let apply = false;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === '--user') {
      user = args[index + 1] || null;
      index += 1;
      continue;
    }

    if (arg === '--apply') {
      apply = true;
    }
  }

  if (!user) {
    throw new Error('deprovision requires --user <nextcloud-user-id>');
  }

  return { user, apply };
}

function toImmichEmail(nextcloudUserId) {
  if (nextcloudUserId.includes('@')) {
    return nextcloudUserId;
  }

  return `${nextcloudUserId}@${config.defaultEmailDomain}`;
}

function shouldDryRun() {
  return config.dryRun || !config.apiKey || config.apiKey === placeholderApiKey;
}

async function immichGet(pathname) {
  return request('GET', pathname);
}

async function immichPost(pathname, body) {
  return request('POST', pathname, body);
}

async function immichPut(pathname, body) {
  return request('PUT', pathname, body);
}

async function request(method, pathname, body) {
  const response = await fetch(`${config.apiUrl}${pathname}`, {
    method,
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      'x-api-key': config.apiKey,
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

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function parseBoolean(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function generatePassword(length) {
  return crypto.randomBytes(Math.max(length, 12)).toString('base64url').slice(0, length);
}

function env(name, fallback) {
  return process.env[name] ?? fallback;
}

function stripTrailingSlash(value) {
  return value.endsWith('/') ? value.slice(0, -1) : value;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asError(error) {
  return {
    message: error.message,
    stack: error.stack,
  };
}

function log(message) {
  console.log(`[bridge] ${new Date().toISOString()} ${message}`);
}

main().catch((error) => {
  log(`fatal: ${error.stack || error.message}`);
  process.exit(1);
});
