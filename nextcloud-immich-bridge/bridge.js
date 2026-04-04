const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const SOURCE_DEFINITIONS = {
  photos: {
    key: 'photos',
    folderName: 'Photos',
    label: 'Photos',
  },
  instantupload: {
    key: 'instantupload',
    folderName: 'InstantUpload',
    label: 'InstantUpload',
  },
};

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
  librarySources: parseLibrarySources(env('BRIDGE_LIBRARY_SOURCES', 'Photos,InstantUpload')),
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
      `cycle finished: users=${report.usersProcessed}, sources=${report.summary.eligibleLibrarySources}, createdLibraries=${report.librariesCreated.length}, mismatches=${report.mismatches.length}, errors=${report.errors.length}`,
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
    librarySources: config.librarySources.map((source) => source.folderName),
    disableCandidateThreshold: config.disableCandidateThreshold,
    summary: {
      nextcloudDirectories: 0,
      managedUsers: 0,
      eligibleUsers: 0,
      eligibleLibrarySources: 0,
      appdataSkipped: 0,
      missingLibrarySources: 0,
      dryRunCandidates: 0,
      usersCreated: 0,
      usersUpdated: 0,
      librariesCreated: 0,
      librariesUpdated: 0,
      librariesScanned: 0,
      mismatches: 0,
      libraryMismatches: 0,
      disabledCandidates: 0,
      skipped: 0,
      errors: 0,
    },
    usersDiscovered: 0,
    usersProcessed: 0,
    nextcloudDirectories: [],
    eligibleUsers: [],
    eligibleLibrarySources: [],
    appdataSkipped: [],
    missingLibrarySources: [],
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
  report.summary.libraryMismatches = report.mismatches.filter((mismatch) => Boolean(mismatch.sourceKey)).length;
  report.summary.disabledCandidates = report.disabledCandidates.length;
  report.summary.skipped = report.skipped.length;
  report.summary.errors = report.errors.length;
}

async function runSync(report) {
  const discovery = discoverUsers(report);
  const discoveredUsers = discovery.eligibleUsers;
  const directoryUserIds = new Set(discovery.directoryUserIds);
  const eligibleSourceKeysByUser = discovery.eligibleSourceKeysByUser;

  report.usersDiscovered = discoveredUsers.length;

  const credentials = normalizeCredentials(loadJson(credentialsPath, {}));
  const managedState = normalizeManagedState(loadJson(managedStatePath, { users: {} }));
  report.summary.managedUsers = countManagedUsers(credentials, managedState);

  if (shouldDryRun()) {
    for (const user of discoveredUsers) {
      report.usersProcessed += 1;
      for (const librarySource of user.librarySources) {
        report.dryRunCandidates.push({
          nextcloudUserId: user.nextcloudUserId,
          sourceKey: librarySource.sourceKey,
          folderName: librarySource.folderName,
          status: 'dry-run-candidate',
          displayName: user.displayName,
          libraryPath: librarySource.libraryPath,
          libraryName: librarySource.libraryName,
          immichEmail: user.immichEmail,
        });
      }
    }

    report.summary.dryRunCandidates = report.dryRunCandidates.length;
    updateMismatchLifecycle({
      report,
      credentials,
      managedState,
      directoryUserIds,
      eligibleSourceKeysByUser,
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
      });
      report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'manual_deprovision'));
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
      userId = updatedUser.id;
      report.usersUpdated.push({
        nextcloudUserId: discovered.nextcloudUserId,
        immichUserId: updatedUser.id,
        fields: ['name'],
      });
    }

    stateEntry.email = discovered.immichEmail;
    stateEntry.immichUserId = userId;
    stateEntry.lastSeenAt = now;
    stateEntry.lastSyncedAt = now;
    stateEntry.lastHealthyAt = now;
    stateEntry.lastMismatchAt = null;
    stateEntry.lastMismatchType = null;
    stateEntry.status = 'active';
    stateEntry.scanPolicy = 'enabled';

    for (const librarySource of discovered.librarySources) {
      const libraryState = ensureManagedLibraryStateEntry(stateEntry, librarySource.sourceKey);
      const existingLibrary = resolveExistingLibrary({
        libraries,
        libraryState,
        ownerId: userId,
        desiredImportPath: librarySource.libraryPath,
      });

      let libraryId = existingLibrary?.id || null;
      const desiredPayload = {
        name: librarySource.libraryName,
        importPaths: [librarySource.libraryPath],
        exclusionPatterns: [],
      };

      if (!existingLibrary) {
        report.mismatches.push({
          type: 'missing_library',
          nextcloudUserId: discovered.nextcloudUserId,
          sourceKey: librarySource.sourceKey,
          folderName: librarySource.folderName,
          immichEmail: discovered.immichEmail,
          libraryPath: librarySource.libraryPath,
          resolution: 'create-library',
        });

        const createdLibrary = await immichPost('/libraries', {
          ownerId: userId,
          ...desiredPayload,
        });
        libraryId = createdLibrary.id;
        libraries.push(createdLibrary);
        report.librariesCreated.push({
          nextcloudUserId: discovered.nextcloudUserId,
          sourceKey: librarySource.sourceKey,
          folderName: librarySource.folderName,
          libraryId: createdLibrary.id,
          libraryPath: librarySource.libraryPath,
          libraryName: librarySource.libraryName,
        });
      } else {
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
            sourceKey: librarySource.sourceKey,
            folderName: librarySource.folderName,
            libraryId: updatedLibrary.id,
            fields: ['name', 'importPaths', 'exclusionPatterns'],
          });
        }
      }

      libraryState.libraryId = libraryId || libraryState.libraryId || null;
      libraryState.libraryPath = librarySource.libraryPath;
      libraryState.libraryName = librarySource.libraryName;
      libraryState.folderName = librarySource.folderName;
      libraryState.lastSeenAt = now;
      libraryState.lastSyncedAt = now;
      libraryState.lastHealthyAt = now;
      libraryState.lastMismatchAt = null;
      libraryState.lastMismatchType = null;
      libraryState.consecutiveMissingCycles = 0;
      libraryState.disabledCandidateSince = null;
      libraryState.scanPolicy = 'enabled';
      libraryState.status = 'active';

      if (libraryState.libraryId) {
        await immichPost(`/libraries/${libraryState.libraryId}/scan`, {});
        report.librariesScanned.push({
          nextcloudUserId: discovered.nextcloudUserId,
          sourceKey: librarySource.sourceKey,
          folderName: librarySource.folderName,
          libraryId: libraryState.libraryId,
          libraryPath: libraryState.libraryPath,
        });
      }

      report.lifecycle.push(buildLibraryLifecycleRecord(discovered.nextcloudUserId, stateEntry, libraryState, 'synced'));
    }

    report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'synced'));
  }

  updateMismatchLifecycle({
    report,
    credentials,
    managedState,
    directoryUserIds,
    eligibleSourceKeysByUser,
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
  eligibleSourceKeysByUser,
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
      report.lifecycle.push(buildUserLifecycleRecord(nextcloudUserId, stateEntry, 'deprovisioned'));
      continue;
    }

    const eligibleKeys = eligibleSourceKeysByUser.get(nextcloudUserId) || new Set();
    const directoryPresent = directoryUserIds.has(nextcloudUserId);
    const immichUser = stateEntry.immichUserId ? users.find((candidate) => candidate.id === stateEntry.immichUserId) : null;

    for (const [sourceKey, libraryState] of Object.entries(stateEntry.libraries || {})) {
      if (eligibleKeys.has(sourceKey)) {
        continue;
      }

      const sourceDefinition = resolveSourceDefinition(sourceKey);
      const library = libraryState.libraryId ? libraries.find((candidate) => candidate.id === libraryState.libraryId) : null;
      const mismatchType = directoryPresent ? 'missing_library_source' : 'missing_nextcloud_user';
      registerManagedLibraryMismatch(report, stateEntry, libraryState, {
        nextcloudUserId,
        sourceKey,
        folderName: sourceDefinition.folderName,
        mismatchType,
        now,
        immichEmail: stateEntry.email || credentials[nextcloudUserId]?.email || null,
        immichUserId: stateEntry.immichUserId || null,
        libraryId: libraryState.libraryId || null,
        libraryPath: libraryState.libraryPath || null,
        directoryPresent,
        sourcePresent: false,
        immichUserMissing: !immichUser && Boolean(stateEntry.immichUserId),
        libraryMissing: !library && Boolean(libraryState.libraryId),
      });
    }

    refreshUserAggregateState(stateEntry);
    report.lifecycle.push(buildUserLifecycleRecord(nextcloudUserId, stateEntry, stateEntry.status));
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

function registerManagedLibraryMismatch(report, stateEntry, libraryState, details) {
  const mismatchChanged = libraryState.lastMismatchType !== details.mismatchType;
  libraryState.consecutiveMissingCycles = mismatchChanged ? 1 : libraryState.consecutiveMissingCycles + 1;
  libraryState.lastMismatchAt = details.now;
  libraryState.lastMismatchType = details.mismatchType;
  libraryState.status = 'mismatch';
  let lifecycleStatus = details.mismatchType;

  report.mismatches.push({
    type: details.mismatchType,
    nextcloudUserId: details.nextcloudUserId,
    sourceKey: details.sourceKey,
    folderName: details.folderName,
    immichEmail: details.immichEmail,
    immichUserId: details.immichUserId,
    libraryId: details.libraryId,
    libraryPath: details.libraryPath,
    consecutiveMissingCycles: libraryState.consecutiveMissingCycles,
    directoryPresent: details.directoryPresent,
    sourcePresent: details.sourcePresent,
    immichUserMissing: details.immichUserMissing,
    libraryMissing: details.libraryMissing,
  });

  if (libraryState.consecutiveMissingCycles >= config.disableCandidateThreshold) {
    libraryState.status = 'disabled_candidate';
    libraryState.scanPolicy = 'disabled';
    libraryState.disabledCandidateSince = libraryState.disabledCandidateSince || details.now;
    lifecycleStatus = 'disabled_candidate';
    report.disabledCandidates.push({
      type: 'disabled_candidate',
      nextcloudUserId: details.nextcloudUserId,
      sourceKey: details.sourceKey,
      folderName: details.folderName,
      mismatchType: details.mismatchType,
      immichEmail: details.immichEmail,
      libraryId: details.libraryId,
      libraryPath: details.libraryPath,
      consecutiveMissingCycles: libraryState.consecutiveMissingCycles,
      scanPolicy: libraryState.scanPolicy,
    });
  }

  stateEntry.lastMismatchAt = details.now;
  stateEntry.lastMismatchType = details.mismatchType;
  refreshUserAggregateState(stateEntry);
  report.lifecycle.push(buildLibraryLifecycleRecord(details.nextcloudUserId, stateEntry, libraryState, lifecycleStatus));
}

function discoverUsers(report) {
  const entries = fs.readdirSync(config.sourceRoot, { withFileTypes: true });
  const directoryUserIds = [];
  const eligibleUsers = [];
  const eligibleSourceKeysByUser = new Map();

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

    const discoveredSources = [];

    for (const sourceDefinition of config.librarySources) {
      const hostLibraryPath = path.join(config.sourceRoot, entry.name, 'files', sourceDefinition.folderName);
      const importPath = path.posix.join(config.libraryRoot, entry.name, 'files', sourceDefinition.folderName);
      const libraryName = buildLibraryName(sourceDefinition, entry.name);
      const exists = fs.existsSync(hostLibraryPath) && fs.statSync(hostLibraryPath).isDirectory();

      if (!exists) {
        report.missingLibrarySources.push({
          nextcloudUserId: entry.name,
          sourceKey: sourceDefinition.key,
          folderName: sourceDefinition.folderName,
          status: 'source-absent',
          expectedLibraryPath: importPath,
        });
        continue;
      }

      const librarySource = {
        sourceKey: sourceDefinition.key,
        folderName: sourceDefinition.folderName,
        libraryPath: importPath,
        libraryName,
        status: 'eligible',
      };
      discoveredSources.push(librarySource);
      report.eligibleLibrarySources.push({
        nextcloudUserId: entry.name,
        ...librarySource,
      });
    }

    if (discoveredSources.length === 0) {
      continue;
    }

    const mappedUser = {
      nextcloudUserId: entry.name,
      displayName: entry.name,
      immichEmail: toImmichEmail(entry.name),
      librarySources: discoveredSources,
      status: 'eligible',
    };
    eligibleUsers.push(mappedUser);
    report.eligibleUsers.push({
      nextcloudUserId: mappedUser.nextcloudUserId,
      displayName: mappedUser.displayName,
      immichEmail: mappedUser.immichEmail,
      sourceKeys: discoveredSources.map((source) => source.sourceKey),
      status: mappedUser.status,
    });
    eligibleSourceKeysByUser.set(entry.name, new Set(discoveredSources.map((source) => source.sourceKey)));
  }

  report.summary.nextcloudDirectories = directoryUserIds.length;
  report.summary.eligibleUsers = report.eligibleUsers.length;
  report.summary.eligibleLibrarySources = report.eligibleLibrarySources.length;
  report.summary.appdataSkipped = report.appdataSkipped.length;
  report.summary.missingLibrarySources = report.missingLibrarySources.length;

  return {
    eligibleUsers: eligibleUsers.sort((left, right) => left.nextcloudUserId.localeCompare(right.nextcloudUserId)),
    directoryUserIds: directoryUserIds.sort((left, right) => left.localeCompare(right)),
    eligibleSourceKeysByUser,
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
          'mark-user-state-as-deprovisioned',
          'disable-future-bridge-rescans-for-all-managed-libraries',
          'keep-existing-immich-user-and-libraries-intact',
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
    for (const libraryState of Object.values(stateEntry.libraries || {})) {
      libraryState.status = 'deprovisioned';
      libraryState.scanPolicy = 'disabled';
      libraryState.deprovisionedAt = now;
      libraryState.lastMismatchAt = now;
      libraryState.lastMismatchType = 'manual_deprovision';
    }
    fs.writeFileSync(managedStatePath, JSON.stringify(managedState, null, 2));
  }

  result.finishedAt = new Date().toISOString();
  result.after = options.apply ? managedState.users[options.user] : stateEntry;
  fs.writeFileSync(deprovisionReportPath, JSON.stringify(result, null, 2));
  log(
    `${options.apply ? 'applied' : 'previewed'} deprovision for ${options.user}; audit written to ${deprovisionReportPath}`,
  );
}

function buildUserLifecycleRecord(nextcloudUserId, stateEntry, status) {
  return {
    recordType: 'user',
    nextcloudUserId,
    status,
    immichEmail: stateEntry.email || null,
    immichUserId: stateEntry.immichUserId || null,
    scanPolicy: stateEntry.scanPolicy || 'enabled',
    managedLibraryCount: Object.keys(stateEntry.libraries || {}).length,
    activeLibraryCount: Object.values(stateEntry.libraries || {}).filter((library) => library.status === 'active').length,
    consecutiveMissingCycles: stateEntry.consecutiveMissingCycles || 0,
    disabledCandidateSince: stateEntry.disabledCandidateSince || null,
    deprovisionedAt: stateEntry.deprovisionedAt || null,
  };
}

function buildLibraryLifecycleRecord(nextcloudUserId, stateEntry, libraryState, status) {
  return {
    recordType: 'library',
    nextcloudUserId,
    sourceKey: libraryState.sourceKey,
    folderName: libraryState.folderName || resolveSourceDefinition(libraryState.sourceKey).folderName,
    status,
    immichEmail: stateEntry.email || null,
    immichUserId: stateEntry.immichUserId || null,
    libraryId: libraryState.libraryId || null,
    libraryPath: libraryState.libraryPath || null,
    libraryName: libraryState.libraryName || null,
    scanPolicy: libraryState.scanPolicy || 'enabled',
    consecutiveMissingCycles: libraryState.consecutiveMissingCycles || 0,
    disabledCandidateSince: libraryState.disabledCandidateSince || null,
    deprovisionedAt: libraryState.deprovisionedAt || null,
  };
}

function ensureManagedStateEntry(managedState, nextcloudUserId) {
  if (!managedState.users[nextcloudUserId]) {
    managedState.users[nextcloudUserId] = createEmptyManagedStateEntry();
  }

  const stateEntry = managedState.users[nextcloudUserId];
  stateEntry.email = stateEntry.email || null;
  stateEntry.immichUserId = stateEntry.immichUserId || null;
  stateEntry.status = stateEntry.status || 'new';
  stateEntry.scanPolicy = stateEntry.scanPolicy || 'enabled';
  stateEntry.consecutiveMissingCycles = stateEntry.consecutiveMissingCycles || 0;
  stateEntry.disabledCandidateSince = stateEntry.disabledCandidateSince || null;
  stateEntry.deprovisionedAt = stateEntry.deprovisionedAt || null;
  stateEntry.lastSeenAt = stateEntry.lastSeenAt || null;
  stateEntry.lastSyncedAt = stateEntry.lastSyncedAt || null;
  stateEntry.lastHealthyAt = stateEntry.lastHealthyAt || null;
  stateEntry.lastMismatchAt = stateEntry.lastMismatchAt || null;
  stateEntry.lastMismatchType = stateEntry.lastMismatchType || null;
  stateEntry.libraries = normalizeLibrariesMap(stateEntry);
  refreshUserAggregateState(stateEntry);
  return stateEntry;
}

function createEmptyManagedStateEntry() {
  return {
    email: null,
    immichUserId: null,
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
    libraries: {},
  };
}

function normalizeLibrariesMap(stateEntry) {
  const libraries = stateEntry.libraries && typeof stateEntry.libraries === 'object' ? stateEntry.libraries : {};

  if (!Object.keys(libraries).length && (stateEntry.libraryId || stateEntry.libraryPath || stateEntry.libraryName)) {
    libraries.photos = {
      sourceKey: 'photos',
      folderName: 'Photos',
      libraryId: stateEntry.libraryId || null,
      libraryPath: stateEntry.libraryPath || null,
      libraryName: stateEntry.libraryName || buildLibraryName(resolveSourceDefinition('photos'), extractUserIdFromPath(stateEntry.libraryPath)),
      status: stateEntry.status || 'active',
      scanPolicy: stateEntry.scanPolicy || 'enabled',
      consecutiveMissingCycles: stateEntry.consecutiveMissingCycles || 0,
      disabledCandidateSince: stateEntry.disabledCandidateSince || null,
      deprovisionedAt: stateEntry.deprovisionedAt || null,
      lastSeenAt: stateEntry.lastSeenAt || null,
      lastSyncedAt: stateEntry.lastSyncedAt || null,
      lastHealthyAt: stateEntry.lastHealthyAt || null,
      lastMismatchAt: stateEntry.lastMismatchAt || null,
      lastMismatchType: stateEntry.lastMismatchType || null,
    };
  }

  const normalized = {};
  for (const [sourceKey, candidate] of Object.entries(libraries)) {
    normalized[sourceKey] = {
      sourceKey,
      folderName: candidate.folderName || resolveSourceDefinition(sourceKey).folderName,
      libraryId: candidate.libraryId || null,
      libraryPath: candidate.libraryPath || null,
      libraryName: candidate.libraryName || null,
      status: candidate.status || 'new',
      scanPolicy: candidate.scanPolicy || 'enabled',
      consecutiveMissingCycles: candidate.consecutiveMissingCycles || 0,
      disabledCandidateSince: candidate.disabledCandidateSince || null,
      deprovisionedAt: candidate.deprovisionedAt || null,
      lastSeenAt: candidate.lastSeenAt || null,
      lastSyncedAt: candidate.lastSyncedAt || null,
      lastHealthyAt: candidate.lastHealthyAt || null,
      lastMismatchAt: candidate.lastMismatchAt || null,
      lastMismatchType: candidate.lastMismatchType || null,
    };
  }

  delete stateEntry.libraryId;
  delete stateEntry.libraryPath;
  delete stateEntry.libraryName;
  return normalized;
}

function ensureManagedLibraryStateEntry(stateEntry, sourceKey) {
  if (!stateEntry.libraries[sourceKey]) {
    stateEntry.libraries[sourceKey] = {
      sourceKey,
      folderName: resolveSourceDefinition(sourceKey).folderName,
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

  return stateEntry.libraries[sourceKey];
}

function refreshUserAggregateState(stateEntry) {
  const libraries = Object.values(stateEntry.libraries || {});
  if (!libraries.length) {
    stateEntry.status = stateEntry.status || 'new';
    stateEntry.scanPolicy = stateEntry.scanPolicy || 'enabled';
    stateEntry.consecutiveMissingCycles = 0;
    stateEntry.disabledCandidateSince = null;
    return;
  }

  if (stateEntry.status === 'deprovisioned') {
    stateEntry.scanPolicy = 'disabled';
    return;
  }

  const activeLibraries = libraries.filter((library) => library.status === 'active');
  const disabledCandidates = libraries.filter((library) => library.status === 'disabled_candidate');
  const mismatchLibraries = libraries.filter((library) => library.status === 'mismatch');
  const maxMissingCycles = Math.max(...libraries.map((library) => library.consecutiveMissingCycles || 0), 0);
  const disabledSince = libraries
    .map((library) => library.disabledCandidateSince)
    .filter(Boolean)
    .sort()[0] || null;

  if (activeLibraries.length > 0) {
    stateEntry.status = 'active';
    stateEntry.scanPolicy = 'enabled';
  } else if (disabledCandidates.length > 0) {
    stateEntry.status = 'disabled_candidate';
    stateEntry.scanPolicy = 'disabled';
  } else if (mismatchLibraries.length > 0) {
    stateEntry.status = 'mismatch';
    stateEntry.scanPolicy = 'enabled';
  } else {
    stateEntry.status = 'new';
    stateEntry.scanPolicy = 'enabled';
  }

  stateEntry.consecutiveMissingCycles = maxMissingCycles;
  stateEntry.disabledCandidateSince = disabledSince;
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

function resolveExistingLibrary({ libraries, libraryState, ownerId, desiredImportPath }) {
  const stateLibrary = libraryState.libraryId ? libraries.find((library) => library.id === libraryState.libraryId) : null;
  if (stateLibrary) {
    return stateLibrary;
  }

  return libraries.find((library) => {
    const sameOwner = library.ownerId === ownerId;
    const importPaths = Array.isArray(library.importPaths) ? library.importPaths : [];
    return sameOwner && importPaths.includes(desiredImportPath);
  });
}

function buildLibraryName(sourceDefinition, nextcloudUserId) {
  if (sourceDefinition.key === 'photos' && config.libraryNamePrefix) {
    return `${config.libraryNamePrefix} ${nextcloudUserId}`.replace(/\s+/g, ' ').trim();
  }

  return `Nextcloud ${sourceDefinition.label} - ${nextcloudUserId}`;
}

function parseLibrarySources(rawValue) {
  const requested = String(rawValue || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const normalized = [];

  for (const value of requested) {
    const key = normalizeSourceKey(value);
    if (!SOURCE_DEFINITIONS[key]) {
      continue;
    }
    normalized.push(SOURCE_DEFINITIONS[key]);
  }

  if (normalized.length === 0) {
    return [SOURCE_DEFINITIONS.photos];
  }

  return normalized.filter((source, index, array) => array.findIndex((candidate) => candidate.key === source.key) === index);
}

function normalizeSourceKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function resolveSourceDefinition(sourceKey) {
  return SOURCE_DEFINITIONS[normalizeSourceKey(sourceKey)] || {
    key: normalizeSourceKey(sourceKey),
    folderName: sourceKey,
    label: sourceKey,
  };
}

function extractUserIdFromPath(libraryPath) {
  if (!libraryPath) {
    return 'unknown-user';
  }

  const parts = String(libraryPath).split('/');
  return parts[3] || 'unknown-user';
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
