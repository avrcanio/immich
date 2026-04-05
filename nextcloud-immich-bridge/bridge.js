const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');

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
  intervalSeconds: parseInteger(env('BRIDGE_INTERVAL_SECONDS', '300'), 300),
  runOnce: parseBoolean(env('BRIDGE_RUN_ONCE', 'false')),
  dryRun: parseBoolean(env('BRIDGE_DRY_RUN', 'true')),
  defaultEmailDomain: env('BRIDGE_DEFAULT_EMAIL_DOMAIN', 'local.invalid'),
  passwordLength: parseInteger(env('BRIDGE_PASSWORD_LENGTH', '24'), 24),
  libraryNamePrefix: env('BRIDGE_LIBRARY_NAME_PREFIX', 'Nextcloud Photos -'),
  librarySources: parseLibrarySources(env('BRIDGE_LIBRARY_SOURCES', 'Photos,InstantUpload')),
  disableCandidateThreshold: parseInteger(env('BRIDGE_DISABLE_CANDIDATE_THRESHOLD', '3'), 3),
  nextcloudContainerName: env('BRIDGE_NEXTCLOUD_CONTAINER_NAME', 'nextcloud'),
};

const credentialsPath = path.join(config.stateDir, 'credentials.json');
const reportPath = path.join(config.stateDir, 'last-run.json');
const managedStatePath = path.join(config.stateDir, 'managed-state.json');
const deprovisionReportPath = path.join(config.stateDir, 'last-deprovision.json');
const passwordIssueReportPath = path.join(config.stateDir, 'last-password-issue.json');
const placeholderApiKey = 'REPLACE_WITH_IMMICH_ADMIN_API_KEY';

fs.mkdirSync(config.stateDir, { recursive: true });

async function main() {
  const { command, args } = parseCommand(process.argv.slice(2));

  if (command === 'deprovision') {
    await runDeprovision(args);
    return;
  }

  if (command === 'issue-password') {
    await runIssuePassword(args);
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
      report.dryRunCandidates.push({
        nextcloudUserId: user.nextcloudUserId,
        immichEmail: user.immichEmail,
        displayName: user.displayName,
        sourceKeys: user.librarySources.map((source) => source.sourceKey),
      });
    }

    report.summary.dryRunCandidates = report.dryRunCandidates.length;
    return;
  }

  const users = await immichGet('/admin/users');
  const libraries = await immichGet('/libraries');
  const now = new Date().toISOString();
  const usersById = new Map(users.map((user) => [user.id, user]));
  const usersByEmail = new Map(
    users
      .filter((user) => normalizeEmail(user.email))
      .map((user) => [normalizeEmail(user.email), user]),
  );

  for (const discovered of discoveredUsers) {
    report.usersProcessed += 1;

    const identityConflict = findStateConflictByNextcloudUserId(managedState, discovered.nextcloudUserId, discovered.immichEmail);
    if (identityConflict) {
      const stateEntry = identityConflict.entry;
      stateEntry.nextcloudUserId = discovered.nextcloudUserId;
      stateEntry.status = 'conflict';
      stateEntry.scanPolicy = 'disabled';
      stateEntry.lastSyncedAt = now;
      stateEntry.lastMismatchAt = now;
      stateEntry.lastMismatchType = 'identity_conflict';
      report.mismatches.push({
        type: 'identity_conflict',
        nextcloudUserId: discovered.nextcloudUserId,
        previousEmail: identityConflict.email,
        nextcloudEmail: discovered.immichEmail,
      });
      report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'conflict'));
      log(`email conflict for ${discovered.nextcloudUserId}: existing=${identityConflict.email} nextcloud=${discovered.immichEmail}`);
      continue;
    }

    const stateEntry = ensureManagedStateEntry(managedState, discovered.immichEmail);
    stateEntry.email = discovered.immichEmail;
    stateEntry.nextcloudUserId = discovered.nextcloudUserId;
    stateEntry.lastSeenAt = now;
    stateEntry.lastSyncedAt = now;

    if (stateEntry.status === 'deprovisioned') {
      report.skipped.push({
        type: 'manual_deprovision',
        nextcloudUserId: discovered.nextcloudUserId,
        immichEmail: discovered.immichEmail,
      });
      report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'manual_deprovision'));
      continue;
    }

    if (!discovered.enabled) {
      stateEntry.status = 'disabled';
      stateEntry.scanPolicy = 'disabled';
      stateEntry.lastMismatchAt = now;
      stateEntry.lastMismatchType = 'nextcloud_disabled';
      report.skipped.push({
        type: 'disabled_nextcloud_user',
        nextcloudUserId: discovered.nextcloudUserId,
        immichEmail: discovered.immichEmail,
      });
      report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'disabled'));
      log(`skipped disabled user ${discovered.nextcloudUserId}`);
      continue;
    }

    if (stateEntry.status === 'conflict') {
      report.skipped.push({
        type: 'conflict_user',
        nextcloudUserId: discovered.nextcloudUserId,
        immichEmail: discovered.immichEmail,
      });
      report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'conflict'));
      continue;
    }

    let immichUser = null;
    let lifecycleStatus = stateEntry.status;

    if (!stateEntry.immichUserId) {
      immichUser = usersByEmail.get(discovered.immichEmail) || null;

      if (immichUser) {
        if (immichUser.name !== discovered.displayName) {
          immichUser = await updateImmichUserName({ users, usersById, usersByEmail, immichUser, displayName: discovered.displayName });
          report.usersUpdated.push({
            nextcloudUserId: discovered.nextcloudUserId,
            immichUserId: immichUser.id,
            fields: ['name'],
          });
        }

        stateEntry.immichUserId = immichUser.id;
        stateEntry.status = 'linked';
        stateEntry.scanPolicy = 'enabled';
        stateEntry.lastHealthyAt = now;
        stateEntry.lastMismatchAt = null;
        stateEntry.lastMismatchType = null;
        lifecycleStatus = 'linked';
        report.usersUpdated.push({
          nextcloudUserId: discovered.nextcloudUserId,
          immichUserId: immichUser.id,
          fields: ['link'],
        });
        log(`linked user ${discovered.nextcloudUserId} -> ${discovered.immichEmail}`);
      } else {
        const generatedPassword = generatePassword(config.passwordLength);
        const createdUser = await immichPost('/admin/users', {
          email: discovered.immichEmail,
          password: generatedPassword,
          name: discovered.displayName,
          shouldChangePassword: true,
          notify: false,
          isAdmin: false,
        });

        credentials[discovered.immichEmail] = {
          email: discovered.immichEmail,
          nextcloudUserId: discovered.nextcloudUserId,
          password: generatedPassword,
          createdAt: now,
        };

        users.push(createdUser);
        usersById.set(createdUser.id, createdUser);
        usersByEmail.set(discovered.immichEmail, createdUser);
        stateEntry.immichUserId = createdUser.id;
        stateEntry.status = 'created';
        stateEntry.scanPolicy = 'enabled';
        stateEntry.lastHealthyAt = now;
        stateEntry.lastMismatchAt = null;
        stateEntry.lastMismatchType = null;
        lifecycleStatus = 'created';
        report.usersCreated.push({
          nextcloudUserId: discovered.nextcloudUserId,
          immichUserId: createdUser.id,
          immichEmail: discovered.immichEmail,
        });
        log(`created user ${discovered.nextcloudUserId} -> ${discovered.immichEmail}`);
      }
    } else {
      immichUser = usersById.get(stateEntry.immichUserId) || null;
      if (!immichUser) {
        stateEntry.status = 'error_missing_user';
        stateEntry.scanPolicy = 'disabled';
        stateEntry.lastMismatchAt = now;
        stateEntry.lastMismatchType = 'missing_immich_user';
        report.mismatches.push({
          type: 'missing_immich_user',
          nextcloudUserId: discovered.nextcloudUserId,
          immichEmail: discovered.immichEmail,
          immichUserId: stateEntry.immichUserId,
        });
        report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'error_missing_user'));
        log(`missing Immich user for ${discovered.nextcloudUserId}: ${stateEntry.immichUserId}`);
        continue;
      }

      const immichEmail = normalizeEmail(immichUser.email);
      if (immichEmail !== discovered.immichEmail) {
        stateEntry.status = 'conflict';
        stateEntry.scanPolicy = 'disabled';
        stateEntry.lastMismatchAt = now;
        stateEntry.lastMismatchType = 'identity_conflict';
        report.mismatches.push({
          type: 'identity_conflict',
          nextcloudUserId: discovered.nextcloudUserId,
          immichEmail: immichUser.email,
          nextcloudEmail: discovered.immichEmail,
          immichUserId: immichUser.id,
        });
        report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, 'conflict'));
        log(`email mismatch detected for ${discovered.nextcloudUserId}: immich=${immichUser.email} nextcloud=${discovered.immichEmail}`);
        continue;
      }

      if (immichUser.name !== discovered.displayName) {
        immichUser = await updateImmichUserName({ users, usersById, usersByEmail, immichUser, displayName: discovered.displayName });
        report.usersUpdated.push({
          nextcloudUserId: discovered.nextcloudUserId,
          immichUserId: immichUser.id,
          fields: ['name'],
        });
      }

      stateEntry.status = stateEntry.status === 'created' ? 'created' : 'linked';
      stateEntry.scanPolicy = 'enabled';
      stateEntry.lastHealthyAt = now;
      stateEntry.lastMismatchAt = null;
      stateEntry.lastMismatchType = null;
      lifecycleStatus = stateEntry.status;
    }

    const credentialEntry = credentials[discovered.immichEmail];
    if (credentialEntry) {
      credentialEntry.email = discovered.immichEmail;
      credentialEntry.nextcloudUserId = discovered.nextcloudUserId;
      credentialEntry.createdAt = credentialEntry.createdAt || now;
    }

    await syncLibrariesForUser({
      report,
      discovered,
      stateEntry,
      libraries,
      now,
    });

    report.lifecycle.push(buildUserLifecycleRecord(discovered.nextcloudUserId, stateEntry, lifecycleStatus));
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

async function syncLibrariesForUser({ report, discovered, stateEntry, libraries, now }) {
  const availableSources = new Set(discovered.librarySources.map((source) => source.sourceKey));

  for (const sourceDefinition of config.librarySources) {
    const libraryState = ensureManagedLibraryStateEntry(stateEntry, sourceDefinition.key);
    const librarySource = discovered.librarySources.find((source) => source.sourceKey === sourceDefinition.key) || null;

    if (!librarySource) {
      libraryState.folderName = sourceDefinition.folderName;
      libraryState.libraryName = libraryState.libraryName || buildLibraryName(sourceDefinition, discovered.nextcloudUserId);
      libraryState.libraryPath = path.posix.join(config.libraryRoot, discovered.nextcloudUserId, 'files', sourceDefinition.folderName);
      libraryState.status = libraryState.libraryId ? 'pending' : 'pending';
      libraryState.scanPolicy = 'disabled';
      libraryState.lastSeenAt = now;
      libraryState.lastSyncedAt = now;
      libraryState.lastMismatchAt = null;
      libraryState.lastMismatchType = null;
      report.lifecycle.push(buildLibraryLifecycleRecord(discovered.nextcloudUserId, stateEntry, libraryState, 'pending'));
      continue;
    }

    const existingLibrary = resolveExistingLibrary({
      libraries,
      libraryState,
      ownerId: stateEntry.immichUserId,
      desiredImportPath: librarySource.libraryPath,
    });
    const desiredPayload = {
      name: librarySource.libraryName,
      importPaths: [librarySource.libraryPath],
      exclusionPatterns: [],
    };

    let libraryId = existingLibrary?.id || null;
    let lifecycleStatus = 'linked';

    if (!existingLibrary) {
      const createdLibrary = await immichPost('/libraries', {
        ownerId: stateEntry.immichUserId,
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
      log(`created library ${librarySource.sourceKey} for ${discovered.nextcloudUserId}`);
      lifecycleStatus = 'created';
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
        lifecycleStatus = 'updated';
      }
    }

    libraryState.libraryId = libraryId;
    libraryState.libraryPath = librarySource.libraryPath;
    libraryState.libraryName = librarySource.libraryName;
    libraryState.folderName = librarySource.folderName;
    libraryState.status = lifecycleStatus === 'created' ? 'created' : 'active';
    libraryState.scanPolicy = 'enabled';
    libraryState.lastSeenAt = now;
    libraryState.lastSyncedAt = now;
    libraryState.lastHealthyAt = now;
    libraryState.lastMismatchAt = null;
    libraryState.lastMismatchType = null;
    libraryState.consecutiveMissingCycles = 0;
    libraryState.disabledCandidateSince = null;

    await immichPost(`/libraries/${libraryState.libraryId}/scan`, {});
    report.librariesScanned.push({
      nextcloudUserId: discovered.nextcloudUserId,
      sourceKey: librarySource.sourceKey,
      folderName: librarySource.folderName,
      libraryId: libraryState.libraryId,
      libraryPath: libraryState.libraryPath,
    });
    report.lifecycle.push(buildLibraryLifecycleRecord(discovered.nextcloudUserId, stateEntry, libraryState, lifecycleStatus));
  }

  for (const [sourceKey, libraryState] of Object.entries(stateEntry.libraries || {})) {
    if (availableSources.has(sourceKey)) {
      continue;
    }
    libraryState.status = libraryState.libraryId ? 'pending' : libraryState.status || 'pending';
    libraryState.scanPolicy = 'disabled';
    libraryState.lastSeenAt = now;
    libraryState.lastSyncedAt = now;
  }
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
  const managedEmails = new Set([
    ...Object.keys(credentials),
    ...Object.keys(managedState.users || {}),
  ]);

  for (const email of managedEmails) {
    const stateEntry = ensureManagedStateEntry(managedState, email);
    const nextcloudUserId = stateEntry.nextcloudUserId || email;

    if (stateEntry.status === 'deprovisioned') {
      report.lifecycle.push(buildUserLifecycleRecord(nextcloudUserId, stateEntry, 'deprovisioned'));
      continue;
    }

    if (stateEntry.status === 'disabled') {
      report.lifecycle.push(buildUserLifecycleRecord(nextcloudUserId, stateEntry, 'disabled'));
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

      if (!libraryState.libraryId) {
        continue;
      }

      registerManagedLibraryMismatch(report, stateEntry, libraryState, {
        nextcloudUserId,
        sourceKey,
        folderName: sourceDefinition.folderName,
        mismatchType: directoryPresent ? 'missing_library_source' : 'missing_nextcloud_user',
        now,
        immichEmail: stateEntry.email || credentials[email]?.email || null,
        immichUserId: stateEntry.immichUserId || null,
        libraryId: libraryState.libraryId || null,
        libraryPath: libraryState.libraryPath || null,
        directoryPresent,
        sourcePresent: false,
        immichUserMissing: !immichUser && Boolean(stateEntry.immichUserId),
        libraryMissing: !library && Boolean(libraryState.libraryId),
      });
    }

    report.lifecycle.push(buildUserLifecycleRecord(nextcloudUserId, stateEntry, stateEntry.status));
  }

  const expectedEmails = new Set(
    Array.from(managedEmails)
      .map((email) => normalizeEmail(credentials[email]?.email || managedState.users?.[email]?.email || email))
      .filter(Boolean),
  );

  for (const user of users) {
    const email = normalizeEmail(user.email);
    if (!expectedEmails.has(email) && !user.isAdmin) {
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
  report.lifecycle.push(buildLibraryLifecycleRecord(details.nextcloudUserId, stateEntry, libraryState, lifecycleStatus));
}

function discoverUsers(report) {
  const nextcloudUsers = loadNextcloudUsers();
  const directoryUserIds = [];
  const eligibleUsers = [];
  const eligibleSourceKeysByUser = new Map();

  for (const user of nextcloudUsers) {
    directoryUserIds.push(user.nextcloudUserId);
    report.nextcloudDirectories.push({
      nextcloudUserId: user.nextcloudUserId,
      status: user.enabled ? 'user-discovered' : 'user-disabled',
    });

    const discoveredSources = discoverLibrarySources(user.nextcloudUserId, report);
    eligibleSourceKeysByUser.set(user.nextcloudUserId, new Set(discoveredSources.map((source) => source.sourceKey)));

    if (!user.immichEmail) {
      report.errors.push({
        message: `Nextcloud user ${user.nextcloudUserId} is missing an email address`,
      });
      continue;
    }

    const mappedUser = {
      nextcloudUserId: user.nextcloudUserId,
      displayName: user.displayName,
      immichEmail: user.immichEmail,
      enabled: user.enabled,
      librarySources: discoveredSources,
      status: user.enabled ? 'eligible' : 'disabled',
    };
    eligibleUsers.push(mappedUser);
    report.eligibleUsers.push({
      nextcloudUserId: mappedUser.nextcloudUserId,
      displayName: mappedUser.displayName,
      immichEmail: mappedUser.immichEmail,
      enabled: mappedUser.enabled,
      sourceKeys: discoveredSources.map((source) => source.sourceKey),
      status: mappedUser.status,
    });
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

function discoverLibrarySources(nextcloudUserId, report) {
  const discoveredSources = [];

  for (const sourceDefinition of config.librarySources) {
    const hostLibraryPath = path.join(config.sourceRoot, nextcloudUserId, 'files', sourceDefinition.folderName);
    const importPath = path.posix.join(config.libraryRoot, nextcloudUserId, 'files', sourceDefinition.folderName);
    const libraryName = buildLibraryName(sourceDefinition, nextcloudUserId);
    const exists = fs.existsSync(hostLibraryPath) && fs.statSync(hostLibraryPath).isDirectory();

    if (!exists) {
      report.missingLibrarySources.push({
        nextcloudUserId,
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
      nextcloudUserId,
      ...librarySource,
    });
  }

  return discoveredSources;
}

function loadNextcloudUsers() {
  const list = runNextcloudOcc(['user:list', '--output=json', '--no-warnings']);
  const parsedList = safeJsonParse(list);
  const userIds = Object.keys(parsedList || {}).sort((left, right) => left.localeCompare(right));
  const users = [];

  for (const nextcloudUserId of userIds) {
    const info = safeJsonParse(runNextcloudOcc(['user:info', nextcloudUserId, '--output=json', '--no-warnings']));
    const email = normalizeEmail(info?.email);
    users.push({
      nextcloudUserId,
      displayName: info?.display_name || parsedList[nextcloudUserId] || nextcloudUserId,
      immichEmail: email,
      enabled: Boolean(info?.enabled),
    });
  }

  return users;
}

function runNextcloudOcc(args) {
  return execFileSync(
    'docker',
    ['exec', config.nextcloudContainerName, 'php', 'occ', ...args],
    {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  ).trim();
}

async function runDeprovision(args) {
  const options = parseDeprovisionArgs(args);
  const credentials = normalizeCredentials(loadJson(credentialsPath, {}));
  const managedState = normalizeManagedState(loadJson(managedStatePath, { users: {} }));
  const userKey = resolveManagedUserKey(managedState, options.user);

  if (!userKey) {
    throw new Error(`Unknown managed user: ${options.user}`);
  }

  const stateEntry = ensureManagedStateEntry(managedState, userKey);
  const credentialEntry = credentials[userKey] || null;
  const now = new Date().toISOString();
  const beforeState = JSON.parse(JSON.stringify(stateEntry));
  const beforeCredentials = credentialEntry ? JSON.parse(JSON.stringify(credentialEntry)) : null;

  const result = {
    startedAt: now,
    finishedAt: null,
    mode: 'deprovision',
    apply: options.apply,
    nextcloudUserId: stateEntry.nextcloudUserId || options.user,
    immichEmail: stateEntry.email,
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
  result.after = options.apply ? managedState.users[userKey] : stateEntry;
  fs.writeFileSync(deprovisionReportPath, JSON.stringify(result, null, 2));
  log(
    `${options.apply ? 'applied' : 'previewed'} deprovision for ${stateEntry.email || options.user}; audit written to ${deprovisionReportPath}`,
  );
}

function buildUserLifecycleRecord(nextcloudUserId, stateEntry, status) {
  return {
    recordType: 'user',
    nextcloudUserId: nextcloudUserId || stateEntry.nextcloudUserId || null,
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
    nextcloudUserId: nextcloudUserId || stateEntry.nextcloudUserId || null,
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

function ensureManagedStateEntry(managedState, email) {
  const key = normalizeEmail(email);
  if (!managedState.users[key]) {
    managedState.users[key] = createEmptyManagedStateEntry();
  }

  const stateEntry = managedState.users[key];
  stateEntry.email = normalizeEmail(stateEntry.email || key);
  stateEntry.nextcloudUserId = stateEntry.nextcloudUserId || null;
  stateEntry.immichUserId = stateEntry.immichUserId || null;
  stateEntry.status = stateEntry.status || 'linked';
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
  return stateEntry;
}

function createEmptyManagedStateEntry() {
  return {
    email: null,
    nextcloudUserId: null,
    immichUserId: null,
    status: 'linked',
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
      libraryName: stateEntry.libraryName || buildLibraryName(resolveSourceDefinition('photos'), stateEntry.nextcloudUserId || extractUserIdFromPath(stateEntry.libraryPath)),
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
      status: candidate.status || 'pending',
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
      status: 'pending',
      scanPolicy: 'disabled',
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

function normalizeManagedState(value) {
  const rawUsers = value && typeof value === 'object' && value.users && typeof value.users === 'object' ? value.users : {};
  const users = {};

  for (const [rawKey, candidate] of Object.entries(rawUsers)) {
    if (!candidate || typeof candidate !== 'object') {
      continue;
    }

    const email = normalizeEmail(candidate.email || (rawKey.includes('@') ? rawKey : null));
    if (!email) {
      continue;
    }

    const existing = users[email] || createEmptyManagedStateEntry();
    users[email] = {
      ...existing,
      ...candidate,
      email,
      nextcloudUserId: candidate.nextcloudUserId || inferLegacyNextcloudUserId(rawKey, candidate),
      libraries: normalizeLibrariesMap({
        ...existing,
        ...candidate,
      }),
    };
  }

  return { users };
}

function normalizeCredentials(value) {
  const credentials = {};
  const raw = value && typeof value === 'object' ? value : {};

  for (const [rawKey, candidate] of Object.entries(raw)) {
    if (!candidate || typeof candidate !== 'object') {
      continue;
    }

    const email = normalizeEmail(candidate.email || (rawKey.includes('@') ? rawKey : null));
    if (!email || !candidate.password) {
      continue;
    }

    credentials[email] = {
      email,
      nextcloudUserId: candidate.nextcloudUserId || inferLegacyNextcloudUserId(rawKey, candidate),
      password: candidate.password,
      createdAt: candidate.createdAt || null,
    };
  }

  return credentials;
}

function countManagedUsers(credentials, managedState) {
  return new Set([...Object.keys(credentials), ...Object.keys(managedState.users || {})]).size;
}

function resolveManagedUserKey(managedState, user) {
  const email = normalizeEmail(user);
  if (email && managedState.users[email]) {
    return email;
  }

  return Object.keys(managedState.users || {}).find((candidate) => managedState.users[candidate]?.nextcloudUserId === user) || null;
}

function findStateConflictByNextcloudUserId(managedState, nextcloudUserId, expectedEmail) {
  for (const [email, entry] of Object.entries(managedState.users || {})) {
    if (entry?.nextcloudUserId === nextcloudUserId && email !== expectedEmail) {
      return { email, entry };
    }
  }

  return null;
}

function inferLegacyNextcloudUserId(rawKey, candidate) {
  if (candidate.nextcloudUserId) {
    return candidate.nextcloudUserId;
  }

  if (candidate.userId) {
    return candidate.userId;
  }

  if (candidate.email && normalizeEmail(candidate.email) !== normalizeEmail(rawKey)) {
    return rawKey;
  }

  return rawKey;
}

function parseCommand(argv) {
  if (argv[0] === 'sync-once' || argv[0] === 'sync-loop' || argv[0] === 'deprovision' || argv[0] === 'issue-password') {
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
    throw new Error('deprovision requires --user <email-or-nextcloud-user-id>');
  }

  return { user, apply };
}

function parseIssuePasswordArgs(args) {
  let user = null;
  let apply = false;
  let password = null;
  let shouldChangePassword = true;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === '--user') {
      user = args[index + 1] || null;
      index += 1;
      continue;
    }

    if (arg === '--password') {
      password = args[index + 1] || null;
      index += 1;
      continue;
    }

    if (arg === '--apply') {
      apply = true;
      continue;
    }

    if (arg === '--no-change-password') {
      shouldChangePassword = false;
    }
  }

  if (!user) {
    throw new Error('issue-password requires --user <email-or-nextcloud-user-id>');
  }

  return { user, apply, password, shouldChangePassword };
}

async function runIssuePassword(args) {
  const options = parseIssuePasswordArgs(args);
  const credentials = normalizeCredentials(loadJson(credentialsPath, {}));
  const managedState = normalizeManagedState(loadJson(managedStatePath, { users: {} }));
  const userKey = resolveManagedUserKey(managedState, options.user);

  if (!userKey) {
    throw new Error(`Unknown managed user: ${options.user}`);
  }

  if (shouldDryRun()) {
    throw new Error('Cannot issue a password while BRIDGE_DRY_RUN=true or IMMICH_API_KEY is unset');
  }

  const stateEntry = ensureManagedStateEntry(managedState, userKey);
  if (!stateEntry.email || !stateEntry.immichUserId) {
    throw new Error(`Managed user is missing Immich identity data: ${options.user}`);
  }

  const nextcloudUserId = stateEntry.nextcloudUserId || options.user;
  const generatedPassword = options.password || generatePassword(config.passwordLength);
  const now = new Date().toISOString();
  const previousCredential = credentials[userKey] ? JSON.parse(JSON.stringify(credentials[userKey])) : null;
  const immichUserBefore = await immichGet(`/admin/users/${stateEntry.immichUserId}`);

  const result = {
    startedAt: now,
    finishedAt: null,
    mode: 'issue-password',
    apply: options.apply,
    nextcloudUserId,
    immichEmail: stateEntry.email,
    immichUserId: stateEntry.immichUserId,
    shouldChangePassword: options.shouldChangePassword,
    foundInCredentials: Boolean(previousCredential),
    generatedPassword,
    before: {
      credentials: previousCredential,
      user: {
        id: immichUserBefore.id,
        email: immichUserBefore.email,
        name: immichUserBefore.name,
        shouldChangePassword: immichUserBefore.shouldChangePassword,
      },
    },
    actions: options.apply
      ? [
          'update-immich-user-password',
          options.shouldChangePassword ? 'require-password-change-on-next-direct-login' : 'leave-password-change-flag-disabled',
          'write-credential-to-bridge-state',
        ]
      : [
          'preview-only',
          'no-remote-changes',
          'no-local-state-written',
        ],
  };

  if (options.apply) {
    await immichPut(`/admin/users/${stateEntry.immichUserId}`, {
      password: generatedPassword,
      shouldChangePassword: options.shouldChangePassword,
    });

    credentials[userKey] = {
      email: stateEntry.email,
      nextcloudUserId,
      password: generatedPassword,
      createdAt: previousCredential?.createdAt || now,
    };

    fs.writeFileSync(credentialsPath, JSON.stringify(credentials, null, 2));
  }

  const immichUserAfter = options.apply ? await immichGet(`/admin/users/${stateEntry.immichUserId}`) : immichUserBefore;
  result.finishedAt = new Date().toISOString();
  result.after = {
    credentials: options.apply ? credentials[userKey] : previousCredential,
    user: {
      id: immichUserAfter.id,
      email: immichUserAfter.email,
      name: immichUserAfter.name,
      shouldChangePassword: immichUserAfter.shouldChangePassword,
    },
  };

  fs.writeFileSync(passwordIssueReportPath, JSON.stringify(result, null, 2));
  log(
    `${options.apply ? 'issued' : 'previewed'} direct-login password for ${stateEntry.email}; audit written to ${passwordIssueReportPath}`,
  );
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
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

function normalizeEmail(value) {
  if (!value) {
    return null;
  }

  const normalized = String(value).trim().toLowerCase();
  return normalized || null;
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

async function updateImmichUserName({ users, usersById, usersByEmail, immichUser, displayName }) {
  const updatedUser = await immichPut(`/admin/users/${immichUser.id}`, {
    name: displayName,
  });
  const index = users.findIndex((candidate) => candidate.id === immichUser.id);
  if (index !== -1) {
    users[index] = updatedUser;
  }
  usersById.set(updatedUser.id, updatedUser);
  usersByEmail.set(normalizeEmail(updatedUser.email), updatedUser);
  return updatedUser;
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
