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
};

const credentialsPath = path.join(config.stateDir, 'credentials.json');
const reportPath = path.join(config.stateDir, 'last-run.json');
const placeholderApiKey = 'REPLACE_WITH_IMMICH_ADMIN_API_KEY';

fs.mkdirSync(config.stateDir, { recursive: true });

async function main() {
  log('bridge starting');

  do {
    const startedAt = new Date().toISOString();
    const report = {
      startedAt,
      finishedAt: null,
      dryRun: shouldDryRun(),
      sourceRoot: config.sourceRoot,
      summary: {
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
        errors: 0,
      },
      usersDiscovered: 0,
      usersProcessed: 0,
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
      skipped: [],
      errors: [],
    };

    try {
      await runSync(report);
    } catch (error) {
      report.errors.push(asError(error));
      log(`cycle failed: ${error.message}`);
    }

    report.finishedAt = new Date().toISOString();
    report.summary.usersCreated = report.usersCreated.length;
    report.summary.usersUpdated = report.usersUpdated.length;
    report.summary.librariesCreated = report.librariesCreated.length;
    report.summary.librariesUpdated = report.librariesUpdated.length;
    report.summary.librariesScanned = report.librariesScanned.length;
    report.summary.mismatches = report.mismatches.length;
    report.summary.errors = report.errors.length;
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
    log(`cycle finished: processed=${report.usersProcessed}, createdUsers=${report.usersCreated.length}, createdLibraries=${report.librariesCreated.length}, errors=${report.errors.length}`);

    if (config.runOnce) {
      break;
    }

    await sleep(config.intervalSeconds * 1000);
  } while (true);
}

async function runSync(report) {
  const discoveredUsers = discoverUsers(report);
  report.usersDiscovered = discoveredUsers.length;

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
    return;
  }

  const users = await immichGet('/admin/users');
  const libraries = await immichGet('/libraries');
  const credentials = loadJson(credentialsPath, {});

  for (const discovered of discoveredUsers) {
    report.usersProcessed += 1;

    const existingUser = users.find((user) => user.email === discovered.immichEmail);
    let userId = existingUser?.id;

    if (!existingUser) {
      const generatedPassword = generatePassword(config.passwordLength);
      credentials[discovered.nextcloudUserId] = {
        email: discovered.immichEmail,
        password: generatedPassword,
        createdAt: new Date().toISOString(),
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
      report.usersUpdated.push({
        nextcloudUserId: discovered.nextcloudUserId,
        immichUserId: existingUser.id,
        fields: ['name'],
      });
      userId = updatedUser.id;
    }

    const desiredLibraryName = `${config.libraryNamePrefix} ${discovered.nextcloudUserId}`;
    const desiredImportPath = discovered.libraryPath;
    const existingLibrary = libraries.find((library) => {
      const sameOwner = library.ownerId === userId;
      const importPaths = Array.isArray(library.importPaths) ? library.importPaths : [];
      return sameOwner && importPaths.includes(desiredImportPath);
    });

    let libraryId = existingLibrary?.id;

    if (!existingLibrary) {
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
        report.librariesUpdated.push({
          nextcloudUserId: discovered.nextcloudUserId,
          libraryId: existingLibrary.id,
        });
        libraryId = updatedLibrary.id;
      }
    }

    if (libraryId) {
      await immichPost(`/libraries/${libraryId}/scan`, {});
      report.librariesScanned.push({
        nextcloudUserId: discovered.nextcloudUserId,
        libraryId,
      });
    }
  }

  fs.writeFileSync(credentialsPath, JSON.stringify(credentials, null, 2));

  const expectedUsers = new Set(discoveredUsers.map((user) => user.immichEmail));
  for (const user of users) {
    if (!expectedUsers.has(user.email) && !user.isAdmin) {
      report.mismatches.push({
        type: 'orphan-immich-user',
        immichUserId: user.id,
        immichEmail: user.email,
      });
    }
  }
}

function discoverUsers(report) {
  const entries = fs.readdirSync(config.sourceRoot, { withFileTypes: true });
  const discovered = [];

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
    discovered.push(mappedUser);
    report.eligibleUsers.push(mappedUser);
  }

  report.summary.eligible = report.eligibleUsers.length;
  report.summary.appdataSkipped = report.appdataSkipped.length;
  report.summary.missingPhotos = report.missingPhotos.length;
  return discovered.sort((left, right) => left.nextcloudUserId.localeCompare(right.nextcloudUserId));
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
