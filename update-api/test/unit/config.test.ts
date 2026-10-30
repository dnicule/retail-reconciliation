/**
 * @file Tests configuration validation, driver limits, and safe startup diagnostics.
 * Pure configuration cases need no database; entrypoint cases use child processes
 * and test-owned loopback sockets to exercise real exit and listener failures.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { execFile } from 'node:child_process';
import { createServer } from 'node:net';
import { fileURLToPath } from 'node:url';
import { readConfig, databaseOptions } from '../../src/config.js';

const base = {
  POSTGRES_URL: 'postgresql://example:secret@127.0.0.1:5432/myapp',
  MONGODB_URI: 'mongodb://127.0.0.1:27017',
};

test('configuration uses project defaults and preserves supplied connection values', /** Checks defaults independently of a running database. */ () => {
  const config = readConfig(base);
  assert.equal(config.host, '127.0.0.1');
  assert.equal(config.port, 43187);
  assert.equal(config.logLevel, 'info');
  assert.equal(config.postgresUrl, base.POSTGRES_URL);
  assert.equal(config.mongoUri, base.MONGODB_URI);
  assert.equal(config.mongoDatabase, 'retail_catalog');
});

test('configuration accepts explicit listener, logging, and catalog overrides', /** Checks valid overrides do not silently revert to defaults. */ () => {
  const config = readConfig({
    ...base,
    HOST: '::1',
    PORT: '8080',
    LOG_LEVEL: 'debug',
    MONGODB_DATABASE: 'isolated_catalog',
  });
  assert.equal(config.host, '::1');
  assert.equal(config.port, 8080);
  assert.equal(config.logLevel, 'debug');
  assert.equal(config.mongoDatabase, 'isolated_catalog');
});

test('invalid ports and hostnames fail before listener or database side effects', /** Rejects malformed overrides rather than selecting another port. */ () => {
  for (const PORT of ['', '0', '-1', '65536', '12.5', '1e3', ' 43187', 'abc']) {
    assert.throws(
      /** Evaluates one invalid port value. */ () => readConfig({ ...base, PORT }),
      /PORT/u,
    );
  }
  for (const HOST of ['', 'https://localhost', 'has space', 'bad/path', '-bad-host']) {
    assert.throws(
      /** Evaluates one invalid listener host. */ () => readConfig({ ...base, HOST }),
      /HOST/u,
    );
  }
  assert.throws(
    /** Rejects a misspelled logger level. */ () => readConfig({ ...base, LOG_LEVEL: 'verbose' }),
    /LOG_LEVEL/u,
  );
});

test('PostgreSQL must name one database without query-string target overrides', /** Prevents fallback or ambiguous target selection. */ () => {
  for (const POSTGRES_URL of [
    '',
    'https://localhost/myapp',
    'postgresql://localhost',
    'postgresql://localhost/',
    'postgresql://localhost/one/two',
    'postgresql://localhost/one%2Ftwo',
    'postgresql://localhost/myapp?database=other',
    'postgresql://localhost/myapp?dbname=other',
    'postgresql://localhost/myapp?DBNAME=other',
    'postgresql://localhost/myapp#fragment',
    'postgresql://localhost/%ZZ',
    'postgresql://user%ZZ:password@localhost/myapp',
    'postgresql://user:password%ZZ@localhost/myapp',
  ]) {
    assert.throws(
      /** Evaluates a malformed or ambiguous PostgreSQL URL. */ () =>
        readConfig({ ...base, POSTGRES_URL }),
      /POSTGRES_URL/u,
    );
  }
});

test('MongoDB syntax and database names are validated lazily without a server', /** Supports real driver URI forms while rejecting invalid catalog settings. */ () => {
  const config = readConfig({
    ...base,
    MONGODB_URI: 'mongodb://host1:27017,host2:27017/?replicaSet=example',
  });
  assert.equal(config.mongoUri, 'mongodb://host1:27017,host2:27017/?replicaSet=example');
  for (const MONGODB_URI of ['', 'https://localhost', 'mongodb://']) {
    assert.throws(
      /** Evaluates one malformed MongoDB URI. */ () => readConfig({ ...base, MONGODB_URI }),
      /MONGODB_URI/u,
    );
  }
  for (const MONGODB_DATABASE of [
    '',
    'has space',
    'one/two',
    'bad.name',
    'bad$database',
    'x'.repeat(64),
  ]) {
    assert.throws(
      /** Evaluates one invalid catalog name. */ () => readConfig({ ...base, MONGODB_DATABASE }),
      /MONGODB_DATABASE/u,
    );
  }
});

test('configuration failures never include connection secrets', /** Verifies failure output is safe for startup logs. */ () => {
  assert.throws(
    /** Triggers invalid database syntax with a credential-bearing URL. */ () =>
      readConfig({ ...base, POSTGRES_URL: 'postgresql://user:top-secret@localhost/' }),
    /** Confirms the variable name is actionable while credentials stay private. */ (
      error: unknown,
    ) =>
      error instanceof Error &&
      error.message.includes('POSTGRES_URL') &&
      !error.message.includes('top-secret'),
  );
});

test('client options cap pools and timeouts and disable standalone-incompatible write retry', /** Checks the actual options consumed by runtime client construction. */ () => {
  const options = databaseOptions(readConfig(base));
  assert.equal(options.postgres.max, 5);
  assert.equal(options.postgres.connectionTimeoutMillis, 3000);
  assert.equal(options.postgres.statement_timeout, 3000);
  assert.equal(options.mongo.maxPoolSize, 5);
  assert.equal(options.mongo.retryWrites, false);
  assert.equal(options.mongo.serverSelectionTimeoutMS, 3000);
  assert.equal(options.mongo.connectTimeoutMS, 3000);
  assert.equal(options.mongo.timeoutMS, 3000);
});

/**
 * Run the compiled entrypoint in a child with deliberately isolated settings.
 * The timeout bounds a broken startup path; the child cannot terminate this test runner.
 * @param overrides - Environment values applied last to select the startup failure scenario.
 * @returns Captured stdout/stderr and exit or execution-error code; success uses zero.
 */
function runRuntime(
  overrides: NodeJS.ProcessEnv,
): Promise<{ code: string | number | null; output: string }> {
  return new Promise(
    /** Starts a child so signal and process-exit behavior cannot affect the test runner. */ (
      resolve,
    ) => {
      execFile(
        process.execPath,
        [fileURLToPath(new URL('../../src/main.js', import.meta.url))],
        {
          env: {
            ...process.env,
            ...base,
            HOST: '127.0.0.1',
            LOG_LEVEL: 'info',
            PORT: '43187',
            ...overrides,
          },
          timeout: 8000,
        },
        /** Returns process status and captured streams after all handles close. */ (
          error,
          stdout,
          stderr,
        ) => {
          resolve({ code: error?.code ?? 0, output: stdout + stderr });
        },
      );
    },
  );
}

test('entrypoint rejects invalid configuration with a safe actionable startup message', /** Ensures connection credentials cannot leak through a top-level exception. */ async () => {
  const result = await runRuntime({ POSTGRES_URL: 'postgresql://user:top-secret@localhost/' });
  assert.equal(result.code, 1);
  assert.match(result.output, /POSTGRES_URL/u);
  assert.equal(result.output.includes('top-secret'), false);
});

test('entrypoint reports an occupied port without choosing another listener', /** Uses a local socket to force a deterministic bind conflict without database access. */ async () => {
  const occupied = createServer();
  await new Promise<void>(
    /** Binds an isolated ephemeral port before starting the child. */ (resolve, reject) => {
      occupied.once('error', reject);
      occupied.listen(0, '127.0.0.1', resolve);
    },
  );
  try {
    const address = occupied.address();
    assert.ok(address && typeof address === 'object');
    const result = await runRuntime({ PORT: String(address.port) });
    assert.equal(result.code, 1);
    assert.match(result.output, /EADDRINUSE/u);
    assert.match(result.output, /port is already in use/u);
  } finally {
    await new Promise<void>(
      /** Closes only the test-owned socket. */ (resolve, reject) =>
        occupied.close(
          /** Reports socket cleanup failure rather than hiding leaked test resources. */ (
            error,
          ) => (error ? reject(error) : resolve()),
        ),
    );
  }
});
