/**
 * @file Checks environment configuration and secret-safe logging without database access.
 * Invalid input must fail before any client or mutation is started.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readConfig, databaseOptions } from '../../src/config.js';
import { createLogger } from '../../src/logging.js';

const environment = {
  POSTGRES_URL: 'postgresql://localhost/myapp',
  MONGODB_URI: 'mongodb://localhost',
};

test('configuration pins bounded pools and requires unambiguous targets', /** Validate defaults without opening either database. */ () => {
  const config = readConfig(environment);
  assert.equal(config.mongoDatabase, 'retail_catalog');
  assert.equal(config.logLevel, 'info');
  const options = databaseOptions(config);
  assert.equal(options.postgres.max, 5);
  assert.equal(options.mongo.maxPoolSize, 5);
  assert.equal(options.mongo.retryWrites, false);
  assert.throws(/** No connection settings are invented. */ () => readConfig({}));
  for (const value of [
    'postgresql://localhost',
    'https://localhost/myapp',
    'postgresql://localhost/myapp?database=another',
  ]) {
    assert.throws(
      /** Ambiguous targets cannot pass validation. */ () =>
        readConfig({ ...environment, POSTGRES_URL: value }),
    );
  }
  assert.throws(
    /** Unknown logging levels fail visibly. */ () =>
      readConfig({ ...environment, LOG_LEVEL: 'verbose' }),
  );
  assert.throws(
    /** Mongo names must be legal before connecting. */ () =>
      readConfig({ ...environment, MONGODB_DATABASE: 'bad/name' }),
  );
});

test('logger redacts connection settings and lease tokens and respects levels', /** Logs are stderr records, not credential or payload dumps. */ () => {
  const records: string[] = [];
  const log = createLogger(
    'info',
    /** Capture emitted JSON without writing terminal output. */ (record) => records.push(record),
  );
  log('job_claimed', {
    jobId: 'id',
    leaseToken: 'secret',
    postgresUrl: 'postgresql://secret',
    nested: { lease_token: 'secret' },
    approvedPayload: { name: 'private' },
  });
  assert.equal(records.length, 1);
  assert.ok(!records[0]?.includes('secret'));
  assert.ok(!records[0]?.includes('private'));
  assert.equal(JSON.parse(records[0] ?? '{}').event, 'job_claimed');
  createLogger(
    'silent',
    /** A silent logger must not call its sink. */ () => assert.fail('silent'),
  )('done', {});
});
