/**
 * @file Exercises the continuous worker's executable boundary in isolated child processes.
 * Invalid configuration must fail before connections and never print supplied secrets.
 */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';

test('worker startup rejects invalid configuration with sanitized JSON', /** Startup must validate configuration before attempting to connect. */ () => {
  const entry = new URL('../../src/main.js', import.meta.url);
  const result = spawnSync(process.execPath, [entry.pathname], {
    env: {
      ...process.env,
      POSTGRES_URL: 'postgresql://secret:password@localhost',
      MONGODB_URI: 'mongodb://localhost',
    },
    encoding: 'utf8',
    timeout: 5000,
  });
  assert.equal(result.status, 1);
  assert.equal(result.stdout, '');
  assert.ok(!result.stderr.includes('secret'));
  assert.ok(!result.stderr.includes('password'));
  const failure = JSON.parse(result.stderr.trim());
  assert.equal(failure.event, 'worker_failed');
  assert.equal(failure.code, 'CONFIGURATION_INVALID');
});
