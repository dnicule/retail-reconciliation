/**
 * @file Exercises the compiled process through real HTTP requests and termination signals.
 * Unreachable loopback database endpoints isolate outage behavior from application data.
 * The suite owns the child process, listener reservation, timers, and cleanup.
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

/**
 * Limit how long an assertion waits without pretending to cancel the underlying work.
 * The timer is cleared after either outcome; subprocess cleanup belongs to the caller.
 * @param work - Already-started operation whose settlement is observed by Promise.race.
 * @param timeoutMs - Maximum local wait in milliseconds.
 * @param message - Failure message when the local deadline wins.
 * @returns The work's value when it settles before the timeout.
 * @throws If the work rejects or the local deadline expires.
 */
async function bounded<T>(work: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>(
    /** Fails a stuck subprocess or readiness wait deterministically. */ (_resolve, reject) => {
      timer = setTimeout(
        /** Rejects without claiming the child operation was cancelled. */ () =>
          reject(new Error(message)),
        timeoutMs,
      );
    },
  );
  try {
    return await Promise.race([work, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Ask the operating system for a listener port and release the temporary reservation.
 * Production rejects port zero, so the child receives this concrete port. The port
 * is no longer reserved when returned and another process could acquire it first.
 * @returns A loopback port that was available during the reservation.
 * @throws If binding, inspecting the address, or closing the reservation fails.
 */
async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>(
    /** Lets the operating system assign an isolated test listener port. */ (resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    },
  );
  try {
    const address = server.address();
    assert.ok(address && typeof address === 'object');
    return address.port;
  } finally {
    await new Promise<void>(
      /** Releases only this test's port reservation. */ (resolve, reject) =>
        server.close(
          /** Makes reservation cleanup failures visible to the test. */ (error) =>
            error ? reject(error) : resolve(),
        ),
    );
  }
}

/**
 * Assert that decoded health JSON is an object before reading nested response fields.
 * @param value - Untrusted value returned by JSON decoding.
 * @returns A shallow copy with string-keyed, still-unknown property values.
 * @throws An assertion error for null, arrays, or non-object values.
 */
function record(value: unknown): Record<string, unknown> {
  assert.ok(typeof value === 'object' && value !== null && !Array.isArray(value));
  return Object.fromEntries(Object.entries(value));
}

test(
  'process starts with both databases unavailable and exits gracefully after SIGTERM',
  { timeout: 25_000 },
  /** Uses unreachable local dependencies so neither initialization nor application database access is possible. */ async () => {
    const port = await availablePort();
    const child = spawn(
      process.execPath,
      [fileURLToPath(new URL('../../src/main.js', import.meta.url))],
      {
        env: {
          ...process.env,
          HOST: '127.0.0.1',
          PORT: String(port),
          LOG_LEVEL: 'info',
          POSTGRES_URL: 'postgresql://runtime_user:runtime-test-secret@127.0.0.1:1/runtime_test',
          MONGODB_URI:
            'mongodb://runtime_user:runtime-test-secret@127.0.0.1:1/runtime_test?authSource=admin',
          MONGODB_DATABASE: 'runtime_test',
        },
        stdio: ['pipe', 'pipe', 'pipe'],
        timeout: 22_000,
        killSignal: 'SIGKILL',
      },
    );
    let stdout = '',
      stderr = '',
      closed = false;
    let confirmStarted: (() => void) | undefined;
    let failStarted: ((error: Error) => void) | undefined;
    const started = new Promise<void>(
      /** Exposes a gate driven by the confirmed listener log. */ (resolve, reject) => {
        confirmStarted = resolve;
        failStarted = reject;
      },
    );
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on(
      'data',
      /** Captures structured lifecycle evidence and observes acknowledged listener startup. */ (
        chunk: string,
      ) => {
        stdout += chunk;
        if (stdout.includes('"event":"api_listening"')) confirmStarted?.();
      },
    );
    child.stderr.on(
      'data',
      /** Retains stderr so startup diagnostics can be checked for credential leakage. */ (
        chunk: string,
      ) => {
        stderr += chunk;
      },
    );
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      /** Observes exit immediately so fast child failures cannot escape cleanup. */ (resolve) => {
        child.once(
          'error',
          /** Makes process creation failures fail the pending startup gate. */ () => {
            failStarted?.(new Error('API child process could not start.'));
          },
        );
        child.once(
          'close',
          /** Resolves after exit and output-stream closure, retaining the true termination reason. */ (
            code,
            signal,
          ) => {
            closed = true;
            failStarted?.(new Error('API child exited before confirmed listener startup.'));
            resolve({ code, signal });
          },
        );
      },
    );
    try {
      await bounded(started, 5000, 'API listener did not start while databases were unavailable.');
      const live = await fetch(`http://127.0.0.1:${port}/health/live`, {
        signal: AbortSignal.timeout(5000),
      });
      assert.equal(live.status, 200);
      assert.deepEqual(await live.json(), { live: true });

      const readiness = await fetch(`http://127.0.0.1:${port}/health/ready`, {
        signal: AbortSignal.timeout(5000),
      });
      assert.equal(readiness.status, 503);
      const health = record(await readiness.json());
      const dependencies = record(health.dependencies);
      assert.equal(health.ready, false);
      assert.equal(record(dependencies.postgres).available, false);
      assert.equal(record(dependencies.mongodb).available, false);

      assert.equal(child.kill('SIGTERM'), true);
      const status = await bounded(
        exited,
        8000,
        'API did not finish graceful shutdown after SIGTERM.',
      );
      assert.equal(status.code, 0);
      assert.equal(status.signal, null);
      assert.match(stdout, /"event":"shutdown_started"/u);
      assert.match(stdout, /"reason":"SIGTERM"/u);
      assert.match(stdout, /"event":"shutdown_finished"/u);
      assert.match(stdout, /"outcome":"closed"/u);
      assert.equal((stdout + stderr).includes('runtime-test-secret'), false);
    } finally {
      // Force only this test-owned child if an assertion fails; never leave a background listener behind.
      if (!closed) {
        child.kill('SIGKILL');
        await bounded(exited, 2000, 'Test child did not terminate during forced cleanup.');
      }
    }
  },
);
