/**
 * @file Runtime ownership of the standalone API listener and shared database clients.
 * Starts lazy clients, logs sanitized lifecycle outcomes, and drains work on termination.
 * Importing this module does not start the service; no startup path initializes databases.
 */
import { pathToFileURL } from 'node:url';
import { MongoClient } from 'mongodb';
import { Pool } from 'pg';
import { buildApp } from './app.js';
import { ConfigurationError, databaseOptions, readConfig } from './config.js';
import { createJobRepository } from './jobs.js';
import { createMongoDestination, createSqlDestination } from './products.js';

/**
 * Extract a syntactically safe error code for lifecycle logs.
 * @param error - Unknown startup or idle-pool failure; messages are deliberately not inspected.
 * @returns Its uppercase code of at most 40 characters, or STARTUP_FAILED when unsuitable.
 */
function safeCode(error: unknown): string {
  if (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    typeof error.code === 'string' &&
    /^[A-Z0-9_]{1,40}$/u.test(error.code)
  )
    return error.code;
  return 'STARTUP_FAILED';
}

/**
 * Report startup failure when the configured application logger is unavailable.
 * @param error - Failure whose controlled configuration message or safe code can be exposed.
 * @returns Nothing; emits JSON to stderr and sets the process exit code to one.
 */
function reportStartupFailure(error: unknown): void {
  console.error(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level: 'error',
      component: 'update-api',
      event: 'startup_failed',
      code: error instanceof ConfigurationError ? 'CONFIGURATION_INVALID' : safeCode(error),
      message:
        error instanceof ConfigurationError
          ? error.message
          : 'The API could not start; inspect configuration and listener availability.',
    }),
  );
  process.exitCode = 1;
}

/**
 * Opens the HTTP listener with lazy database clients so dependency outages remain inspectable.
 * Signal handling stops admission, drains bounded requests, then closes the shared clients once.
 * @returns Resolves after listening and registering signal handlers, or after handling a
 * listener failure and cleanup. Resolution does not mean database readiness or process exit.
 * @throws If configuration or client/application construction fails before listening,
 * or cleanup of a failed listener unexpectedly rejects; the executable entry point reports these failures.
 */
export async function main(): Promise<void> {
  const config = readConfig();
  const options = databaseOptions(config);
  const pool = new Pool(options.postgres);
  const mongoClient = new MongoClient(config.mongoUri, options.mongo);
  const jobs = createJobRepository(pool);
  const sql = createSqlDestination(pool);
  const mongo = createMongoDestination(mongoClient, config.mongoDatabase);
  const app = buildApp({ jobs, sql, mongo, logger: { level: config.logLevel } });
  let stopping: Promise<void> | undefined;

  pool.on(
    'error',
    /** Handles idle connection failures without crashing the listener or logging credentials. */ (
      error,
    ) => {
      app.log.warn(
        { component: 'update-api', event: 'postgres_pool_error', code: safeCode(error) },
        'PostgreSQL connection became unavailable; later requests can reconnect',
      );
    },
  );

  /**
   * Drain the listener before closing both runtime-owned database clients.
   * @param reason - Controlled signal or startup-failure label included in lifecycle logs.
   * @returns Resolves after cleanup attempts and signal-handler removal; unconfirmed closures
   * set exit code one. An 18-second deadline forcefully exits with durable work left for recovery.
   */
  async function drain(reason: string): Promise<void> {
    const started = Date.now();
    app.log.info(
      { component: 'update-api', event: 'shutdown_started', reason },
      'Stopping API admission and draining requests',
    );
    const timeout = setTimeout(
      /** Ends a stuck shutdown; durable processing leases remain recoverable by the worker. */ () => {
        app.log.error(
          {
            component: 'update-api',
            event: 'shutdown_deadline',
            durationMs: Date.now() - started,
            unfinishedWork: true,
          },
          'Shutdown deadline elapsed; unfinished durable jobs require worker recovery',
        );
        app.server.closeAllConnections();
        process.exit(1);
      },
      18_000,
    );
    timeout.unref();
    let failed = false;
    try {
      // Fastify stops accepting new work and waits for admitted HTTP requests before clients close.
      try {
        await app.close();
      } catch {
        failed = true;
        app.log.error(
          { component: 'update-api', event: 'listener_close_failed' },
          'The HTTP listener did not confirm graceful closure',
        );
      }
      const closed = await Promise.allSettled([pool.end(), mongoClient.close()]);
      for (const [index, result] of closed.entries()) {
        if (result.status === 'rejected') {
          failed = true;
          app.log.error(
            {
              component: 'update-api',
              event: 'database_close_failed',
              target: index === 0 ? 'postgresql' : 'mongodb',
            },
            'A database client did not confirm closure',
          );
        }
      }
      if (failed) process.exitCode = 1;
      app.log[failed ? 'error' : 'info'](
        {
          component: 'update-api',
          event: 'shutdown_finished',
          outcome: failed ? 'incomplete' : 'closed',
          durationMs: Date.now() - started,
        },
        failed
          ? 'API shutdown finished with unconfirmed cleanup'
          : 'API listener and database clients closed',
      );
    } finally {
      clearTimeout(timeout);
      process.off('SIGINT', onInterrupt);
      process.off('SIGTERM', onTerminate);
    }
  }

  /**
   * Coalesce simultaneous shutdown requests so shared clients close only once.
   * @param reason - Reason passed to the first drain request; later callers share that request.
   * @returns The existing or newly started cleanup promise, including any unexpected rejection.
   */
  function shutdown(reason: string): Promise<void> {
    stopping ??= drain(reason);
    return stopping;
  }

  /**
   * Start bounded cleanup for SIGINT without blocking the signal callback.
   * @returns Nothing; attaches sanitized reporting to any unexpected cleanup rejection.
   */
  function onInterrupt(): void {
    void shutdown('SIGINT').catch(reportStartupFailure);
  }

  /**
   * Start bounded cleanup for SIGTERM without blocking the signal callback.
   * @returns Nothing; attaches sanitized reporting to any unexpected cleanup rejection.
   */
  function onTerminate(): void {
    void shutdown('SIGTERM').catch(reportStartupFailure);
  }

  process.on('SIGINT', onInterrupt);
  process.on('SIGTERM', onTerminate);
  try {
    // Constructors above are lazy. Readiness endpoints probe existing stores without startup writes.
    await app.listen({ host: config.host, port: config.port });
    app.log.info(
      { component: 'update-api', event: 'api_listening', host: config.host, port: config.port },
      'Update API listener started',
    );
  } catch (error) {
    const code = safeCode(error);
    app.log.error(
      {
        component: 'update-api',
        event: 'startup_failed',
        code,
        host: config.host,
        port: config.port,
      },
      code === 'EADDRINUSE'
        ? 'Configured API port is already in use; stop its owner or set PORT explicitly'
        : 'API listener could not start',
    );
    process.exitCode = 1;
    await shutdown('startup_failure');
  }
}

// Importing this module for tooling does not start listeners or consume database connections.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main().catch(reportStartupFailure);
}
