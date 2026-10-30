/**
 * @file Starts the continuous worker and owns database clients and bounded process shutdown.
 * Uses existing schemas read-only at startup; importing this module starts no work or listeners.
 */
import { pathToFileURL } from 'node:url';
import { Client, Pool } from 'pg';
import { MongoClient } from 'mongodb';
import { ConfigurationError, databaseOptions, readConfig } from './config.js';
import { createLogger } from './logging.js';
import { createJobRepository } from './jobs.js';
import { createMongoStore, createSqlStore } from './products.js';
import { processJob } from './processor.js';
import { createStartupReadiness } from './readiness.js';
import { runContinuous } from './runtime.js';
import type { RuntimeDependencies } from './runtime.js';
import { SystemicError } from './model/diagnostic.js';

/**
 * Run continuously with environment configuration, bounded clients and independent listener ownership.
 * @returns Resolves after shutdown drains the current attempt and closes owned connections.
 * @throws Configuration, infrastructure or cleanup errors; executable reporting sanitizes them.
 */
export async function main(): Promise<void> {
  const config = readConfig(),
    options = databaseOptions(config);
  const log = createLogger(config.logLevel),
    pool = new Pool(options.postgres);
  const client = new MongoClient(config.mongoUri, options.mongo);
  const jobs = createJobRepository(pool, log),
    sql = createSqlStore(pool, log),
    mongo = createMongoStore(client, config.mongoDatabase, log);
  const controller = new AbortController();
  let deadlineTimer: NodeJS.Timeout | undefined;
  pool.on(
    'error',
    /** Idle pool errors should not terminate the worker or leak connection information. */ () => {
      log('postgres_pool_unavailable', { outcome: 'reconnect_on_next_operation' });
    },
  );

  /** Bound shutdown to eighteen seconds; durable uncompleted work remains recoverable by lease expiry. */
  function beginShutdown(): void {
    if (controller.signal.aborted) return;
    log('shutdown_started', { outcome: 'draining' });
    controller.abort();
    deadlineTimer = setTimeout(
      /** Stop a stuck close after logging explicit uncertainty. */ () => {
        log('shutdown_deadline', { level: 'error', outcome: 'unfinished', unfinishedWork: true });
        process.exit(1);
      },
      18000,
    );
    deadlineTimer.unref();
  }

  const runtime: RuntimeDependencies = {
    jobs,
    /** Process exactly the committed lease passed by the scheduler. */ process(job, deadline) {
      return processJob(job, { jobs, sql, mongo, log }, deadline);
    },
    ready: createStartupReadiness(
      /** Validate the existing queue and SQL product schema within the startup budget. */ (
        deadline,
      ) => jobs.ready(deadline),
      /** Validate the existing MongoDB collection and effective permissions within the startup budget. */ (
        deadline,
      ) => mongo.ready(deadline),
      log,
    ),
    log,
  };
  process.on('SIGINT', beginShutdown);
  process.on('SIGTERM', beginShutdown);
  const started = Date.now();
  try {
    await runContinuous(runtime, {
      signal: controller.signal,
      onStopping: beginShutdown,
      /** Allocate a dedicated connection separate from the five-connection query pool. */ listenerFactory() {
        return new Client({
          connectionString: config.postgresUrl,
          connectionTimeoutMillis: 3000,
          statement_timeout: 3000,
          query_timeout: 3000,
          application_name: 'retailer-worker-listener',
        });
      },
    });
  } finally {
    // Unexpected runtime rejection or partial startup must also have bounded resource cleanup.
    if (!deadlineTimer) {
      deadlineTimer = setTimeout(
        /** A stuck driver close must not prevent process termination indefinitely. */ () => {
          log('shutdown_deadline', { level: 'error', outcome: 'unfinished', unfinishedWork: true });
          process.exit(1);
        },
        5000,
      );
      deadlineTimer.unref();
    }
    const closed = await Promise.allSettled([pool.end(), client.close()]);
    clearTimeout(deadlineTimer);
    process.off('SIGINT', beginShutdown);
    process.off('SIGTERM', beginShutdown);
    const failed = closed.some(
      /** A rejected closure is not a confirmed shutdown. */ (result) =>
        result.status === 'rejected',
    );
    log('shutdown_finished', {
      level: failed ? 'error' : 'info',
      outcome: failed ? 'unconfirmed' : 'closed',
      durationMs: Date.now() - started,
    });
    if (failed) throw new Error('Database client closure was unconfirmed.');
  }
}

/**
 * Emit a controlled worker error without driver messages, SQL or connection strings.
 * @param error - Rejected startup or runtime operation; only explicitly safe errors contribute messages.
 * @returns Nothing; writes one JSON record to stderr and marks the process unsuccessful.
 */
function report(error: unknown): void {
  const diagnostic =
    error instanceof ConfigurationError
      ? { code: 'CONFIGURATION_INVALID', message: error.message }
      : error instanceof SystemicError
        ? error.diagnostic
        : {
            code: 'WORKER_FAILED',
            message:
              'Worker stopped unexpectedly; inspect database availability and configuration.',
          };
  createLogger('error')('worker_failed', { level: 'error', ...diagnostic });
  process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main().catch(report);
}
