/**
 * @file Provides an isolated worker process for deterministic crash and lease recovery tests.
 * Test-only wrappers stop MongoDB dispatch while SQL commits; parent-owned fixture IDs and
 * externally initialized TEST_* databases are required, and this helper never provisions data.
 */
import assert from 'node:assert/strict';
import { MongoClient } from 'mongodb';
import { Pool } from 'pg';
import { createJobRepository } from '../../src/jobs.js';
import { createMongoStore, createSqlStore } from '../../src/products.js';
import { processJob } from '../../src/processor.js';
import type { Logger, ProductStore } from '../../src/model/contracts.js';
import type { RepairResult } from '../../src/model/reconciliation.js';
import { readUpdateJob } from './job-state.js';

/** Send a controlled lifecycle event after the corresponding real operation is acknowledged. */
async function report(event: string, fields: Record<string, unknown> = {}): Promise<void> {
  await new Promise<void>(
    /** Wait for IPC transmission so the parent kills only after observing the committed-write barrier. */ (
      resolve,
      reject,
    ) => {
      if (!process.send) {
        reject(new Error('Crash test requires an IPC parent.'));
        return;
      }
      process.send(
        { event, ...fields },
        /** Resolve transmission or propagate only its transport failure. */ (
          error: Error | null,
        ) => {
          if (error) reject(error);
          else resolve();
        },
      );
    },
  );
}

/**
 * Claim one test-owned job in a fresh process and either pause after SQL or finish recovery.
 * The parent kills a paused process; normal paths close every borrowed client before exit.
 */
async function main(): Promise<void> {
  const [mode, jobId] = process.argv.slice(2);
  assert.ok((mode === 'crash' || mode === 'resume') && jobId && /^[a-f0-9-]{36}$/.test(jobId));
  const postgresUrl = process.env.TEST_POSTGRES_URL,
    mongoUri = process.env.TEST_MONGODB_URI,
    database = process.env.TEST_MONGODB_DATABASE;
  assert.ok(postgresUrl && mongoUri && database);
  let postgres: URL;
  let mongo: URL;
  try {
    postgres = new URL(postgresUrl);
    mongo = new URL(mongoUri);
  } catch {
    throw new Error('Invalid test connection syntax.');
  }
  const sqlDatabase = decodeURIComponent(postgres.pathname.slice(1));
  assert.match(sqlDatabase, /^(?:reconciliation_worker|update_api)_test_[a-z0-9_]+$/);
  assert.match(database, /^(?:reconciliation_worker|update_api)_test_[a-z0-9_]+$/);
  for (const key of postgres.searchParams.keys())
    assert.ok(
      !['db', 'dbname', 'database', 'connectionstring', 'options'].includes(key.toLowerCase()),
    );
  assert.ok(!mongo.pathname.slice(1) || decodeURIComponent(mongo.pathname.slice(1)) === database);
  for (const key of mongo.searchParams.keys())
    assert.ok(!['db', 'dbname', 'database', 'connectionstring'].includes(key.toLowerCase()));
  const pool = new Pool({
    connectionString: postgresUrl,
    max: 5,
    connectionTimeoutMillis: 3000,
    statement_timeout: 3000,
    query_timeout: 3000,
  });
  const client = new MongoClient(mongoUri, {
    retryWrites: false,
    retryReads: false,
    maxPoolSize: 5,
    serverSelectionTimeoutMS: 3000,
    timeoutMS: 3000,
  });
  const log: Logger =
    /** Child tests expose only controlled IPC events, never credentials or private leases. */ () => {};
  try {
    assert.equal(
      (await pool.query<{ name: string }>('SELECT current_database() AS name')).rows[0]?.name,
      sqlDatabase,
    );
    const jobs = createJobRepository(pool, log);
    const sql = createSqlStore(pool, log);
    const mongoStore = createMongoStore(client, database, log);
    await Promise.all([jobs.ready(Date.now() + 15_000), mongoStore.ready(Date.now() + 15_000)]);
    const existing = await readUpdateJob(pool, jobId);
    assert.ok(
      existing?.kind === 'update' && existing.productId.startsWith('WIT-'),
      'Crash helper accepts only registered test update identities.',
    );
    const claim = await jobs.claim(Date.now() + 15_000, jobId);
    assert.equal(claim.kind, 'claimed');
    if (claim.kind !== 'claimed') throw new Error('Crash test job was not claimable.');
    const sqlWithBarrier: ProductStore = {
      ...sql,
      /** Report only after the real SQL write is acknowledged, before any job bookkeeping. */
      async repair(snapshot, observation, deadline): Promise<RepairResult> {
        const result = await sql.repair(snapshot, observation, deadline);
        if (mode === 'crash' && result.outcome === 'applied')
          await report('sql_committed', { jobId });
        return result;
      },
    };
    const mongoWithBarrier: ProductStore = {
      ...mongoStore,
      /** Prevent the MongoDB write entirely while the parent stages the process interruption. */
      async repair(snapshot, observation, deadline): Promise<RepairResult> {
        if (mode === 'crash')
          return new Promise<RepairResult>(
            /** Intentionally never resolves: the parent owns termination at the SQL-commit barrier. */ () => {},
          );
        return mongoStore.repair(snapshot, observation, deadline);
      },
    };
    const result = await processJob(claim.job, {
      jobs,
      sql: sqlWithBarrier,
      mongo: mongoWithBarrier,
      log,
    });
    await report('processed', { status: result.status, jobId });
    if (result.status !== 'completed') process.exitCode = 1;
  } finally {
    await Promise.all([pool.end(), client.close()]);
  }
}

void main()
  .catch(
    /** Report a sanitized failure without serializing driver messages or connection details. */ async () => {
      process.exitCode = 1;
      if (process.connected) await report('failed');
    },
  )
  .finally(
    /** Release the IPC channel after normal completion so the child can exit. */ () => {
      if (process.connected) process.disconnect();
    },
  );
