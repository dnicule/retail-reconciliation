/**
 * @file Verifies worker repair, conflict, uncertainty, and process-crash recovery against real stores.
 * Only externally initialized isolated TEST_* databases are accepted. Fresh WIT identities
 * register every owned product and job before writes; cleanup never removes unrelated data.
 */
import assert from 'node:assert/strict';
import { fork, spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';
import type { TestContext } from 'node:test';
import { Int32, MongoClient } from 'mongodb';
import { Pool } from 'pg';
import { createJobRepository } from '../../src/jobs.js';
import { createMongoStore, createSqlStore } from '../../src/products.js';
import { processJob } from '../../src/processor.js';
import type { ProcessorDependencies } from '../../src/processor.js';
import type { Job } from '../../src/model/job.js';
import type { Logger, ProductStore } from '../../src/model/contracts.js';
import type { Snapshot } from '../../src/model/product.js';
import { readUpdateJob } from '../support/job-state.js';
type TestConfiguration = {
  postgresUrl: string;
  postgresDatabase: string;
  mongoUri: string;
  mongoDatabase: string;
};
type ChildMessage = { event: string; status?: string };
type RunningChild = {
  child: ChildProcess;
  reached: Promise<ChildMessage>;
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
};
const seedTime = new Date('2026-01-01T12:00:00.000Z');
/** Parse a connection URI without including credentials in assertion errors. */
function connection(value: string): URL {
  try {
    return new URL(value);
  } catch {
    throw new Error('Invalid TEST_* connection URI; value omitted.');
  }
}
/**
 * Validate explicit isolated targets before any connection or fixture write.
 * @param env - All three TEST_* settings, or none to skip execution.
 * @returns Guarded targets, or null when integration settings are absent.
 * @throws If settings are incomplete, ambiguous, or target application databases.
 */
function configuration(env: NodeJS.ProcessEnv): TestConfiguration | null {
  const postgresUrl = env.TEST_POSTGRES_URL,
    mongoUri = env.TEST_MONGODB_URI,
    mongoDatabase = env.TEST_MONGODB_DATABASE;
  if (!postgresUrl && !mongoUri && !mongoDatabase) return null;
  assert.ok(
    postgresUrl && mongoUri && mongoDatabase,
    'All TEST_* settings must be supplied together.',
  );
  const postgres = connection(postgresUrl),
    mongo = connection(mongoUri);
  assert.ok(
    ['postgres:', 'postgresql:'].includes(postgres.protocol) && postgres.hostname && !postgres.hash,
  );
  assert.ok(['mongodb:', 'mongodb+srv:'].includes(mongo.protocol) && mongo.hostname && !mongo.hash);
  const postgresDatabase = decodeURIComponent(postgres.pathname.slice(1));
  const permitted = /^(?:reconciliation_worker|update_api)_test_[a-z0-9_]+$/;
  assert.match(postgresDatabase, permitted, 'Refusing application PostgreSQL target.');
  assert.match(mongoDatabase, permitted, 'Refusing application MongoDB target.');
  for (const key of postgres.searchParams.keys())
    assert.ok(
      !['db', 'dbname', 'database', 'connectionstring', 'options'].includes(key.toLowerCase()),
      'Refusing target/session overrides.',
    );
  const mongoPath = decodeURIComponent(mongo.pathname.slice(1));
  assert.ok(
    !mongoPath || mongoPath === mongoDatabase,
    'MongoDB path must match the checked database.',
  );
  for (const key of mongo.searchParams.keys())
    assert.ok(
      !['db', 'dbname', 'database', 'connectionstring'].includes(key.toLowerCase()),
      'Refusing MongoDB target overrides.',
    );
  return { postgresUrl, postgresDatabase, mongoUri, mongoDatabase };
}
/** Allocate a fresh absolute deadline after earlier setup operations finish. */
function deadline(): number {
  return Date.now() + 15_000;
}
/** Construct source-assigned content without reading destination revisions. */
function snapshot(
  productId: string,
  revision: number,
  name = 'Integration notebook',
  description = `Approved revision ${revision}`,
  category = 'Tests',
): Snapshot {
  return { productId, revision, product: { name, description, category } };
}
/**
 * Start a separate worker process with a deterministic IPC barrier.
 * @param mode - Crash pauses before Mongo repair; resume finishes a fresh claim.
 * @param jobId - Owned fixture job; the child never scans unrelated work.
 * @param children - Parent-owned cleanup registry for every child process.
 * @returns Expected event and exit promises, with bounded barrier failure.
 */
function startChild(
  mode: 'crash' | 'resume',
  jobId: string,
  children: Set<ChildProcess>,
): RunningChild {
  const child = fork(
    fileURLToPath(new URL('../support/crash-child.js', import.meta.url)),
    [mode, jobId],
    { env: process.env, execArgv: [], stdio: ['ignore', 'ignore', 'ignore', 'ipc'] },
  );
  children.add(child);
  const lifetime = setTimeout(
    /** Bound child cleanup as well as the IPC barrier; a timeout remains a failing exit signal. */ () =>
      child.kill('SIGKILL'),
    25_000,
  );
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
    /** Observe termination before the child can reach its write barrier. */ (resolve) => {
      child.once(
        'exit',
        /** Retire an owned process after it can no longer mutate fixtures. */ (code, signal) => {
          clearTimeout(lifetime);
          children.delete(child);
          resolve({ code, signal });
        },
      );
      child.once(
        'error',
        /** Retire a process that failed to spawn before any fixture could be touched. */ () => {
          clearTimeout(lifetime);
          children.delete(child);
          resolve({ code: 1, signal: null });
        },
      );
    },
  );
  const reached = new Promise<ChildMessage>(
    /** Accept only the controlled expected event and reject early termination. */ (
      resolve,
      reject,
    ) => {
      const timer = setTimeout(
        /** Bound the barrier wait; cleanup separately owns actual termination. */ () => {
          cleanup();
          reject(new Error('Crash helper did not reach its expected barrier.'));
        },
        20_000,
      );
      /** Remove temporary observers once the barrier resolves or fails. */
      function cleanup(): void {
        clearTimeout(timer);
        child.off('message', onMessage);
        child.off('error', onError);
        child.off('exit', onExit);
      }
      /** Inspect controlled IPC fields without serializing child errors or environment. */
      function onMessage(value: unknown): void {
        if (
          typeof value !== 'object' ||
          value === null ||
          !('event' in value) ||
          typeof value.event !== 'string'
        )
          return;
        if (value.event === 'failed') {
          cleanup();
          reject(new Error('Crash helper reported a processing failure.'));
          return;
        }
        if (value.event !== (mode === 'crash' ? 'sql_committed' : 'processed')) return;
        cleanup();
        resolve({
          event: value.event,
          ...('status' in value && typeof value.status === 'string'
            ? { status: value.status }
            : {}),
        });
      }
      /** Sanitize process-spawn and IPC errors. */
      function onError(): void {
        cleanup();
        reject(new Error('Crash helper could not execute.'));
      }
      /** Any exit before the expected event is a failed barrier. */
      function onExit(): void {
        cleanup();
        reject(new Error('Crash helper exited before its expected event.'));
      }
      child.on('message', onMessage);
      child.once('error', onError);
      child.once('exit', onExit);
    },
  );
  return { child, reached, exited };
}

test('reconciliation integration guards reject application databases and target overrides', /** Guards run before any socket or fixture exists. */ () => {
  const safe = {
    TEST_POSTGRES_URL: 'postgresql://localhost/reconciliation_worker_test_safe',
    TEST_MONGODB_URI: 'mongodb://localhost',
    TEST_MONGODB_DATABASE: 'reconciliation_worker_test_safe',
  };
  assert.ok(configuration(safe));
  assert.equal(configuration({}), null);
  assert.throws(
    /** Incomplete settings cannot choose application defaults. */ () =>
      configuration({ TEST_POSTGRES_URL: safe.TEST_POSTGRES_URL }),
  );
  assert.throws(
    /** Protect populated sales data. */ () =>
      configuration({ ...safe, TEST_POSTGRES_URL: 'postgresql://localhost/myapp' }),
  );
  assert.throws(
    /** Protect populated catalog data. */ () =>
      configuration({ ...safe, TEST_MONGODB_DATABASE: 'retail_catalog' }),
  );
  assert.throws(
    /** Query precedence cannot change the checked target. */ () =>
      configuration({ ...safe, TEST_POSTGRES_URL: `${safe.TEST_POSTGRES_URL}?database=myapp` }),
  );
  assert.throws(
    /** MongoDB cannot contradict its explicit test target. */ () =>
      configuration({ ...safe, TEST_MONGODB_URI: 'mongodb://localhost/retail_catalog' }),
  );
});
const config = configuration(process.env);
test(
  'real-store reconciliation and child-process crash recovery',
  {
    skip:
      config === null
        ? 'Set all TEST_* variables to externally initialized isolated databases.'
        : false,
  },
  /** Exercise actual products and durable jobs; cleanup removes only this invocation's registered identities. */ async (
    t: TestContext,
  ) => {
    assert.ok(config);
    const pool = new Pool({
      connectionString: config.postgresUrl,
      max: 5,
      connectionTimeoutMillis: 3000,
      statement_timeout: 3000,
      query_timeout: 3000,
    });
    const client = new MongoClient(config.mongoUri, {
      retryWrites: false,
      retryReads: false,
      maxPoolSize: 5,
      serverSelectionTimeoutMS: 3000,
      timeoutMS: 3000,
    });
    const products: string[] = [],
      children = new Set<ChildProcess>();
    const events: { event: string; fields: Record<string, unknown> }[] = [];
    const log: Logger =
      /** Capture operation evidence without forwarding credential-bearing driver errors. */ (
        event,
        fields,
      ) => {
        events.push({ event, fields });
      };
    const jobs = createJobRepository(pool, log);
    const sql = createSqlStore(pool, log),
      mongo = createMongoStore(client, config.mongoDatabase, log);
    const collection = client
      .db(config.mongoDatabase)
      .collection<{ _id: string; [key: string]: unknown }>('products');
    const dependencies: ProcessorDependencies = {
      jobs,
      sql,
      mongo,
      log,
      random: /** Choose repeatable minimum retry jitter. */ () => 0,
    };
    let safeToClean = false;
    t.after(
      /** Stop all owned children before deleting owned rows and always close both clients. */ async () => {
        try {
          await Promise.all(
            [...children].map(
              /** Kill abandoned barriers and await real termination. */ (child) =>
                new Promise<void>(
                  /** Install exit observation before signaling. */ (resolve) => {
                    if (child.exitCode !== null || child.signalCode !== null) {
                      resolve();
                      return;
                    }
                    child.once(
                      'exit',
                      /** Confirm the child cannot write after fixture cleanup. */ () => resolve(),
                    );
                    child.kill('SIGKILL');
                  },
                ),
            ),
          );
          if (safeToClean && products.length > 0) {
            await pool.query(
              'DELETE FROM public.reconciliation_jobs WHERE product_id=ANY($1::text[])',
              [products],
            );
            await pool.query('DELETE FROM public.products WHERE product_id=ANY($1::text[])', [
              products,
            ]);
            await collection.deleteMany({ _id: { $in: products } });
          }
        } finally {
          await Promise.all([pool.end(), client.close()]);
        }
      },
    );
    assert.equal(
      (await pool.query<{ name: string }>('SELECT current_database() AS name')).rows[0]?.name,
      config.postgresDatabase,
    );
    assert.equal(client.db(config.mongoDatabase).databaseName, config.mongoDatabase);
    assert.equal(
      (
        await client
          .db(config.mongoDatabase)
          .listCollections({ name: 'products' }, { nameOnly: true })
          .toArray()
      ).length,
      1,
      'Initialize isolated test structures separately.',
    );
    await Promise.all([jobs.ready(deadline()), mongo.ready(deadline())]);
    safeToClean = true;
    /** Register a fresh cleanup identity before its first possible write. */
    function identity(): string {
      const id = `WIT-${randomUUID()}`;
      products.push(id);
      return id;
    }
    /** Insert SQL evidence at a fixed timestamp with a field that repairs must preserve. */
    async function putSql(value: Snapshot): Promise<void> {
      await pool.query(
        'INSERT INTO public.products(product_id,name,description,category,source_revision,applied_at,internal_note) VALUES($1,$2,$3,$4,$5,$6,$7)',
        [
          value.productId,
          value.product.name,
          value.product.description,
          value.product.category,
          value.revision,
          seedTime,
          'Sales-only note',
        ],
      );
    }
    /** Insert BSON-correct catalog evidence with both known and additional local fields. */
    async function putMongo(value: Snapshot): Promise<void> {
      await collection.insertOne({
        _id: value.productId,
        ...value.product,
        sourceRevision: new Int32(value.revision),
        appliedAt: seedTime,
        seoTitle: 'Catalog-only title',
        customLabel: 'Preserved metadata',
      });
    }
    /** Retain an immutable source event without importing or depending on API runtime code. */
    async function putUpdate(
      value: Snapshot,
      status: 'pending' | 'needs_attention' = 'pending',
    ): Promise<Job> {
      const jobId = randomUUID();
      await pool.query(
        "INSERT INTO public.reconciliation_jobs(job_id,kind,product_id,source_revision,approved_payload,status,next_attempt_at,last_error) VALUES($1,'update',$2,$3,$4,$5,CASE WHEN $5='pending' THEN clock_timestamp() ELSE NULL END,$6)",
        [
          jobId,
          value.productId,
          value.revision,
          value.product,
          status,
          status === 'needs_attention'
            ? { code: 'TEST_BLOCKED', message: 'Retained approval deliberately blocked.' }
            : null,
        ],
      );
      const job = await readUpdateJob(pool, jobId);
      assert.ok(job);
      return job;
    }
    /**
     * Model a future external component resetting a blocked test-owned update in one SQL statement.
     * @param previous - Last confirmed blocked state; immutable source columns must survive unchanged.
     * @returns After the atomic reset is acknowledged; the running worker may already have claimed it.
     * @throws If the record is unowned, no longer blocked, or any reset/immutability assertion fails.
     */
    async function externalRequeue(previous: Job): Promise<void> {
      assert.ok(
        products.includes(previous.productId) &&
          (previous.status === 'conflict' || previous.status === 'needs_attention'),
      );
      const reset = await pool.query<Record<string, unknown>>(
        "UPDATE public.reconciliation_jobs SET status='pending',attempt_count=0,next_attempt_at=clock_timestamp(),lease_token=NULL,lease_expires_at=NULL,completed_at=NULL,sql_outcome='unknown',mongo_outcome='unknown',last_error=NULL,conflict_details=NULL WHERE job_id=$1 AND product_id=$2 AND kind='update' AND status IN ('conflict','needs_attention') RETURNING *",
        [previous.jobId, previous.productId],
      );
      assert.equal(reset.rowCount, 1);
      const row = reset.rows[0];
      assert.ok(row);
      assert.equal(row.job_id, previous.jobId);
      assert.equal(row.kind, 'update');
      assert.equal(row.product_id, previous.productId);
      assert.equal(row.source_revision, previous.revision);
      assert.deepEqual(row.approved_payload, previous.approvedPayload);
      assert.ok(row.created_at instanceof Date);
      assert.equal(row.created_at.toISOString(), previous.createdAt);
      assert.equal(row.status, 'pending');
      assert.equal(row.attempt_count, 0);
      assert.ok(row.next_attempt_at instanceof Date);
      assert.equal(row.sql_outcome, 'unknown');
      assert.equal(row.mongo_outcome, 'unknown');
      for (const field of [
        'lease_token',
        'lease_expires_at',
        'completed_at',
        'last_error',
        'conflict_details',
      ])
        assert.equal(row[field], null);
    }
    /** Claim only an owned fixture, then compare the processor response with actual durable state. */
    async function run(
      jobId: string,
      overrides: Partial<ProcessorDependencies> = {},
    ): Promise<Job> {
      const claim = await jobs.claim(deadline(), jobId);
      assert.equal(claim.kind, 'claimed');
      if (claim.kind !== 'claimed') throw new Error('Fixture was not eligible.');
      const result = await processJob(claim.job, { ...dependencies, ...overrides }, deadline());
      const stored = await readUpdateJob(pool, jobId);
      assert.ok(stored);
      assert.equal(result.status, stored.status);
      return stored;
    }
    /** Inspect actual shared contents in both databases independently of durable job status. */
    async function equalCopies(value: Snapshot): Promise<void> {
      for (const store of [sql, mongo]) {
        const result = await store.read(value.productId, deadline());
        assert.equal(result.state, 'present');
        if (result.state !== 'present') throw new Error('Expected present product.');
        assert.deepEqual(result.snapshot, value);
      }
    }
    /** Read raw timestamps and local metadata that shared snapshot projections intentionally omit. */
    async function raw(
      productId: string,
    ): Promise<{ sql: Record<string, unknown>; mongo: Record<string, unknown> }> {
      const [sales, catalog] = await Promise.all([
        pool.query<Record<string, unknown>>('SELECT * FROM public.products WHERE product_id=$1', [
          productId,
        ]),
        collection.findOne({ _id: productId }),
      ]);
      assert.ok(sales.rows[0] && catalog);
      return { sql: sales.rows[0], mongo: catalog };
    }
    let conflictJob: Job | undefined, conflictedId: string | undefined;
    await t.test(
      'approved updates reconcile five matching or incomplete cases and expose one notebook conflict',
      /** Explicit source snapshots exercise familiar discrepancy shapes under fresh product identities. */ async () => {
        const cases = [
          {
            name: 'Ceramic mug',
            category: 'Home',
            approved: [3, '350 ml ceramic mug.'],
            sql: [3, '350 ml ceramic mug.'],
            mongo: [3, '350 ml ceramic mug.'],
          },
          {
            name: 'Cotton tote',
            category: 'Accessories',
            approved: [3, 'Reusable cotton shopping bag.'],
            sql: [3, 'Reusable cotton shopping bag.'],
            mongo: null,
          },
          {
            name: 'Desk lamp',
            category: 'Home',
            approved: [3, 'Adjustable LED desk lamp.'],
            sql: null,
            mongo: [3, 'Adjustable LED desk lamp.'],
          },
          {
            name: 'Running shoes',
            category: 'Footwear',
            approved: [5, 'Lightweight running shoes with recycled mesh.'],
            sql: [5, 'Lightweight running shoes with recycled mesh.'],
            mongo: [4, 'Lightweight running shoes.'],
          },
          {
            name: 'Water bottle',
            category: 'Sports',
            approved: [5, '750 ml stainless steel bottle.'],
            sql: [4, '500 ml stainless steel bottle.'],
            mongo: [5, '750 ml stainless steel bottle.'],
          },
          {
            name: 'Notebook',
            category: 'Stationery',
            approved: [6, 'A5 notebook with ruled pages.'],
            sql: [6, 'A5 notebook with ruled pages.'],
            mongo: [6, 'A5 notebook with blank pages.'],
          },
        ] as const;
        const ids: string[] = [],
          updates: Job[] = [];
        for (const entry of cases) {
          const id = identity();
          ids.push(id);
          // Each fixture records explicit approval before constructing either partial-delivery destination.
          updates.push(
            await putUpdate(
              snapshot(id, entry.approved[0], entry.name, entry.approved[1], entry.category),
            ),
          );
          if (entry.sql)
            await putSql(snapshot(id, entry.sql[0], entry.name, entry.sql[1], entry.category));
          if (entry.mongo)
            await putMongo(
              snapshot(id, entry.mongo[0], entry.name, entry.mongo[1], entry.category),
            );
        }
        const completed: Job[] = [];
        for (const update of updates) completed.push(await run(update.jobId));
        assert.deepEqual(
          completed.map(
            /** Inspect the durable result of each separately retained source event. */ (job) =>
              job.status,
          ),
          ['completed', 'completed', 'completed', 'completed', 'completed', 'conflict'],
        );
        for (let index = 0; index < 5; index += 1) {
          const entry = cases[index],
            id = ids[index];
          assert.ok(entry && id);
          await equalCopies(
            snapshot(id, entry.approved[0], entry.name, entry.approved[1], entry.category),
          );
          const state = await raw(id);
          assert.equal(state.sql.internal_note, entry.sql ? 'Sales-only note' : null);
          assert.equal(state.mongo.seoTitle, entry.mongo ? 'Catalog-only title' : null);
          if (entry.mongo) assert.equal(state.mongo.customLabel, 'Preserved metadata');
          if (index === 0) {
            assert.deepEqual(state.sql.applied_at, seedTime);
            assert.deepEqual(state.mongo.appliedAt, seedTime);
          }
          // A fixed past timestamp reveals no-op churn even if operations finish within one millisecond.
          await pool.query('UPDATE public.products SET applied_at=$2 WHERE product_id=$1', [
            id,
            seedTime,
          ]);
          await collection.updateOne({ _id: id }, { $set: { appliedAt: seedTime } });
          // An out-of-order approved event has a unique old revision; current retained approval supersedes it.
          const older = await putUpdate(
            snapshot(id, 1, entry.name, entry.approved[1], entry.category),
          );
          assert.equal((await run(older.jobId)).status, 'completed');
          const repeated = await raw(id);
          assert.deepEqual(repeated.sql.applied_at, seedTime);
          assert.deepEqual(repeated.mongo.appliedAt, seedTime);
        }
        conflictJob = completed[5];
        conflictedId = ids[5];
        assert.ok(conflictJob && conflictedId);
        assert.deepEqual(conflictJob.conflictDetails?.fields, ['description']);
        const state = await raw(conflictedId);
        assert.equal(state.sql.description, 'A5 notebook with ruled pages.');
        assert.equal(state.mongo.description, 'A5 notebook with blank pages.');
        assert.deepEqual(state.sql.applied_at, seedTime);
        assert.deepEqual(state.mongo.appliedAt, seedTime);
      },
    );
    await t.test(
      'higher approval resolves content and external requeue closes the original conflict update',
      /** A newer event does not silently close a previously blocked source event. */ async () => {
        assert.ok(conflictJob && conflictedId);
        const approved = snapshot(
          conflictedId,
          7,
          'Notebook',
          'A5 notebook with ruled pages.',
          'Stationery',
        );
        const update = await putUpdate(approved);
        assert.equal((await run(update.jobId)).status, 'completed');
        await equalCopies(approved);
        assert.equal((await readUpdateJob(pool, conflictJob.jobId))?.status, 'conflict');
        await externalRequeue(conflictJob);
        const retried = await readUpdateJob(pool, conflictJob.jobId);
        assert.ok(retried);
        assert.equal(retried.attemptCount, 0);
        assert.equal(retried.revision, 6);
        assert.deepEqual(retried.approvedPayload, conflictJob.approvedPayload);
        assert.equal((await run(retried.jobId)).status, 'completed');
      },
    );
    await t.test(
      'old update reconciles to newer current destination evidence',
      /** Merely superseding the original event is insufficient until both shared snapshots agree. */ async () => {
        const id = identity(),
          old = await putUpdate(snapshot(id, 1)),
          newest = snapshot(id, 6);
        await putSql(newest);
        await putMongo(snapshot(id, 5));
        const result = await run(old.jobId);
        assert.equal(result.status, 'completed');
        assert.equal(result.revision, 1);
        await equalCopies(newest);
      },
    );
    await t.test(
      'both missing copies use latest retained blocked approval',
      /** Mutable attention status never removes immutable source authority. */ async () => {
        const id = identity(),
          old = await putUpdate(snapshot(id, 1)),
          newest = snapshot(id, 7);
        const blocked = await putUpdate(newest, 'needs_attention');
        assert.equal((await run(old.jobId)).status, 'completed');
        await equalCopies(newest);
        assert.equal((await readUpdateJob(pool, blocked.jobId))?.status, 'needs_attention');
        const state = await raw(id);
        assert.equal(state.sql.internal_note, null);
        assert.equal(state.mongo.seoTitle, null);
      },
    );
    await t.test(
      'conditional repair preserves a newer concurrent write and refreshes both stores',
      /** Insert revision eight between Mongo revision-four observation and its expected-value write. */ async () => {
        const id = identity(),
          update = await putUpdate(snapshot(id, 5));
        await putSql(snapshot(id, 5));
        await putMongo(snapshot(id, 4));
        const newer = snapshot(id, 8);
        let raced = false;
        const racing: ProductStore = {
          ...mongo,
          /** Commit the source-assigned newer snapshot immediately before the first CAS repair. */
          async repair(value, observed, limit) {
            if (!raced) {
              raced = true;
              await collection.updateOne(
                { _id: id },
                { $set: { ...newer.product, sourceRevision: new Int32(8), appliedAt: new Date() } },
              );
            }
            return mongo.repair(value, observed, limit);
          },
        };
        assert.equal((await run(update.jobId, { mongo: racing })).status, 'completed');
        await equalCopies(newer);
        assert.ok(
          events.some(
            /** Confirm the real adapter observed zero matches rather than accidentally overwriting revision eight. */ (
              entry,
            ) => entry.fields.productId === id && entry.fields.outcome === 'race',
          ),
        );
        const state = await raw(id);
        assert.equal(state.sql.internal_note, 'Sales-only note');
        assert.equal(state.mongo.customLabel, 'Preserved metadata');
      },
    );
    await t.test(
      'transient read outage retries without treating uncertainty as absence',
      /** Simulate catalog read failure while the real durable job database remains available. */ async () => {
        const id = identity(),
          approved = snapshot(id, 2),
          update = await putUpdate(approved);
        const unavailable: ProductStore = {
          ...mongo,
          /** Report unknown transport outcome rather than a successful empty lookup. */
          async read() {
            return {
              state: 'unavailable',
              observedAt: new Date().toISOString(),
              diagnostic: { code: 'TEST_READ_OUTAGE', message: 'Injected catalog read outage.' },
            };
          },
        };
        const pending = await run(update.jobId, { mongo: unavailable });
        assert.equal(pending.status, 'pending');
        assert.equal(pending.outcomes.mongo, 'retry');
        assert.ok(pending.nextAttemptAt);
        assert.equal((await sql.read(id, deadline())).state, 'missing');
        assert.equal(await collection.countDocuments({ _id: id }), 0);
        await pool.query(
          "UPDATE public.reconciliation_jobs SET next_attempt_at=clock_timestamp() WHERE job_id=$1 AND product_id=$2 AND status='pending'",
          [update.jobId, id],
        );
        const recovered = await run(update.jobId);
        assert.equal(recovered.status, 'completed');
        assert.equal(recovered.attemptCount, 2);
        await equalCopies(approved);
      },
    );
    await t.test(
      'committed write with lost acknowledgment is verified before completion',
      /** Throw only after the real Mongo insert commits; fresh reads must establish its effect. */ async () => {
        const id = identity(),
          approved = snapshot(id, 3),
          update = await putUpdate(approved);
        let losses = 0;
        const uncertain: ProductStore = {
          ...mongo,
          /** Model one lost acknowledgment while retaining the destination's real write. */
          async repair(value, observed, limit) {
            await mongo.repair(value, observed, limit);
            losses += 1;
            throw new Error('Injected acknowledgment loss.');
          },
        };
        const result = await run(update.jobId, { mongo: uncertain });
        assert.equal(result.status, 'completed');
        assert.deepEqual(result.outcomes, { sql: 'satisfied', mongo: 'satisfied' });
        assert.equal(losses, 1);
        await equalCopies(approved);
        assert.equal(await collection.countDocuments({ _id: id }), 1);
        assert.equal(
          (await pool.query('SELECT product_id FROM public.products WHERE product_id=$1', [id]))
            .rowCount,
          1,
        );
      },
    );
    await t.test(
      'a lost finalization acknowledgment returns unknown while preserving the actual committed result',
      /** Throw after the fenced SQL update commits to distinguish an uncertain receipt from database rollback. */ async () => {
        const id = identity(),
          approved = snapshot(id, 2),
          update = await putUpdate(approved);
        const claim = await jobs.claim(deadline(), update.jobId);
        assert.equal(claim.kind, 'claimed');
        if (claim.kind !== 'claimed') throw new Error('Fixture was not claimable.');
        const result = await processJob(
          claim.job,
          {
            ...dependencies,
            jobs: {
              ...jobs,
              /** Hide the confirmed finalization response without issuing another update. */
              async finalize(jobId, token, proposal, limit) {
                await jobs.finalize(jobId, token, proposal, limit);
                throw new Error('Injected finalization acknowledgment loss.');
              },
            },
          },
          deadline(),
        );
        assert.equal(result.status, 'unknown');
        assert.equal(result.job, null);
        assert.equal((await readUpdateJob(pool, update.jobId))?.status, 'completed');
        assert.equal((await jobs.claim(deadline(), update.jobId)).kind, 'none');
        await equalCopies(approved);
      },
    );
    await t.test(
      'continuous service stays alive idle, handles new updates and external requeues, then stops on SIGTERM',
      /** One real process handles empty scans, later admissions, full external cycle resets, and bounded shutdown. */ async (
        continuousTest: TestContext,
      ) => {
        // A continuous scanner could later see scheduled retries, so protect every unfinished unowned update.
        const foreign = await pool.query(
          "SELECT job_id FROM public.reconciliation_jobs WHERE kind='update' AND status IN ('pending','processing') AND NOT(product_id=ANY($1::text[])) LIMIT 1",
          [products],
        );
        if (foreign.rowCount) {
          continuousTest.skip(
            'An unrelated unfinished test job prevents safely starting a broad continuous scanner.',
          );
          return;
        }
        const child = spawn(
          process.execPath,
          [fileURLToPath(new URL('../../src/main.js', import.meta.url))],
          {
            env: {
              ...process.env,
              POSTGRES_URL: config.postgresUrl,
              MONGODB_URI: config.mongoUri,
              MONGODB_DATABASE: config.mongoDatabase,
              LOG_LEVEL: 'info',
            },
            stdio: ['ignore', 'ignore', 'pipe'],
          },
        );
        children.add(child);
        const records: Record<string, unknown>[] = [],
          observers = new Set<() => void>();
        let buffer = '',
          ended = false,
          invalidLog = false;
        const lifetime = setTimeout(
          /** Bound both work and driver shutdown; forced termination must fail the exit assertion. */ () =>
            child.kill('SIGKILL'),
          90_000,
        );
        const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
          /** Install termination observation before processing any listener event. */ (
            resolve,
          ) => {
            child.once(
              'close',
              /** Release ownership only after process exit and final stderr delivery. */ (
                code,
                signal,
              ) => {
                ended = true;
                clearTimeout(lifetime);
                children.delete(child);
                for (const notify of observers) notify();
                resolve({ code, signal });
              },
            );
            child.once(
              'error',
              /** Turn launch failure into a bounded, sanitized failed exit. */ () => {
                ended = true;
                clearTimeout(lifetime);
                children.delete(child);
                for (const notify of observers) notify();
                resolve({ code: 1, signal: null });
              },
            );
          },
        );
        child.stderr?.on(
          'data',
          /** Reassemble structured log records; event content is never echoed or treated as instructions. */ (
            chunk: Buffer,
          ) => {
            buffer += chunk.toString();
            let newline = buffer.indexOf('\n');
            while (newline >= 0) {
              const line = buffer.slice(0, newline);
              buffer = buffer.slice(newline + 1);
              if (line.trim()) {
                try {
                  const value: unknown = JSON.parse(line);
                  if (typeof value === 'object' && value !== null && !Array.isArray(value))
                    records.push(value as Record<string, unknown>);
                  else invalidLog = true;
                } catch {
                  invalidLog = true;
                }
              }
              newline = buffer.indexOf('\n');
            }
            for (const notify of observers) notify();
          },
        );
        /** Wait for a retained or future lifecycle event; only fallback-scan recovery extends the normal deadline. */
        async function event(
          name: string,
          jobId?: string,
          after = 0,
          target?: 'postgresql' | 'mongodb',
          timeoutMs = 10_000,
        ): Promise<Record<string, unknown>> {
          return new Promise(
            /** Observe buffered and subsequent records with a finite deadline and exit detection. */ (
              resolve,
              reject,
            ) => {
              const timer = setTimeout(
                /** Fail the barrier without claiming the child stopped; finally owns termination. */ () => {
                  observers.delete(check);
                  reject(new Error(`Continuous worker did not emit ${name}.`));
                },
                timeoutMs,
              );
              /** Resolve the matching event or reject a malformed log or premature process exit. */
              function check(): void {
                const found = records
                  .slice(after)
                  .find(
                    /** Match a fresh event without assuming log timing relative to the calling query. */ (
                      record,
                    ) =>
                      record.event === name &&
                      (jobId === undefined || record.jobId === jobId) &&
                      (target === undefined || record.target === target),
                  );
                if (found || invalidLog || ended) {
                  clearTimeout(timer);
                  observers.delete(check);
                  if (found) resolve(found);
                  else
                    reject(
                      new Error(
                        'Continuous worker stopped or emitted malformed logs before the expected event.',
                      ),
                    );
                }
              }
              observers.add(check);
              check();
            },
          );
        }
        /** Verify successful startup probes remain cached across every subsequent lifecycle event. */
        function assertValidatedOnce(): void {
          const validations = records.filter(
            /** Select confirmed compatibility checks rather than general dependency operations. */ (
              record,
            ) => record.event === 'database_validated',
          );
          assert.deepEqual(
            validations
              .map(
                /** Database completion order is independent, so compare only the target set. */ (
                  record,
                ) => record.target,
              )
              .sort(),
            ['mongodb', 'postgresql'],
          );
          for (const validation of validations) assert.equal(validation.outcome, 'ready');
        }
        try {
          await event('listener_subscribed');
          await event('database_validated', undefined, 0, 'postgresql');
          await event('database_validated', undefined, 0, 'mongodb');
          assertValidatedOnce();
          // Span the fixed thirty-second scan period to prove an empty queue does not terminate the service.
          await delay(30_200);
          assertValidatedOnce();
          assert.equal(ended, false);
          assert.equal(child.exitCode, null);
          assert.equal(child.signalCode, null);
          assert.equal(
            records.some(
              /** No work exists at startup, so no attempt should have been claimed. */ (record) =>
                record.event === 'job_claimed',
            ),
            false,
          );
          const incomingProduct = snapshot(identity(), 3),
            incoming = await putUpdate(incomingProduct);
          await event('job_completed', incoming.jobId);
          const subscribed = records.findIndex(
            /** Locate the acknowledged listener subscription. */ (record) =>
              record.event === 'listener_subscribed',
          );
          const claimed = records.findIndex(
            /** Locate the first admitted update's real claim. */ (record) =>
              record.event === 'job_claimed' && record.jobId === incoming.jobId,
          );
          assert.ok(subscribed >= 0 && claimed > subscribed);
          for (const target of ['postgresql', 'mongodb']) {
            const validated = records.findIndex(
              /** Both successful compatibility probes must precede the first claimed job. */ (
                record,
              ) => record.event === 'database_validated' && record.target === target,
            );
            assert.ok(validated >= 0 && validated < claimed);
          }
          assert.equal((await readUpdateJob(pool, incoming.jobId))?.status, 'completed');
          await equalCopies(incomingProduct);

          const conflictingId = identity(),
            original = snapshot(conflictingId, 3, 'Notebook', 'Ruled pages');
          await putSql(original);
          await putMongo(snapshot(conflictingId, 3, 'Notebook', 'Blank pages'));
          const conflicting = await putUpdate(original);
          await event('job_conflict', conflicting.jobId);
          const blocked = await readUpdateJob(pool, conflicting.jobId);
          assert.equal(blocked?.status, 'conflict');
          assert.ok(blocked);
          const resolvedSnapshot = snapshot(conflictingId, 4, 'Notebook', 'Approved ruled pages');
          const resolving = await putUpdate(resolvedSnapshot);
          await event('job_completed', resolving.jobId);
          assert.equal((await readUpdateJob(pool, conflicting.jobId))?.status, 'conflict');
          await externalRequeue(blocked);
          await event('job_completed', conflicting.jobId);
          const resumedConflict = await readUpdateJob(pool, conflicting.jobId);
          assert.equal(resumedConflict?.status, 'completed');
          assert.equal(resumedConflict?.attemptCount, 1);
          assert.equal(resumedConflict?.revision, 3);
          assert.deepEqual(resumedConflict?.approvedPayload, original.product);
          await equalCopies(resolvedSnapshot);

          const exhaustedProduct = snapshot(identity(), 2),
            exhaustedId = randomUUID();
          // Processing insertion emits no ready notification; the periodic scan must recover this expired eighth lease.
          await pool.query(
            "INSERT INTO public.reconciliation_jobs(job_id,kind,product_id,source_revision,approved_payload,status,attempt_count,next_attempt_at,lease_token,lease_expires_at) VALUES($1,'update',$2,$3,$4,'processing',8,NULL,$5,clock_timestamp()-interval '1 second')",
            [
              exhaustedId,
              exhaustedProduct.productId,
              exhaustedProduct.revision,
              exhaustedProduct.product,
              randomUUID(),
            ],
          );
          await event('job_retry_exhausted', exhaustedId, 0, undefined, 40_000);
          const exhausted = await readUpdateJob(pool, exhaustedId);
          assert.equal(exhausted?.status, 'needs_attention');
          assert.equal(exhausted?.attemptCount, 8);
          assert.equal(exhausted?.lastError?.code, 'RETRY_EXHAUSTED');
          assert.equal((await sql.read(exhaustedProduct.productId, deadline())).state, 'missing');
          assert.equal(await collection.countDocuments({ _id: exhaustedProduct.productId }), 0);
          const afterFirstExhaustion = records.length;
          // Deliberately incomplete external intervention must not permit a ninth automatic attempt.
          await pool.query(
            "UPDATE public.reconciliation_jobs SET status='pending',next_attempt_at=clock_timestamp() WHERE job_id=$1 AND product_id=$2 AND kind='update' AND status='needs_attention'",
            [exhaustedId, exhaustedProduct.productId],
          );
          await event('job_retry_exhausted', exhaustedId, afterFirstExhaustion);
          const stillExhausted = await readUpdateJob(pool, exhaustedId);
          assert.ok(stillExhausted);
          assert.equal(stillExhausted.status, 'needs_attention');
          assert.equal(stillExhausted.attemptCount, 8);
          assert.equal(
            records.some(
              /** Exhaustion transitions must never execute product processing or start attempt nine. */ (
                record,
              ) => record.event === 'job_claimed' && record.jobId === exhaustedId,
            ),
            false,
          );
          await externalRequeue(stillExhausted);
          await event('job_completed', exhaustedId);
          const recovered = await readUpdateJob(pool, exhaustedId);
          assert.equal(recovered?.status, 'completed');
          assert.equal(recovered?.attemptCount, 1);
          await equalCopies(exhaustedProduct);
          child.kill('SIGTERM');
          assert.equal((await event('shutdown_finished')).outcome, 'closed');
          assert.deepEqual(await exited, { code: 0, signal: null });
          // Final stderr is drained: polling, notifications, external requeues and shutdown ran no new probes.
          assertValidatedOnce();
          const stopped = records.findIndex(
            /** The signal handler logs before it disables future claims. */ (record) =>
              record.event === 'shutdown_started',
          );
          assert.ok(stopped >= 0);
          assert.equal(
            records
              .slice(stopped + 1)
              .some(
                /** No attempt may begin after shutdown starts. */ (record) =>
                  record.event === 'job_claimed',
              ),
            false,
          );
          const afterExit = await putUpdate(snapshot(identity(), 1));
          assert.equal(afterExit.status, 'pending');
          assert.equal(afterExit.attemptCount, 0);
        } finally {
          if (!ended) child.kill('SIGKILL');
          await exited;
        }
      },
    );
    await t.test(
      'fresh child recovers a killed worker after one acknowledged destination commit',
      /** Real SIGKILL at the SQL barrier plus owned lease expiry deterministically stages process loss. */ async () => {
        const id = identity(),
          approved = snapshot(id, 4),
          update = await putUpdate(approved);
        const interrupted = startChild('crash', update.jobId, children);
        assert.equal((await interrupted.reached).event, 'sql_committed');
        const processing = await readUpdateJob(pool, update.jobId);
        assert.equal(processing?.status, 'processing');
        assert.equal(processing?.attemptCount, 1);
        assert.equal((await sql.read(id, deadline())).state, 'present');
        assert.equal(await collection.countDocuments({ _id: id }), 0);
        const sqlBefore = (
          await pool.query<{ applied_at: Date }>(
            'SELECT applied_at FROM public.products WHERE product_id=$1',
            [id],
          )
        ).rows[0]?.applied_at;
        assert.ok(sqlBefore);
        interrupted.child.kill('SIGKILL');
        assert.equal((await interrupted.exited).signal, 'SIGKILL');
        // Expire only the registered fixture's lease; unrelated jobs and application records remain unchanged.
        await pool.query(
          "UPDATE public.reconciliation_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1 AND product_id=$2 AND status='processing'",
          [update.jobId, id],
        );
        const resumed = startChild('resume', update.jobId, children);
        assert.equal((await resumed.reached).status, 'completed');
        assert.equal((await resumed.exited).code, 0);
        const completed = await readUpdateJob(pool, update.jobId);
        assert.equal(completed?.status, 'completed');
        assert.equal(completed?.attemptCount, 2);
        await equalCopies(approved);
        const state = await raw(id);
        assert.deepEqual(state.sql.applied_at, sqlBefore);
        assert.equal(await collection.countDocuments({ _id: id }), 1);
        assert.equal(
          (await pool.query('SELECT product_id FROM public.products WHERE product_id=$1', [id]))
            .rowCount,
          1,
        );
      },
    );
  },
);
