/**
 * @file Verifies admission, revision fencing, and diagnostic routes against real databases.
 * Requires dedicated, externally initialized test targets and skips when all settings are absent.
 * Fixture IDs are unique to this invocation; cleanup removes only those owned records
 * after checking the connected targets, and the suite never creates or drops structures.
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';
import type { TestContext } from 'node:test';
import { MongoClient } from 'mongodb';
import { Pool } from 'pg';
import { buildApp } from '../../src/app.js';
import { createJobRepository } from '../../src/jobs.js';
import { createMongoDestination, createSqlDestination } from '../../src/products.js';
import type {
  ApprovedUpdate,
  AttemptResult,
  ProductDestination,
  Snapshot,
} from '../../src/model.js';

/** Explicit connection targets that passed the suite's database-name and URI override guards. */
type TestConfiguration = {
  postgresUrl: string;
  mongoUri: string;
  mongoDatabase: string;
  postgresDatabase: string;
};

/**
 * Parse connection syntax while keeping the raw credential-bearing value out of errors.
 * @param value - Test connection URI to parse; protocol and target checks occur separately.
 * @returns The parsed URL for subsequent safety validation.
 * @throws A sanitized error when URL parsing fails.
 */
function connectionUri(value: string): URL {
  try {
    return new URL(value);
  } catch {
    throw new Error('A TEST_* connection URI is invalid; its value is omitted.');
  }
}

/**
 * Validate isolated database targets before constructing any driver connection.
 * URI overrides are rejected because they could bypass the validated database names.
 * @param env - Environment containing all three TEST_* connection settings or none of them.
 * @returns Validated target settings, or null when no integration configuration is supplied.
 * @throws If settings are incomplete, malformed, contradictory, or name a non-test database.
 */
function configuration(env: NodeJS.ProcessEnv): TestConfiguration | null {
  const postgresUrl = env.TEST_POSTGRES_URL,
    mongoUri = env.TEST_MONGODB_URI,
    mongoDatabase = env.TEST_MONGODB_DATABASE;
  if (!postgresUrl && !mongoUri && !mongoDatabase) return null;
  assert.ok(
    postgresUrl && mongoUri && mongoDatabase,
    'All three TEST_* connection variables are required together.',
  );
  const postgres = connectionUri(postgresUrl);
  assert.ok(
    ['postgres:', 'postgresql:'].includes(postgres.protocol),
    'Test PostgreSQL requires an explicit URI.',
  );
  assert.ok(
    postgres.hostname && !postgres.hash,
    'Test PostgreSQL requires a host and no URI fragment.',
  );
  const postgresDatabase = decodeURIComponent(postgres.pathname.slice(1));
  assert.match(
    postgresDatabase,
    /^update_api_test_[a-z0-9_]+$/,
    'Refusing a non-test PostgreSQL database.',
  );
  for (const key of postgres.searchParams.keys()) {
    assert.ok(
      !['db', 'dbname', 'database', 'connectionstring', 'options'].includes(key.toLowerCase()),
      'Database/session URI overrides are prohibited in integration tests.',
    );
  }
  assert.match(
    mongoDatabase,
    /^update_api_test_[a-z0-9_]+$/,
    'Refusing a non-test MongoDB database.',
  );
  const mongo = connectionUri(mongoUri);
  assert.ok(
    ['mongodb:', 'mongodb+srv:'].includes(mongo.protocol),
    'Test MongoDB requires an explicit URI.',
  );
  const mongoPath = decodeURIComponent(mongo.pathname.slice(1));
  assert.ok(
    !mongoPath || mongoPath === mongoDatabase,
    'MongoDB URI path must be empty or match TEST_MONGODB_DATABASE.',
  );
  for (const key of mongo.searchParams.keys()) {
    assert.ok(
      !['db', 'dbname', 'database', 'connectionstring'].includes(key.toLowerCase()),
      'MongoDB database URI overrides are prohibited.',
    );
  }
  return { postgresUrl, postgresDatabase, mongoUri, mongoDatabase };
}

/**
 * Give each operation a fresh budget instead of reusing time consumed by fixture setup.
 * @returns An absolute epoch-millisecond deadline fifteen seconds from the current time.
 */
function deadline(): number {
  return Date.now() + 15000;
}

/**
 * Create a complete approved event and register its unique product before any writes.
 * @param products - Mutable cleanup registry owned by this suite invocation.
 * @param revision - Source revision for the scenario; defaults to the first revision.
 * @returns An event with fresh event and product IDs plus deterministic managed fields.
 */
function event(products: string[], revision = 1): ApprovedUpdate {
  const productId = `UIT-${randomUUID()}`;
  products.push(productId);
  return {
    eventId: randomUUID(),
    productId,
    revision,
    product: {
      name: 'Integration product',
      description: 'Immutable test snapshot.',
      category: 'Tests',
    },
  };
}

/**
 * Build another approved version using an explicit source revision supplied by the test.
 * @param update - Existing product identity and managed-field fixture; it is not mutated.
 * @param revision - Scenario's source-assigned revision, independent of database observations.
 * @returns A complete snapshot with a revision-specific name and no event identity.
 */
function version(update: Snapshot, revision: number): Snapshot {
  return {
    productId: update.productId,
    revision,
    product: { ...update.product, name: `Approved revision ${revision}` },
  };
}

/**
 * Inject a product-delivery outage while keeping the destination's real observation methods.
 * @param destination - Existing adapter whose reads, readiness, and lifecycle remain available.
 * @returns A shallow adapter wrapper whose delivery reports retryable unavailability.
 */
function failing(destination: ProductDestination): ProductDestination {
  return {
    ...destination,
    /** Simulate unavailable delivery independently from the healthy job database. */ async deliver() {
      return {
        outcome: 'retry',
        action: 'unavailable',
        diagnostic: { code: 'TEST_UNAVAILABLE', message: 'Injected destination outage.' },
      };
    },
  };
}

/**
 * Build the result a stale attempt would try to record in lease-fencing tests.
 * @returns A completed attempt with satisfied targets; this creates no destination writes.
 */
function completed(): AttemptResult {
  return {
    status: 'completed',
    sql: { outcome: 'satisfied', action: 'unchanged' },
    mongo: { outcome: 'satisfied', action: 'unchanged' },
    retryDelayMs: 0,
  };
}

test('integration safety rejects application names and URI database overrides', /** Exercise all guards without connecting. */ () => {
  const safe = {
    TEST_POSTGRES_URL: 'postgresql://localhost/update_api_test_safe',
    TEST_MONGODB_URI: 'mongodb://localhost',
    TEST_MONGODB_DATABASE: 'update_api_test_safe',
  };
  assert.ok(configuration(safe));
  assert.equal(configuration({}), null);
  assert.throws(
    /** Refuse incomplete configuration rather than silently running another target. */ () =>
      configuration({ TEST_POSTGRES_URL: safe.TEST_POSTGRES_URL }),
  );
  assert.throws(
    /** Protect the populated PostgreSQL application. */ () =>
      configuration({ ...safe, TEST_POSTGRES_URL: 'postgresql://localhost/myapp' }),
  );
  assert.throws(
    /** Protect the populated MongoDB application. */ () =>
      configuration({ ...safe, TEST_MONGODB_DATABASE: 'retail_catalog' }),
  );
  assert.throws(
    /** Block pg-connection-string query precedence from changing the checked target. */ () =>
      configuration({ ...safe, TEST_POSTGRES_URL: `${safe.TEST_POSTGRES_URL}?database=myapp` }),
  );
  assert.throws(
    /** Block session settings from changing schema lookup. */ () =>
      configuration({ ...safe, TEST_POSTGRES_URL: `${safe.TEST_POSTGRES_URL}?options=anything` }),
  );
  assert.throws(
    /** Reject a contradictory MongoDB URI target. */ () =>
      configuration({ ...safe, TEST_MONGODB_URI: 'mongodb://localhost/retail_catalog' }),
  );
});

const config = configuration(process.env);
test(
  'real PostgreSQL and MongoDB API invariants',
  {
    skip:
      config === null
        ? 'Set all TEST_* variables to externally initialized update_api_test_* databases.'
        : false,
  },
  /** Use isolated records and real drivers; never create, seed, truncate, or drop database structures. */ async (
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
    const products: string[] = [];
    const collection = client
      .db(config.mongoDatabase)
      .collection<{ _id: string; [key: string]: unknown }>('products');
    let safeToClean = false;
    t.after(
      /** Remove only this invocation's registered fixture IDs and close both clients even after assertion failures. */ async () => {
        try {
          if (safeToClean && products.length) {
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
    // Verify the actual PostgreSQL target and existing Mongo collection before enabling any fixture write.
    assert.equal(
      (await pool.query<{ name: string }>('SELECT current_database() AS name')).rows[0]?.name,
      config.postgresDatabase,
    );
    const collections = await client
      .db(config.mongoDatabase)
      .listCollections({ name: 'products' }, { nameOnly: true })
      .toArray();
    assert.equal(collections.length, 1, 'Externally initialize the test MongoDB collection first.');
    const jobs = createJobRepository(pool),
      sql = createSqlDestination(pool),
      mongo = createMongoDestination(client, config.mongoDatabase);
    assert.deepEqual(
      await jobs.ready(deadline()),
      { available: true },
      'Externally initialize the dedicated PostgreSQL schema first.',
    );
    assert.deepEqual(await sql.ready(deadline()), { available: true });
    safeToClean = true;

    await t.test(
      'concurrent identical HTTP submissions admit one job and start one delivery per target',
      /** Race independent transactions; durable uniqueness must select a single initial dispatcher. */ async () => {
        const update = event(products);
        let sqlCalls = 0,
          mongoCalls = 0;
        const countedSql = {
          ...sql,
          /** Count confirmed initial dispatch and use the real SQL adapter. */ async deliver(
            snapshot: Snapshot,
            limit: number,
          ) {
            sqlCalls += 1;
            return sql.deliver(snapshot, limit);
          },
        };
        const countedMongo = {
          ...mongo,
          /** Count confirmed initial dispatch and use the real Mongo adapter. */ async deliver(
            snapshot: Snapshot,
            limit: number,
          ) {
            mongoCalls += 1;
            return mongo.deliver(snapshot, limit);
          },
        };
        const app = buildApp({ jobs, sql: countedSql, mongo: countedMongo, logger: false });
        try {
          const responses = await Promise.all(
            Array.from(
              { length: 8 },
              /** Submit the same immutable event concurrently. */ () =>
                app.inject({ method: 'POST', url: '/approved-updates', payload: update }),
            ),
          );
          assert.ok(
            responses.every(
              /** Every submission must be durably accepted. */ (response) =>
                [200, 202].includes(response.statusCode),
            ),
          );
          assert.equal(
            responses.filter(
              /** Exactly one transaction owns initial delivery. */ (response) =>
                response.json<{ duplicate: boolean }>().duplicate === false,
            ).length,
            1,
          );
          assert.equal(sqlCalls, 1);
          assert.equal(mongoCalls, 1);
          const saved = await jobs.get(update.eventId, deadline());
          assert.equal(saved?.job.status, 'completed');
          assert.equal(saved?.job.attemptCount, 1);
          assert.equal(
            (
              await pool.query(
                'SELECT job_id FROM public.reconciliation_jobs WHERE product_id=$1',
                [update.productId],
              )
            ).rowCount,
            1,
          );
          assert.equal(await collection.countDocuments({ _id: update.productId }), 1);
        } finally {
          await app.close();
        }
      },
    );

    await t.test(
      'event, revision, and audit identity collisions are distinct from duplicates',
      /** A conflict cannot repurpose immutable history or invoke another delivery attempt. */ async () => {
        const update = event(products),
          app = buildApp({ jobs, sql, mongo, logger: false });
        try {
          assert.equal(
            (await app.inject({ method: 'POST', url: '/approved-updates', payload: update }))
              .statusCode,
            200,
          );
          const changed = await app.inject({
            method: 'POST',
            url: '/approved-updates',
            payload: { ...update, product: { ...update.product, name: 'Collision' } },
          });
          assert.equal(changed.statusCode, 409);
          assert.equal(changed.json().error.code, 'EVENT_ID_COLLISION');
          const revision = await app.inject({
            method: 'POST',
            url: '/approved-updates',
            payload: { ...update, eventId: randomUUID() },
          });
          assert.equal(revision.statusCode, 409);
          assert.equal(revision.json().error.code, 'REVISION_ALREADY_REGISTERED');
          assert.equal(revision.json().canonicalJobId, update.eventId);
          const audit = event(products);
          await pool.query(
            "INSERT INTO public.reconciliation_jobs(job_id,kind,product_id) VALUES($1,'audit',$2)",
            [audit.eventId, audit.productId],
          );
          const collision = await app.inject({
            method: 'POST',
            url: '/approved-updates',
            payload: audit,
          });
          assert.equal(collision.statusCode, 409);
          assert.equal(collision.json().error.code, 'EVENT_ID_COLLISION');
        } finally {
          await app.close();
        }
      },
    );

    await t.test(
      'older admission delivers the highest retained approved snapshot without changing either event',
      /** Retained processing history is authoritative even when both products are still absent. */ async () => {
        const newest = event(products, 9),
          first = await jobs.admit(newest, deadline());
        assert.equal(first.kind, 'accepted');
        const older = {
          ...newest,
          eventId: randomUUID(),
          revision: 3,
          product: { ...newest.product, name: 'Old approved product' },
        };
        const app = buildApp({ jobs, sql, mongo, logger: false });
        try {
          const response = await app.inject({
            method: 'POST',
            url: '/approved-updates',
            payload: older,
          });
          assert.equal(response.statusCode, 200);
          assert.equal(response.json().revision, 3);
          for (const destination of [sql, mongo]) {
            const observed = await destination.read(newest.productId, deadline());
            assert.equal(observed.state, 'present');
            if (observed.state === 'present')
              assert.deepEqual(observed.snapshot, {
                productId: newest.productId,
                revision: 9,
                product: newest.product,
              });
          }
          assert.equal(
            (await jobs.get(older.eventId, deadline()))?.job.approvedPayload?.name,
            'Old approved product',
          );
          assert.equal((await jobs.get(newest.eventId, deadline()))?.job.status, 'processing');
        } finally {
          await app.close();
        }
      },
    );

    await t.test(
      'atomic revision writes preserve local fields, no-op timestamps, and newer data',
      /** Exercise both actual storage validators and conditional update predicates. */ async () => {
        const update = event(products),
          newer = version(update, 2);
        assert.equal((await sql.deliver(update, deadline())).action, 'inserted');
        assert.equal((await mongo.deliver(update, deadline())).action, 'inserted');
        await pool.query('UPDATE public.products SET internal_note=$2 WHERE product_id=$1', [
          update.productId,
          'Sales only',
        ]);
        await collection.updateOne(
          { _id: update.productId },
          { $set: { seoTitle: 'Catalog only', privateCatalogField: { preserve: true } } },
        );
        for (const destination of [sql, mongo]) {
          assert.equal((await destination.deliver(newer, deadline())).action, 'updated');
          const before = await destination.read(update.productId, deadline());
          assert.equal((await destination.deliver(newer, deadline())).action, 'unchanged');
          assert.equal((await destination.deliver(update, deadline())).action, 'superseded');
          assert.equal(
            (
              await destination.deliver(
                { ...newer, product: { ...newer.product, name: 'Disagrees' } },
                deadline(),
              )
            ).outcome,
            'conflict',
          );
          const after = await destination.read(update.productId, deadline());
          assert.equal(before.state, 'present');
          assert.equal(after.state, 'present');
          if (before.state === 'present' && after.state === 'present')
            assert.deepEqual(after.stored, before.stored);
        }
        assert.equal(
          (
            await pool.query('SELECT internal_note FROM public.products WHERE product_id=$1', [
              update.productId,
            ])
          ).rows[0]?.internal_note,
          'Sales only',
        );
        const stored = await collection.findOne({ _id: update.productId });
        assert.equal(stored?.seoTitle, 'Catalog only');
        assert.deepEqual(stored?.privateCatalogField, { preserve: true });
      },
    );

    await t.test(
      'concurrent destination writers never downgrade the highest approved revision',
      /** Real competing inserts/CAS updates must converge to the largest submitted revision. */ async () => {
        const update = event(products);
        const writes: Promise<unknown>[] = [];
        for (const revision of [2, 7])
          for (const destination of [sql, mongo])
            writes.push(destination.deliver(version(update, revision), deadline()));
        await Promise.all(writes);
        for (const destination of [sql, mongo]) {
          const observed = await destination.read(update.productId, deadline());
          assert.equal(observed.state, 'present');
          if (observed.state === 'present') assert.deepEqual(observed.snapshot, version(update, 7));
        }
      },
    );

    await t.test(
      'equal revision disagreement records conflict and preserves both destination observations',
      /** Keep conflicting business evidence intact while recording each target's independent outcome. */ async () => {
        const update = event(products),
          different = {
            productId: update.productId,
            revision: update.revision,
            product: { ...update.product, description: 'Conflicting approved-revision content.' },
          };
        await sql.deliver(update, deadline());
        await mongo.deliver(different, deadline());
        const app = buildApp({ jobs, sql, mongo, logger: false });
        try {
          const response = await app.inject({
            method: 'POST',
            url: '/approved-updates',
            payload: update,
          });
          assert.equal(response.statusCode, 202);
          assert.equal(response.json().status, 'conflict');
          const saved = await jobs.get(update.eventId, deadline());
          assert.deepEqual(saved?.job.outcomes, { sql: 'satisfied', mongo: 'conflict' });
          assert.ok(saved?.job.conflictDetails);
          assert.equal(saved?.job.nextAttemptAt, null);
          assert.equal(saved?.job.leaseExpiresAt, null);
          const observed = await mongo.read(update.productId, deadline());
          assert.equal(observed.state, 'present');
          if (observed.state === 'present') assert.deepEqual(observed.snapshot, different);
          assert.equal(
            (await app.inject(`/products/${update.productId}/state`)).json().comparison,
            'different',
          );
        } finally {
          await app.close();
        }
      },
    );

    await t.test(
      'partial delivery persists retry state and identical replay does not retry or repair',
      /** Simulate one product adapter outage while retaining actual durable admission and the other store. */ async () => {
        for (const failedTarget of ['sql', 'mongo'] as const) {
          const update = event(products);
          const app = buildApp({
            jobs,
            sql: failedTarget === 'sql' ? failing(sql) : sql,
            mongo: failedTarget === 'mongo' ? failing(mongo) : mongo,
            logger: false,
          });
          try {
            const response = await app.inject({
              method: 'POST',
              url: '/approved-updates',
              payload: update,
            });
            assert.equal(response.statusCode, 202);
            assert.equal(response.json().status, 'pending');
          } finally {
            await app.close();
          }
          const before = await jobs.get(update.eventId, deadline());
          assert.equal(before?.job.outcomes[failedTarget], 'retry');
          assert.ok(before?.job.nextAttemptAt);
          assert.equal(before?.job.leaseExpiresAt, null);
          // Rebuilding an API instance is an API restart, not worker recovery.
          const restarted = buildApp({ jobs, sql, mongo, logger: false });
          try {
            const duplicate = await restarted.inject({
              method: 'POST',
              url: '/approved-updates',
              payload: update,
            });
            assert.equal(duplicate.statusCode, 202);
            assert.equal(duplicate.json().duplicate, true);
            assert.deepEqual((await jobs.get(update.eventId, deadline()))?.job, before?.job);
            assert.equal(
              (await (failedTarget === 'sql' ? sql : mongo).read(update.productId, deadline()))
                .state,
              'missing',
            );
          } finally {
            await restarted.close();
          }
        }
      },
    );

    await t.test(
      'expired and replaced leases reject stale finalization',
      /** Database time and ownership tokens fence late bookkeeping independently of destination writes. */ async () => {
        for (const mode of ['expired', 'replaced'] as const) {
          const update = event(products),
            admitted = await jobs.admit(update, deadline());
          assert.equal(admitted.kind, 'accepted');
          if (admitted.kind !== 'accepted') throw new Error('Admission failed.');
          if (mode === 'expired')
            await pool.query(
              "UPDATE public.reconciliation_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1",
              [update.eventId],
            );
          else
            await pool.query(
              'UPDATE public.reconciliation_jobs SET lease_token=$2 WHERE job_id=$1',
              [update.eventId, randomUUID()],
            );
          const before = await jobs.get(update.eventId, deadline());
          assert.equal(
            await jobs.finalize(update.eventId, admitted.leaseToken, completed(), deadline()),
            null,
          );
          assert.deepEqual((await jobs.get(update.eventId, deadline()))?.job, before?.job);
        }
      },
    );

    await t.test(
      'lease expiry while finalization waits for a row lock cannot record a stale completion',
      /** Hold an unchanged row until database time passes expiry; the final predicate must run after the lock. */ async () => {
        const update = event(products),
          admitted = await jobs.admit(update, deadline());
        assert.equal(admitted.kind, 'accepted');
        if (admitted.kind !== 'accepted') throw new Error('Admission failed.');
        await pool.query(
          "UPDATE public.reconciliation_jobs SET lease_expires_at=clock_timestamp()+interval '900 milliseconds' WHERE job_id=$1",
          [update.eventId],
        );
        const lock = await pool.connect();
        let finalizing: ReturnType<typeof jobs.finalize> | undefined;
        try {
          await lock.query('BEGIN');
          await lock.query(
            'SELECT job_id FROM public.reconciliation_jobs WHERE job_id=$1 FOR UPDATE',
            [update.eventId],
          );
          finalizing = jobs.finalize(
            update.eventId,
            admitted.leaseToken,
            completed(),
            Date.now() + 2500,
          );
          let waiting = false;
          for (let count = 0; count < 40; count += 1) {
            const state = await pool.query<{ waiting: boolean }>(
              "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%WITH locked AS MATERIALIZED%') AS waiting",
            );
            if (state.rows[0]?.waiting) {
              waiting = true;
              break;
            }
            await delay(10);
          }
          assert.equal(waiting, true, 'Finalization must be demonstrably blocked on the held row.');
          await lock.query(
            "SELECT pg_sleep_until(lease_expires_at+interval '50 milliseconds') FROM public.reconciliation_jobs WHERE job_id=$1",
            [update.eventId],
          );
          await lock.query('COMMIT');
          assert.equal(await finalizing, null);
          assert.equal((await jobs.get(update.eventId, deadline()))?.job.status, 'processing');
        } finally {
          try {
            await lock.query('ROLLBACK');
            // Drain the contender even when a barrier assertion fails, before suite cleanup closes its pool.
            if (finalizing) await finalizing;
          } finally {
            lock.release();
          }
        }
      },
    );

    await t.test(
      'diagnostic and health routes leave products and job state unchanged and hide lease tokens',
      /** Snapshot exact rows around observations to detect timestamp churn or implicit worker work. */ async () => {
        const update = event(products),
          admitted = await jobs.admit(update, deadline());
        assert.equal(admitted.kind, 'accepted');
        await sql.deliver(update, deadline());
        const app = buildApp({ jobs, sql, mongo, logger: false });
        try {
          const before = await pool.query(
            'SELECT row_to_json(j) AS row FROM public.reconciliation_jobs j WHERE job_id=$1',
            [update.eventId],
          );
          const productBefore = await sql.read(update.productId, deadline());
          const job = await app.inject(`/jobs/${update.eventId}`);
          assert.equal(job.statusCode, 200);
          assert.ok(!job.body.includes('lease_token'));
          assert.ok(!job.body.includes('leaseToken'));
          const state = await app.inject(`/products/${update.productId}/state`);
          assert.equal(state.statusCode, 200);
          assert.equal(state.json().comparison, 'one_missing');
          assert.equal(state.json().atomic, false);
          assert.equal((await app.inject('/health/live')).statusCode, 200);
          assert.equal((await app.inject('/health/ready')).statusCode, 200);
          assert.equal((await app.inject(`/jobs/${randomUUID()}`)).statusCode, 404);
          assert.deepEqual(
            (
              await pool.query(
                'SELECT row_to_json(j) AS row FROM public.reconciliation_jobs j WHERE job_id=$1',
                [update.eventId],
              )
            ).rows,
            before.rows,
          );
          const productAfter = await sql.read(update.productId, deadline());
          if (productBefore.state === 'present' && productAfter.state === 'present')
            assert.deepEqual(productAfter.stored, productBefore.stored);
          else assert.fail('The SQL copy must remain present.');
          assert.equal((await mongo.read(update.productId, deadline())).state, 'missing');
        } finally {
          await app.close();
        }
      },
    );
  },
);
