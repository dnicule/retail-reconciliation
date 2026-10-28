/**
 * @file Verifies durable worker claims and lease fencing against an externally initialized test database.
 * Uses fresh product/job IDs and deletes only those registered records; never initializes schemas.
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';
import type { TestContext } from 'node:test';
import { Pool } from 'pg';
import { createJobRepository } from '../../src/jobs.js';
import { readUpdateJob } from '../support/job-state.js';
import type { AttemptResult, Job } from '../../src/model/job.js';

/**
 * Reject application database targets and URI overrides before opening any socket.
 * @returns The explicit test connection and expected name, or null when not configured.
 * @throws When TEST_POSTGRES_URL is malformed or does not name an isolated test database.
 */
function configuration(): { connection: string; database: string } | null {
  const connection = process.env.TEST_POSTGRES_URL;
  if (!connection) return null;
  let url: URL;
  try {
    url = new URL(connection);
  } catch {
    throw new Error('Invalid TEST_POSTGRES_URL; connection omitted.');
  }
  assert.ok(['postgres:', 'postgresql:'].includes(url.protocol) && url.hostname && !url.hash);
  const database = decodeURIComponent(url.pathname.slice(1));
  assert.match(
    database,
    /^(?:reconciliation_worker|update_api)_test_[a-z0-9_]+$/,
    'Refusing a non-test PostgreSQL database.',
  );
  for (const key of url.searchParams.keys())
    assert.ok(
      !['db', 'dbname', 'database', 'connectionstring', 'options'].includes(key.toLowerCase()),
      'Refusing URI target/session overrides.',
    );
  return { connection, database };
}

/** Return a fresh absolute deadline so prior setup never consumes the current operation budget. */
function deadline(): number {
  return Date.now() + 15000;
}

/** Return a proposed successful result; repository fencing decides whether it may be saved. */
function completed(): AttemptResult {
  return {
    status: 'completed',
    outcomes: { sql: 'satisfied', mongo: 'satisfied' },
    lastError: null,
    conflictDetails: null,
    retryDelayMs: 0,
  };
}

const config = configuration();
test(
  'real PostgreSQL worker claims, inspection, and lease boundaries',
  {
    skip:
      config === null ? 'Set TEST_POSTGRES_URL to an externally initialized test database.' : false,
  },
  /** Exercise actual row locks and triggers using test-owned identities with reliable cleanup. */ async (
    t: TestContext,
  ) => {
    assert.ok(config);
    const applicationName = `worker-jobs-${randomUUID()}`;
    const pool = new Pool({
      connectionString: config.connection,
      application_name: applicationName,
      max: 5,
      connectionTimeoutMillis: 3000,
      statement_timeout: 3000,
      query_timeout: 3000,
    });
    const products: string[] = [];
    let cleanup = false;
    const logs: { event: string; fields: Record<string, unknown> }[] = [];
    const jobs = createJobRepository(
      pool,
      /** Capture confirmed operations and verify no lease token escapes. */ (event, fields) => {
        logs.push({ event, fields });
      },
    );
    t.after(
      /** Remove only this invocation's jobs and release all pool connections. */ async () => {
        try {
          if (cleanup && products.length)
            await pool.query(
              'DELETE FROM public.reconciliation_jobs WHERE product_id=ANY($1::text[])',
              [products],
            );
        } finally {
          await pool.end();
        }
      },
    );
    assert.equal(
      (await pool.query<{ name: string }>('SELECT current_database() AS name')).rows[0]?.name,
      config.database,
    );
    await jobs.ready(deadline());
    cleanup = true;

    /** Register a test identity before its first insertion so cleanup remains safe on failure. */
    function product(): string {
      const value = `JIT-${randomUUID()}`;
      products.push(value);
      return value;
    }

    /** Insert a unique approved-update fixture directly, simulating existing durable API work. */
    async function pendingUpdate(): Promise<Job> {
      const jobId = randomUUID();
      await pool.query(
        "INSERT INTO public.reconciliation_jobs(job_id,kind,product_id,source_revision,approved_payload) VALUES($1,'update',$2,1,$3)",
        [
          jobId,
          product(),
          { name: 'Job fixture', description: 'Approved integration snapshot.', category: 'Tests' },
        ],
      );
      const saved = await readUpdateJob(pool, jobId);
      assert.ok(saved);
      return saved;
    }

    await t.test(
      'competing workers claim once and reject the old token after takeover',
      /** SKIP LOCKED and live leases must protect the same row across independent clients. */ async () => {
        const update = await pendingUpdate();
        const claims = await Promise.all([
          jobs.claim(deadline(), update.jobId),
          jobs.claim(deadline(), update.jobId),
        ]);
        assert.equal(
          claims.filter(/** Count only committed attempts. */ (claim) => claim.kind === 'claimed')
            .length,
          1,
        );
        const first = claims.find(
          /** Pick the lease holder for takeover validation. */ (claim) => claim.kind === 'claimed',
        );
        assert.ok(first?.kind === 'claimed' && first.job.leaseToken);
        await pool.query(
          "UPDATE public.reconciliation_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1",
          [update.jobId],
        );
        const second = await jobs.claim(deadline(), update.jobId);
        assert.ok(second.kind === 'claimed' && second.job.leaseToken);
        assert.equal(second.job.attemptCount, 2);
        assert.notEqual(second.job.leaseToken, first.job.leaseToken);
        assert.equal(
          await jobs.finalize(update.jobId, first.job.leaseToken, completed(), deadline()),
          null,
        );
        assert.equal(
          (await jobs.finalize(update.jobId, second.job.leaseToken, completed(), deadline()))
            ?.status,
          'completed',
        );
      },
    );

    await t.test(
      'waiting for an unchanged row past expiry loses ownership',
      /** PostgreSQL must check the clock after the row lock, rather than preserving an earlier eligible snapshot. */ async () => {
        const update = await pendingUpdate();
        const claim = await jobs.claim(deadline(), update.jobId);
        assert.ok(claim.kind === 'claimed' && claim.job.leaseToken);
        await pool.query(
          "UPDATE public.reconciliation_jobs SET lease_expires_at=clock_timestamp()+interval '500 milliseconds' WHERE job_id=$1",
          [update.jobId],
        );
        const locker = await pool.connect();
        let finish: Promise<unknown> | undefined;
        try {
          await locker.query('BEGIN');
          await locker.query(
            'SELECT job_id FROM public.reconciliation_jobs WHERE job_id=$1 FOR UPDATE',
            [update.jobId],
          );
          finish = jobs.finalize(update.jobId, claim.job.leaseToken, completed(), deadline());
          // Observe the server-side blocking state instead of guessing when the finalizer reached its lock.
          const until = Date.now() + 1500;
          let blocked = false;
          while (!blocked && Date.now() < until) {
            const result = await pool.query<{ blocked: boolean }>(
              "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND application_name=$1 AND query LIKE '%reconciliation_jobs%' AND pid<>pg_backend_pid()) AS blocked",
              [applicationName],
            );
            blocked = result.rows[0]?.blocked === true;
            if (!blocked) await delay(10);
          }
          assert.equal(blocked, true, 'Finalizer must reach the locked row.');
          await pool.query(
            'SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_expires_at-clock_timestamp()))+0.02) FROM public.reconciliation_jobs WHERE job_id=$1',
            [update.jobId],
          );
          await locker.query('COMMIT');
          assert.equal(await finish, null);
          assert.equal((await readUpdateJob(pool, update.jobId))?.status, 'processing');
        } finally {
          await locker.query('ROLLBACK');
          locker.release();
          await finish;
        }
      },
    );

    await t.test(
      'expired eighth attempt stops until an external component requeues the same immutable job',
      /** Test-only SQL models another component resetting mutable bookkeeping; the worker only consumes the resulting due update. */ async () => {
        const update = await pendingUpdate();
        await pool.query(
          "UPDATE public.reconciliation_jobs SET status='processing',attempt_count=8,next_attempt_at=NULL,lease_token=$2,lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1",
          [update.jobId, randomUUID()],
        );
        const exhausted = await jobs.claim(deadline(), update.jobId);
        assert.equal(exhausted.kind, 'exhausted');
        const old = await readUpdateJob(pool, update.jobId);
        assert.equal(old?.status, 'needs_attention');
        assert.equal(old?.attemptCount, 8);
        assert.deepEqual(await jobs.claim(deadline(), update.jobId), { kind: 'none' });
        // This direct test-owned transition models an external owner; the worker exposes no requeue operation.
        const requeued = await pool.query(
          "UPDATE public.reconciliation_jobs SET status='pending',attempt_count=0,next_attempt_at=clock_timestamp(),lease_token=NULL,lease_expires_at=NULL,completed_at=NULL,sql_outcome='unknown',mongo_outcome='unknown',last_error=NULL,conflict_details=NULL WHERE job_id=$1 AND kind='update' AND status IN ('conflict','needs_attention')",
          [update.jobId],
        );
        assert.equal(requeued.rowCount, 1);
        const retried = await readUpdateJob(pool, update.jobId);
        assert.ok(retried);
        assert.equal(retried.status, 'pending');
        assert.equal(retried.attemptCount, 0);
        assert.equal(retried.createdAt, update.createdAt);
        assert.deepEqual(retried.approvedPayload, update.approvedPayload);
        assert.equal(retried.revision, update.revision);
        assert.equal(retried.lastError, null);
        assert.equal(retried.productId, update.productId);
        assert.equal(retried.kind, update.kind);
        assert.equal(retried.leaseToken, null);
        assert.equal(retried.leaseExpiresAt, null);
        assert.equal(retried.completedAt, null);
        assert.equal(retried.conflictDetails, null);
        assert.deepEqual(retried.outcomes, { sql: 'unknown', mongo: 'unknown' });
        const recovered = await jobs.claim(deadline(), update.jobId);
        assert.equal(recovered.kind, 'claimed');
        assert.equal(recovered.job.attemptCount, 1);
        assert.equal(recovered.job.jobId, update.jobId);
        assert.equal(JSON.stringify(logs).includes('leaseToken'), false);
      },
    );

    await t.test(
      'future retry stays unclaimed until its database schedule is due',
      /** Continuous scans inspect current eligibility instead of capturing a one-shot scope. */ async () => {
        const future = await pendingUpdate();
        await pool.query(
          "UPDATE public.reconciliation_jobs SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE job_id=$1",
          [future.jobId],
        );
        assert.deepEqual(await jobs.claim(deadline(), future.jobId), { kind: 'none' });
        assert.equal((await readUpdateJob(pool, future.jobId))?.attemptCount, 0);
        await pool.query(
          "UPDATE public.reconciliation_jobs SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE job_id=$1",
          [future.jobId],
        );
        const due = await jobs.claim(deadline(), future.jobId);
        assert.equal(due.kind, 'claimed');
        assert.equal(due.job.attemptCount, 1);
      },
    );

    await t.test(
      'approved history ignores mutable delivery status',
      /** Existing immutable updates remain authoritative even when their delivery is blocked. */ async () => {
        const source = product();
        await pool.query(
          "INSERT INTO public.reconciliation_jobs(job_id,kind,product_id,source_revision,approved_payload,status,next_attempt_at,last_error) VALUES($1,'update',$2,9,$3,'needs_attention',NULL,$4)",
          [
            randomUUID(),
            source,
            { name: 'Approved nine', description: '', category: 'Tests' },
            { code: 'TEST', message: 'Blocked history' },
          ],
        );
        const latest = await jobs.latest(source, deadline());
        assert.equal(latest?.revision, 9);
        assert.equal(latest?.product.name, 'Approved nine');
      },
    );

    await t.test(
      'retained audits are invisible to worker operations and remain unchanged',
      /** Another component owns these rows; even a caller holding their IDs/tokens cannot repurpose them here. */ async () => {
        const auditIds: string[] = [];
        const auditToken = randomUUID();
        for (const state of [
          'pending',
          'scheduled',
          'processing',
          'live_processing',
          'needs_attention',
          'conflict',
        ] as const) {
          const auditId = randomUUID();
          auditIds.push(auditId);
          const status =
            state === 'scheduled' ? 'pending' : state === 'live_processing' ? 'processing' : state;
          await pool.query(
            "INSERT INTO public.reconciliation_jobs(job_id,kind,product_id,status,attempt_count,next_attempt_at,lease_token,lease_expires_at,conflict_details) VALUES($1,'audit',$2,$3,CASE WHEN $3='processing' THEN 1 ELSE 0 END,CASE WHEN $3='pending' THEN clock_timestamp()+($4::double precision*interval '1 second') ELSE NULL END,CASE WHEN $3='processing' THEN $5::uuid ELSE NULL END,CASE WHEN $3='processing' THEN clock_timestamp()+($6::double precision*interval '1 second') ELSE NULL END,CASE WHEN $3='conflict' THEN '{\"revision\":1}'::jsonb ELSE NULL END)",
            [
              auditId,
              product(),
              status,
              state === 'scheduled' ? 3600 : -3600,
              auditToken,
              state === 'live_processing' ? 30 : -1,
            ],
          );
        }
        const original = (
          await pool.query(
            'SELECT * FROM public.reconciliation_jobs WHERE job_id=ANY($1::uuid[]) ORDER BY job_id',
            [auditIds],
          )
        ).rows;
        for (const auditId of auditIds) {
          assert.deepEqual(await jobs.claim(deadline(), auditId), { kind: 'none' });
          assert.equal(await jobs.finalize(auditId, auditToken, completed(), deadline()), null);
        }
        const after = (
          await pool.query(
            'SELECT * FROM public.reconciliation_jobs WHERE job_id=ANY($1::uuid[]) ORDER BY job_id',
            [auditIds],
          )
        ).rows;
        assert.deepEqual(
          after,
          original,
          'No audit identity, state, diagnostics, lease, or timestamp may change.',
        );
      },
    );
  },
);
