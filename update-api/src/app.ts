/**
 * @file HTTP admission, durable receipts, and read-only operational diagnostics.
 * Validates source envelopes, dispatches only acknowledged new jobs, and reports
 * confirmed or explicitly stale results. Injected adapters own database behavior;
 * the runtime owns connections, while later retries and reconciliation belong to the worker.
 */
import Fastify, { type FastifyReply, type FastifyServerOptions } from 'fastify';
import type {
  ApprovedUpdate,
  AttemptResult,
  JobRepository,
  ObservedJob,
  ProductDestination,
  ProductObservation,
  TargetResult,
} from './model.js';
import { sameProduct } from './products.js';

/** Caller-owned adapters plus optional logger and epoch-millisecond clock for isolated route tests. */
export type AppOptions = {
  jobs: JobRepository;
  sql: ProductDestination;
  mongo: ProductDestination;
  logger?: FastifyServerOptions['logger'];
  now?: () => number;
};
const uuidPattern = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
const productIdPattern = '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$';
const bodySchema = {
  type: 'object',
  additionalProperties: false,
  required: ['eventId', 'productId', 'revision', 'product'],
  properties: {
    eventId: { type: 'string', pattern: uuidPattern },
    productId: { type: 'string', pattern: productIdPattern },
    revision: { type: 'integer', minimum: 1, maximum: 2147483647 },
    product: {
      type: 'object',
      additionalProperties: false,
      required: ['name', 'description', 'category'],
      properties: {
        name: { type: 'string', minLength: 1, maxLength: 200 },
        description: { type: 'string', maxLength: 4000 },
        category: { type: 'string', minLength: 1, maxLength: 100 },
      },
    },
  },
};

/**
 * Build the stable public error envelope from controlled application text.
 * @param code - Machine-readable error identifier; callers do not pass raw driver errors.
 * @param message - Safe explanation of the failure and any relevant next step.
 * @param retryable - Whether another request may succeed; defaults to false.
 * @returns A new error object without mutating a response or writing logs.
 */
function errorBody(code: string, message: string, retryable = false) {
  return { error: { code, message, retryable } };
}

/**
 * Describe unconfirmed delivery without asserting whether a write reached the destination.
 * @param code - Safe reason for deferring work; defaults to destination unavailability.
 * @returns A retry outcome for later guarded bookkeeping, not a persisted retry schedule.
 */
function retry(code = 'DESTINATION_UNAVAILABLE'): TargetResult {
  return {
    outcome: 'retry',
    action: 'unavailable',
    diagnostic: {
      code,
      message: 'Delivery could not be confirmed; background processing is required.',
    },
  };
}

/**
 * Choose the attempt's proposed status while preserving both independent target outcomes.
 * Conflict precedes permanent failure, then retry, then completion. Scheduling is performed
 * separately by PostgreSQL; drawing jitter here does not change any durable state.
 * @param sql - Semantic result of the initial SQL destination attempt.
 * @param mongo - Semantic result of the initial MongoDB destination attempt.
 * @returns Bookkeeping input with a 500–1,000 millisecond delay used only for pending status.
 */
function attempt(sql: TargetResult, mongo: TargetResult): AttemptResult {
  const outcomes = [sql.outcome, mongo.outcome];
  const status = outcomes.includes('conflict')
    ? 'conflict'
    : outcomes.includes('failed')
      ? 'needs_attention'
      : outcomes.includes('retry')
        ? 'pending'
        : 'completed';
  return { status, sql, mongo, retryDelayMs: 500 + Math.floor(Math.random() * 501) };
}

/**
 * Send a receipt from a confirmed job observation, never from proposed in-memory outcomes.
 * @param reply - Fastify reply whose HTTP status, Location header, and body will be sent.
 * @param state - Last confirmed stored job and its original database observation time.
 * @param duplicate - Whether this request reused existing identity without dispatching delivery.
 * @param fresh - Whether status lookup/finalization was confirmed rather than falling back to admission.
 * @returns The sent reply: 200 for recorded completion, otherwise 202 for accepted unresolved work.
 */
function receipt(reply: FastifyReply, state: ObservedJob, duplicate: boolean, fresh: boolean) {
  return reply
    .code(state.job.status === 'completed' ? 200 : 202)
    .header('Location', `/jobs/${state.job.jobId}`)
    .send({
      jobId: state.job.jobId,
      productId: state.job.productId,
      revision: state.job.revision,
      status: state.job.status,
      duplicate,
      statusFresh: fresh,
      statusAsOf: state.observedAt,
    });
}

/**
 * Classify independent product observations using only source revision and shared content.
 * Local fields and application timestamps do not choose a winner; this function performs no repairs.
 * @param sql - Validated SQL read, confirmed absence, or diagnostic failure.
 * @param mongo - Independently obtained MongoDB observation.
 * @returns Matching/different, one/both missing, or unknown if either observation is unavailable/invalid.
 */
function compare(sql: ProductObservation, mongo: ProductObservation): string {
  if (
    sql.state === 'unavailable' ||
    sql.state === 'invalid' ||
    mongo.state === 'unavailable' ||
    mongo.state === 'invalid'
  )
    return 'unknown';
  if (sql.state === 'missing' && mongo.state === 'missing') return 'both_missing';
  if (sql.state === 'missing' || mongo.state === 'missing') return 'one_missing';
  if (sql.state !== 'present' || mongo.state !== 'present') return 'unknown';
  return sql.snapshot.revision === mongo.snapshot.revision &&
    sameProduct(sql.snapshot.product, mongo.snapshot.product)
    ? 'matching'
    : 'different';
}

/**
 * Convert an unexpected adapter rejection into a safe unavailable observation.
 * @param result - Settled product read; fulfilled semantic observations are retained unchanged.
 * @param observedAt - Local ISO timestamp used only when the rejected read has no observation time.
 * @returns The adapter's observation or an unavailable result, never inferred absence or a raw error.
 */
function observation(
  result: PromiseSettledResult<ProductObservation>,
  observedAt: string,
): ProductObservation {
  return result.status === 'fulfilled'
    ? result.value
    : {
        state: 'unavailable',
        observedAt,
        diagnostic: { code: 'READ_UNAVAILABLE', message: 'The product could not be read.' },
      };
}

/**
 * Register the API on a new Fastify instance without opening a listener or probing databases.
 * Routes use caller-supplied adapters; closing this instance does not close their shared clients.
 * @param options - Job/product adapters, optional Fastify logger (enabled by default), and
 * optional epoch-millisecond clock. A test clock must agree with any deadline-aware test adapters.
 * @returns An unstarted instance for listen() or inject(); the caller must close it after requests drain.
 * @throws If Fastify rejects supplied server/logger configuration during construction or registration.
 * @example
 * const app = buildApp({ jobs, sql, mongo, logger: false });
 * try {
 *   const response = await app.inject('/health/live');
 *   console.log(response.statusCode);
 * } finally {
 *   await app.close(); // The owner closes the supplied database clients separately.
 * }
 */
export function buildApp(options: AppOptions) {
  const { jobs, sql, mongo } = options;
  const now = options.now ?? Date.now;
  const app = Fastify({
    logger: options.logger ?? true,
    bodyLimit: 32768,
    requestTimeout: 15000,
    ajv: { customOptions: { removeAdditional: false, coerceTypes: false, useDefaults: false } },
  });
  // Fastify otherwise accepts text/plain, which is outside the approved JSON source contract.
  app.removeContentTypeParser('text/plain');
  app.setErrorHandler(
    /**
     * Turn framework/request failures into controlled HTTP errors without logging exception contents.
     * @param error - Parser, validator, or route failure; inspect only its status/code metadata.
     * @param request - Request logger used for unexpected application failure correlation.
     * @param reply - Response to send with the corresponding safe status and error envelope.
     * @returns The sent reply; unexpected errors become 500 and known input failures retain 4xx status.
     */ (error, request, reply) => {
      const record = typeof error === 'object' && error !== null ? error : {};
      const code = 'code' in record && typeof record.code === 'string' ? record.code : '';
      const status =
        'statusCode' in record && typeof record.statusCode === 'number' ? record.statusCode : 500;
      if (status === 413)
        return reply
          .code(413)
          .send(errorBody('BODY_TOO_LARGE', 'JSON bodies must not exceed 32 KiB.'));
      if (status === 415)
        return reply.code(415).send(errorBody('UNSUPPORTED_MEDIA_TYPE', 'Use application/json.'));
      if (status === 400 || code === 'FST_ERR_VALIDATION')
        return reply
          .code(400)
          .send(errorBody('INVALID_REQUEST', 'Request fields, identifiers, or JSON are invalid.'));
      request.log.error(
        { event: 'request_failed', code: 'INTERNAL_ERROR' },
        'Request could not be completed',
      );
      return reply
        .code(500)
        .send(errorBody('INTERNAL_ERROR', 'An unexpected application error occurred.'));
    },
  );
  app.setNotFoundHandler(
    /** Keeps unknown routes consistent with the structured error contract. */ (_request, reply) =>
      reply.code(404).send(errorBody('NOT_FOUND', 'The requested route does not exist.')),
  );

  app.post<{ Body: ApprovedUpdate }>(
    '/approved-updates',
    { schema: { body: bodySchema } },
    /**
     * Admit a validated source event, attempt initial delivery, and return only confirmed job state.
     * Admission owns the transaction; only its accepted result permits product writes. A duplicate
     * returns immediately. Failed/expired bookkeeping triggers one status read; a stale receipt
     * is used only when that lookup also cannot confirm current state.
     * @param request - Schema-validated JSON envelope and correlation logger; only UUID casing is normalized.
     * @param reply - Response receiving an identity/admission error or the durable job's Location and receipt.
     * @returns The sent reply after bounded processing; accepted delivery failures remain inspectable jobs.
     */ async (request, reply) => {
      // Delivery must leave room to record its outcomes; requestTimeout also bounds incoming HTTP data.
      const started = now(),
        deadline = started + 15000,
        deliveryDeadline = deadline - 3000;
      const update = { ...request.body, eventId: request.body.eventId.toLowerCase() };
      const log = request.log.child({
        component: 'update-api',
        jobId: update.eventId,
        productId: update.productId,
        submittedRevision: update.revision,
      });
      const admission = await jobs.admit(update, deadline);
      if (admission.kind === 'unavailable') {
        const uncertain = admission.acceptance === 'unknown';
        log.warn(
          {
            event: 'admission_unavailable',
            acceptance: admission.acceptance,
            durationMs: now() - started,
          },
          'Admission could not be confirmed',
        );
        return reply.code(503).send({
          error: {
            code: uncertain ? 'ADMISSION_UNCERTAIN' : 'ADMISSION_UNAVAILABLE',
            message: uncertain
              ? 'Admission may have committed. Retry with the same event ID and unchanged content.'
              : 'Admission was not accepted. Retry with the same event ID.',
            retryable: true,
            acceptance: admission.acceptance,
          },
          eventId: update.eventId,
        });
      }
      if (admission.kind === 'collision')
        return reply.code(409).send({
          ...errorBody(
            admission.code,
            admission.code === 'EVENT_ID_COLLISION'
              ? 'The event ID identifies different immutable content.'
              : 'This product revision is already registered.',
          ),
          eventId: update.eventId,
          canonicalJobId: admission.canonicalJobId,
        });
      if (admission.kind === 'duplicate') {
        log.info(
          {
            event: 'duplicate_returned',
            status: admission.job.status,
            durationMs: now() - started,
          },
          'Existing job returned without another attempt',
        );
        return receipt(reply, admission, true, true);
      }
      log.info(
        {
          event: 'update_admitted',
          selectedRevision: admission.selected.revision,
          attemptCount: admission.job.attemptCount,
          durationMs: now() - started,
        },
        'Immutable update committed',
      );
      let result: AttemptResult;
      if (now() >= deliveryDeadline) {
        // Durable work survives even when late admission leaves no safe initial-delivery allowance.
        result = attempt(retry('DELIVERY_DEADLINE'), retry('DELIVERY_DEADLINE'));
      } else {
        const deliveries = await Promise.allSettled([
          Promise.resolve().then(
            /** Starts SQL independently, even if the other adapter throws synchronously. */ () =>
              sql.deliver(admission.selected, deliveryDeadline),
          ),
          Promise.resolve().then(
            /** Uses the committed in-memory snapshot even when PostgreSQL is now unavailable. */ () =>
              mongo.deliver(admission.selected, deliveryDeadline),
          ),
        ]);
        result = attempt(
          deliveries[0].status === 'fulfilled' ? deliveries[0].value : retry(),
          deliveries[1].status === 'fulfilled' ? deliveries[1].value : retry(),
        );
      }
      for (const target of ['sql', 'mongo'] as const) {
        const fields = {
          event: 'product_delivery',
          target,
          selectedRevision: admission.selected.revision,
          ...result[target],
          durationMs: now() - started,
        };
        if (result[target].outcome === 'satisfied')
          log.info(fields, 'Destination delivery confirmed');
        else if (result[target].outcome === 'failed')
          log.error(fields, 'Destination requires attention');
        else log.warn(fields, 'Destination requires further processing or attention');
      }
      try {
        // Leave one second for a final status read if bookkeeping acknowledgment is lost.
        const saved = await jobs.finalize(
          update.eventId,
          admission.leaseToken,
          result,
          Math.min(now() + 2000, deadline - 1000),
        );
        if (saved) {
          log.info(
            {
              event: saved.job.status === 'pending' ? 'retry_scheduled' : 'attempt_recorded',
              status: saved.job.status,
              nextAttemptAt: saved.job.nextAttemptAt,
              durationMs: now() - started,
            },
            'Guarded attempt result committed',
          );
          return receipt(reply, saved, false, true);
        }
        log.warn({ event: 'lease_lost' }, 'Current owner result will be read');
      } catch {
        log.warn(
          { event: 'finalization_unconfirmed' },
          'Attempt result acknowledgment unavailable',
        );
      }
      try {
        const fresh = await jobs.get(update.eventId, deadline);
        if (fresh) return receipt(reply, fresh, false, true);
      } catch {
        log.warn(
          { event: 'status_unavailable' },
          'Returning last confirmed durable job observation',
        );
      }
      return receipt(reply, admission, false, false);
    },
  );

  app.get<{ Params: { jobId: string } }>(
    '/jobs/:jobId',
    {
      schema: {
        params: {
          type: 'object',
          required: ['jobId'],
          properties: { jobId: { type: 'string', pattern: uuidPattern } },
        },
      },
    },
    /**
     * Look up durable job state without claiming, delivering, or repairing work.
     * @param request - Validated job UUID; uppercase input is canonicalized for lookup.
     * @param reply - Response used to distinguish a public job view from proven absence or unavailability.
     * @returns A 200 job view, 404 only after successful absence, or 503 after failed lookup or stored-row decoding.
     */ async (request, reply) => {
      try {
        const found = await jobs.get(request.params.jobId.toLowerCase(), now() + 3000);
        return found
          ? reply.send(found.job)
          : reply.code(404).send(errorBody('JOB_NOT_FOUND', 'No job has this ID.'));
      } catch {
        return reply
          .code(503)
          .send(errorBody('JOB_READ_UNAVAILABLE', 'Job status could not be read.', true));
      }
    },
  );

  app.get<{ Params: { productId: string } }>(
    '/products/:productId/state',
    {
      schema: {
        params: {
          type: 'object',
          required: ['productId'],
          properties: { productId: { type: 'string', pattern: productIdPattern } },
        },
      },
    },
    /**
     * Observe both stores and recent jobs independently without using failure as absence.
     * @param request - Validated case-sensitive product identifier for all three bounded reads.
     * @returns A diagnostic body with per-source availability, observation times, and atomic=false;
     * partial dependency failure is represented in the body and does not trigger repair or retry.
     */ async (request) => {
      const started = now(),
        deadline = started + 3000,
        id = request.params.productId;
      const reads = await Promise.allSettled([
        Promise.resolve().then(
          /** Starts a bounded SQL observation. */ () => sql.read(id, deadline),
        ),
        Promise.resolve().then(
          /** Starts a bounded Mongo observation. */ () => mongo.read(id, deadline),
        ),
        Promise.resolve().then(
          /** Reads history independently so failure cannot become an empty list. */ () =>
            jobs.recent(id, deadline),
        ),
      ]);
      const observedAt = new Date(now()).toISOString();
      const sqlState = observation(reads[0], observedAt),
        mongoState = observation(reads[1], observedAt);
      return {
        productId: id,
        atomic: false,
        startedAt: new Date(started).toISOString(),
        observedAt,
        sql: sqlState,
        mongo: mongoState,
        comparison: compare(sqlState, mongoState),
        jobs:
          reads[2].status === 'fulfilled'
            ? { state: 'available', observedAt, items: reads[2].value }
            : {
                state: 'unavailable',
                observedAt,
                diagnostic: {
                  code: 'JOB_HISTORY_UNAVAILABLE',
                  message: 'Job history could not be read.',
                },
              },
      };
    },
  );

  app.get('/health/live', /** Reports only process liveness. */ async () => ({ live: true }));
  app.get(
    '/health/ready',
    /**
     * Check admission readiness while exposing MongoDB degradation separately.
     * @param _request - Framework request; readiness does not depend on a body or identifier.
     * @param reply - Response whose status reflects the PostgreSQL admission check only.
     * @returns A 200/503 dependency report; rejected probes become unavailable, without schema writes.
     */ async (_request, reply) => {
      const deadline = now() + 3000;
      const checks = await Promise.allSettled([
        Promise.resolve().then(
          /** Checks required admission schema and privileges without writes. */ () =>
            jobs.ready(deadline),
        ),
        Promise.resolve().then(
          /** Observes catalog availability without blocking durable acceptance. */ () =>
            mongo.ready(deadline),
        ),
      ]);
      const postgres = checks[0].status === 'fulfilled' ? checks[0].value : { available: false };
      const mongodb = checks[1].status === 'fulfilled' ? checks[1].value : { available: false };
      return reply
        .code(postgres.available ? 200 : 503)
        .send({ ready: postgres.available, dependencies: { postgres, mongodb } });
    },
  );
  return app;
}
