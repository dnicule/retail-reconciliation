/**
 * @file Reads validated product evidence and applies one conditional reconciliation repair.
 * Adapters preserve destination-local fields, report races and uncertain writes, and never
 * decide cross-store completion or own the supplied database clients.
 */
import { Int32 } from 'mongodb';
import type { MongoClient } from 'mongodb';
import type { Pool } from 'pg';
import { sameProduct, validateSnapshot } from './model/product.js';
import { SystemicError } from './model/diagnostic.js';
import type { Diagnostic } from './model/diagnostic.js';
import type { Log, ProductStore } from './model/contracts.js';
import type { Observation, RepairResult, Target } from './model/reconciliation.js';
import type { Snapshot } from './model/product.js';
import { query } from './postgres.js';
import {
  advanceProductSql,
  insertProductSql,
  selectProductSql,
} from './resources/postgres/products.js';
import {
  productCollectionName,
  productIdentityFilter,
  productInsertDocument,
  productReadOptions,
  productRevisionFilter,
  productUpdateDocument,
  productWriteOptions,
} from './resources/mongodb/products.js';
import type { ProductDocument } from './resources/mongodb/products.js';
import {
  assertMongoReady,
  assertPostgresReady,
  mongoOptions,
  rethrowMongoSystemic,
} from './readiness.js';

interface Operations {
  /** Read raw stored evidence; null exclusively means a successful empty lookup. */
  read(productId: string, deadline: number): Promise<Record<string, unknown> | null>;
  /** Insert absence or advance the observed lower revision; false means a concurrent write won. */
  write(snapshot: Snapshot, expected: number | null, deadline: number): Promise<boolean>;
  /** Inspect existing schema and permissions without mutation; reject when readiness is unconfirmed. */
  ready(deadline: number): Promise<void>;
}

/**
 * Validate a stored string by Unicode code points without changing its contents.
 * @param value - Untrusted field from a persisted record.
 * @param minimum - Inclusive character minimum.
 * @param maximum - Inclusive character maximum.
 * @returns Whether the field is a string satisfying the documented bounds.
 */
function validString(value: unknown, minimum: number, maximum: number): value is string {
  return typeof value === 'string' && [...value].length >= minimum && [...value].length <= maximum;
}

/**
 * Build trusted revision evidence only from a completely valid stored product.
 * @param row - Raw database fields, retaining MongoDB BSON types.
 * @param target - Destination-specific naming and local-field rules.
 * @param productId - Identifier that the stored primary key must exactly match.
 * @returns A snapshot or null for invalid type, length, timestamp, local field, or identity.
 */
function snapshotFrom(
  row: Record<string, unknown>,
  target: Target,
  productId: string,
): Snapshot | null {
  const id = target === 'sql' ? row.product_id : row._id;
  const storedRevision = target === 'sql' ? row.source_revision : row.sourceRevision;
  const revision = storedRevision instanceof Int32 ? storedRevision.valueOf() : storedRevision;
  const applied = target === 'sql' ? row.applied_at : row.appliedAt;
  const local = target === 'sql' ? row.internal_note : row.seoTitle;
  const validLocal =
    target === 'sql'
      ? local === null || validString(local, 0, 2000)
      : local === undefined || local === null || validString(local, 0, 200);
  // Promotion must remain disabled: an integer-valued BSON Double is incompatible evidence.
  if (
    (target === 'mongo' && !(storedRevision instanceof Int32)) ||
    typeof id !== 'string' ||
    typeof revision !== 'number' ||
    !(applied instanceof Date) ||
    !Number.isFinite(applied.getTime()) ||
    !validLocal ||
    !validString(row.name, 1, 200) ||
    !validString(row.description, 0, 4000) ||
    !validString(row.category, 1, 100)
  )
    return null;
  const candidate: Snapshot = {
    productId: id,
    revision,
    product: { name: row.name, description: row.description, category: row.category },
  };
  return validateSnapshot(candidate, productId) === null ? candidate : null;
}

/** Extracts only the driver's code, never its potentially sensitive error text. */
function errorCode(error: unknown): unknown {
  return typeof error === 'object' && error !== null && 'code' in error ? error.code : undefined;
}

/**
 * Preserve systemic stop signals and classify product-specific failures safely.
 * @param error - Unknown driver failure whose raw text is discarded.
 * @param target - Destination identifying the sanitized diagnostic.
 * @returns A permanent data rejection or retryable uncertainty; retries can hide committed writes.
 * @throws SystemicError for recognized access or schema failures.
 */
function failed(error: unknown, target: Target): RepairResult {
  if (error instanceof SystemicError) throw error;
  if (target === 'mongo') rethrowMongoSystemic(error);
  const code = errorCode(error);
  const invalid =
    target === 'sql' ? typeof code === 'string' && /^(22|23)/.test(code) : code === 121;
  return {
    outcome: invalid ? 'failed' : 'retry',
    diagnostic: {
      code: `${target.toUpperCase()}_${invalid ? 'VALIDATION_FAILED' : 'UNAVAILABLE'}`,
      message: invalid
        ? 'Product data was rejected by the destination storage rules.'
        : 'The destination operation could not be confirmed; a submitted write may have applied.',
    },
  };
}

/**
 * Add validation, safety checks, and meaningful outcome logs around native bounded I/O.
 * @param target - Store identity for logging and diagnostics; never a winner preference.
 * @param operations - Native adapter operations with explicit absence and revision predicates.
 * @param log - Caller-owned sanitized structured event sink.
 * @returns A lazy store performing no I/O until a method is called; clients remain caller-owned.
 */
function createStore(target: Target, operations: Operations, log: Log): ProductStore {
  return {
    /**
     * Observe one product without interpreting connection failures as missing records.
     * @param productId - Shared identifier to look up.
     * @param deadline - Absolute Unix millisecond limit including connection acquisition.
     * @returns Valid present evidence, confirmed missing, invalid data, or unavailable uncertainty.
     * @throws SystemicError when schema or access configuration must stop further claims.
     */
    async read(productId, deadline): Promise<Observation> {
      const started = Date.now();
      try {
        const row = await operations.read(productId, deadline);
        const observedAt = new Date().toISOString();
        if (row === null) return { state: 'missing', observedAt };
        const snapshot = snapshotFrom(row, target, productId);
        if (snapshot) return { state: 'present', snapshot, observedAt };
        const diagnostic = {
          code: 'INVALID_PRODUCT_RECORD',
          message:
            'The destination product violates its documented field, revision, or timestamp contract.',
          target,
        };
        log('product_read_invalid', {
          productId,
          target,
          outcome: 'invalid',
          diagnostic,
          durationMs: Date.now() - started,
        });
        return { state: 'invalid', diagnostic, observedAt };
      } catch (error) {
        const result = failed(error, target);
        const diagnostic = result.diagnostic ?? {
          code: 'DESTINATION_UNAVAILABLE',
          message: 'The destination read could not be confirmed.',
        };
        log('product_read_unavailable', {
          productId,
          target,
          outcome: 'unavailable',
          diagnostic,
          durationMs: Date.now() - started,
        });
        return { state: 'unavailable', diagnostic, observedAt: new Date().toISOString() };
      }
    },
    /**
     * Apply one repair authorized by validated observations, without rereading only one store.
     * @param snapshot - Complete selected source evidence whose revision is never changed.
     * @param observed - This round's destination evidence; unknown/invalid never authorizes insertion.
     * @param deadline - Absolute Unix millisecond limit for the conditional operation.
     * @returns Applied after acknowledgement, race for refreshed reconciliation, noop for already
     * satisfied content, failed for malformed evidence, or retry for an uncertain write.
     * @throws SystemicError for incompatible schema or permissions requiring process attention.
     */
    async repair(snapshot, observed, deadline): Promise<RepairResult> {
      const started = Date.now();
      const invalid = validateSnapshot(snapshot);
      if (invalid) return { outcome: 'failed', diagnostic: invalid };
      if (observed.state === 'unavailable')
        return { outcome: 'retry', diagnostic: observed.diagnostic };
      if (observed.state === 'invalid')
        return { outcome: 'failed', diagnostic: observed.diagnostic };
      const current = observed.state === 'present' ? observed.snapshot : null;
      if (current) {
        const invalidCurrent = validateSnapshot(current, snapshot.productId);
        if (invalidCurrent) return { outcome: 'failed', diagnostic: invalidCurrent };
        if (current.revision >= snapshot.revision) {
          if (
            current.revision === snapshot.revision &&
            !sameProduct(current.product, snapshot.product)
          ) {
            return {
              outcome: 'failed',
              diagnostic: {
                code: 'EQUAL_REVISION_CONFLICT',
                message: 'Repair cannot overwrite different content at the same revision.',
              },
            };
          }
          log('product_noop', {
            productId: snapshot.productId,
            target,
            revision: snapshot.revision,
            outcome: current.revision > snapshot.revision ? 'superseded' : 'unchanged',
            durationMs: Date.now() - started,
          });
          return { outcome: 'noop' };
        }
      }
      try {
        const applied = await operations.write(snapshot, current?.revision ?? null, deadline);
        log(applied ? 'product_write_succeeded' : 'product_write_raced', {
          productId: snapshot.productId,
          target,
          revision: snapshot.revision,
          outcome: applied ? (current ? 'updated' : 'inserted') : 'race',
          durationMs: Date.now() - started,
        });
        return { outcome: applied ? 'applied' : 'race' };
      } catch (error) {
        const result = failed(error, target);
        log('product_write_unconfirmed', {
          productId: snapshot.productId,
          target,
          revision: snapshot.revision,
          outcome: result.outcome,
          diagnostic: result.diagnostic,
          durationMs: Date.now() - started,
        });
        return result;
      }
    },
    /**
     * Probe existing schema and access without any sample writes or temporary objects.
     * @param deadline - Absolute Unix millisecond readiness limit.
     * @returns Resolves only after readiness is confirmed.
     * @throws A systemic incompatibility or transient driver/deadline failure.
     */
    async ready(deadline): Promise<void> {
      await operations.ready(deadline);
    },
  };
}

/**
 * Construct independent SQL reads and expected-revision repairs over existing products.
 * @param pool - Caller-owned PostgreSQL pool, borrowed per operation and never closed here.
 * @param log - Structured event sink; connection details and payloads are excluded.
 * @returns A lazy store preserving internal_note and changing applied_at only on advancement.
 */
export function createSqlStore(pool: Pool, log: Log): ProductStore {
  return createStore(
    'sql',
    {
      /** Reads known columns by primary key; failed reads reject instead of implying absence. */
      async read(productId, deadline) {
        const result = await query(pool, selectProductSql, [productId], deadline);
        return result.rows[0] ?? null;
      },
      /** Writes only the selected managed fields when absence or the exact older revision still holds. */
      async write(snapshot, expected, deadline) {
        const values = [
          snapshot.productId,
          snapshot.product.name,
          snapshot.product.description,
          snapshot.product.category,
          snapshot.revision,
        ];
        if (expected === null) {
          const result = await query(pool, insertProductSql, values, deadline);
          return result.rowCount === 1;
        }
        // Equality fences the observation; the extra monotonic predicate excludes accidental downgrades.
        const result = await query(pool, advanceProductSql, [...values, expected], deadline);
        return result.rowCount === 1;
      },
      /** Checks worker-required PostgreSQL structures and privileges using read-only catalogs. */
      async ready(deadline) {
        await assertPostgresReady(pool, deadline);
      },
    },
    log,
  );
}

/**
 * Construct independent MongoDB reads and conditional writes using explicit BSON Int32 revisions.
 * @param client - Caller-owned MongoDB client configured with retryWrites=false.
 * @param database - Existing catalog database name; construction never creates it.
 * @param log - Structured event sink for confirmed operations and sanitized uncertainty.
 * @returns A lazy store preserving extra application fields during managed-field updates.
 */
export function createMongoStore(client: MongoClient, database: string, log: Log): ProductStore {
  const collection = client.db(database).collection<ProductDocument>(productCollectionName);
  return createStore(
    'mongo',
    {
      /** Retains BSON types so integer-valued Double revisions cannot masquerade as valid Int32. */
      async read(productId, deadline) {
        return await collection.findOne(productIdentityFilter(productId), {
          ...mongoOptions(deadline),
          ...productReadOptions,
        });
      },
      /** Inserts confirmed absence or updates exactly the observed lower revision, never using upsert. */
      async write(snapshot, expected, deadline) {
        const appliedAt = new Date();
        const options = { ...mongoOptions(deadline), ...productWriteOptions };
        if (expected === null) {
          try {
            const result = await collection.insertOne(
              productInsertDocument(snapshot, appliedAt),
              options,
            );
            if (!result.acknowledged)
              throw new Error('MongoDB insertion acknowledgement unavailable.');
            return true;
          } catch (error) {
            if (errorCode(error) === 11000) return false;
            throw error;
          }
        }
        // The wrapper proves selected > observed; equality here detects a newer concurrent winner.
        const result = await collection.updateOne(
          productRevisionFilter(snapshot.productId, expected),
          productUpdateDocument(snapshot, appliedAt),
          options,
        );
        if (!result.acknowledged) throw new Error('MongoDB update acknowledgement unavailable.');
        return result.matchedCount === 1;
      },
      /** Checks the existing validator, indexes, and effective permissions without mutation. */
      async ready(deadline) {
        await assertMongoReady(client, database, deadline);
      },
    },
    log,
  );
}
