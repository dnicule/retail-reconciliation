/**
 * @file Initial delivery and diagnostic reads for the existing product stores.
 * Validates stored evidence, applies immutable snapshots with revision predicates, and
 * converts driver failures into safe outcomes. Admission, job leases, and client shutdown
 * belong to their callers; this module neither initializes schemas nor reconciles both stores.
 */
import { Int32 } from 'mongodb';
import type { MongoClient } from 'mongodb';
import type { Pool } from 'pg';
import type {
  Diagnostic,
  ProductDestination,
  ProductObservation,
  SharedProduct,
  Snapshot,
  TargetResult,
} from './model.js';
import { query } from './postgres.js';

type Target = 'SQL' | 'MONGODB';
type ProductDocument = { _id: string; [field: string]: unknown };
type StoreOperations = {
  /**
   * Read one record without converting failures into absence.
   * @param productId - Exact destination key to look up.
   * @param deadline - Absolute operation deadline in epoch milliseconds.
   * @returns Stored fields, or null only after a successful lookup proves absence.
   * @throws If the deadline expires or the destination lookup cannot be confirmed.
   */
  read(productId: string, deadline: number): Promise<Record<string, unknown> | null>;
  /**
   * Insert a confirmed missing copy or conditionally advance an observed revision.
   * @param snapshot - Validated immutable snapshot selected for delivery.
   * @param expected - Observed lower revision, or null for confirmed absence.
   * @param deadline - Absolute operation deadline in epoch milliseconds.
   * @returns True after a confirmed write; false when a race requires a fresh read.
   * @throws If the write cannot be confirmed; rejection does not prove that no write occurred.
   */
  write(snapshot: Snapshot, expected: number | null, deadline: number): Promise<boolean>;
  /**
   * Check the destination using a read-only query or command.
   * @param deadline - Absolute probe deadline in epoch milliseconds.
   * @returns Resolves when the destination acknowledges the probe.
   * @throws If the deadline expires or the probe fails.
   */
  probe(deadline: number): Promise<void>;
};

/**
 * Compare the managed strings exactly; timestamps and destination-local fields are excluded.
 * @param left - First complete shared snapshot.
 * @param right - Other complete shared snapshot.
 * @returns Whether all three values are identical without normalization.
 */
export function sameProduct(left: SharedProduct, right: SharedProduct): boolean {
  return (
    left.name === right.name &&
    left.description === right.description &&
    left.category === right.category
  );
}

/**
 * Validate a stored string using Unicode character counts, as both storage validators do.
 * @param value - Untrusted stored field whose type and length must be checked.
 * @param minimum - Inclusive minimum number of Unicode code points.
 * @param maximum - Inclusive maximum number of Unicode code points.
 * @returns Whether the value is a string within the bounds, without normalizing its contents.
 */
function validString(value: unknown, minimum: number, maximum: number): value is string {
  if (typeof value !== 'string') return false;
  const length = [...value].length;
  return length >= minimum && length <= maximum;
}

/**
 * Validate all managed fields, diagnostic timestamps, and documented local-field types.
 * @param row - Raw record, retaining native BSON types for MongoDB validation.
 * @param target - Store whose field names and local-field requirements apply.
 * @param productId - Requested identifier that the stored key must exactly match.
 * @returns The trustworthy shared snapshot, or null for malformed stored evidence.
 */
function storedSnapshot(
  row: Record<string, unknown>,
  target: Target,
  productId: string,
): Snapshot | null {
  const id = target === 'SQL' ? row.product_id : row._id;
  const revisionValue = target === 'SQL' ? row.source_revision : row.sourceRevision;
  const revision =
    target === 'MONGODB' && revisionValue instanceof Int32
      ? revisionValue.valueOf()
      : revisionValue;
  const timestamp = target === 'SQL' ? row.applied_at : row.appliedAt;
  const local = target === 'SQL' ? row.internal_note : row.seoTitle;
  const validLocal =
    target === 'SQL'
      ? local === null || validString(local, 0, 2000)
      : local === undefined || local === null || validString(local, 0, 200);
  // Disable BSON promotion on reads so an integer-valued Double cannot masquerade as Int32.
  if (target === 'MONGODB' && !(revisionValue instanceof Int32)) return null;
  if (
    typeof id !== 'string' ||
    id !== productId ||
    !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(id) ||
    /[^A-Za-z0-9._-]/.test(id) ||
    typeof revision !== 'number' ||
    !Number.isInteger(revision) ||
    revision < 1 ||
    revision > 2147483647 ||
    !(timestamp instanceof Date) ||
    !Number.isFinite(timestamp.getTime()) ||
    !validLocal ||
    !validString(row.name, 1, 200) ||
    !validString(row.description, 0, 4000) ||
    !validString(row.category, 1, 100)
  )
    return null;
  return {
    productId: id,
    revision,
    product: { name: row.name, description: row.description, category: row.category },
  };
}

/**
 * Extract a driver's code for comparison without copying its message or connection details.
 * @param error - Unknown failure from a destination operation.
 * @returns The unvalidated code property, or undefined when absent; callers match known codes.
 */
function errorCode(error: unknown): unknown {
  return typeof error === 'object' && error !== null && 'code' in error ? error.code : undefined;
}

/**
 * Classify dependency failures without exposing driver messages or assuming a write rolled back.
 * @param error - Original read, write, or probe failure used only for error-code matching.
 * @param target - Destination whose authentication, schema, and validation codes apply.
 * @returns A permanent failed result for recognized configuration/validation errors, otherwise
 * a retry result whose diagnostic preserves uncertainty about possibly committed writes.
 */
function failedOperation(error: unknown, target: Target): TargetResult {
  const code = errorCode(error);
  const configuration =
    target === 'SQL'
      ? ['28P01', '28000', '42501', '42P01', '42703', '42804', '42883', '3D000'].includes(
          String(code),
        )
      : code === 13 || code === 18 || code === 26;
  const validation =
    target === 'SQL' ? typeof code === 'string' && /^(22|23)/.test(code) : code === 121;
  if (configuration || validation) {
    return {
      outcome: 'failed',
      action: 'invalid',
      diagnostic: {
        code: `${target}_${configuration ? 'CONFIGURATION_INVALID' : 'VALIDATION_FAILED'}`,
        message: configuration
          ? 'The destination schema or access configuration requires attention.'
          : 'The destination rejected product data under its storage rules.',
      },
    };
  }
  return {
    outcome: 'retry',
    action: 'unavailable',
    diagnostic: {
      code: `${target}_UNAVAILABLE`,
      message:
        'The destination operation could not be confirmed; a write may already have applied.',
    },
  };
}

/**
 * Limit MongoDB work to the remaining request budget and a three-second operation ceiling.
 * @param deadline - Absolute request or delivery deadline in epoch milliseconds.
 * @returns Positive remaining milliseconds, rounded down and capped at 3,000.
 * @throws If less than one whole millisecond remains; no operation should then start.
 */
function timeoutMilliseconds(deadline: number): number {
  const remaining = Math.floor(deadline - Date.now());
  if (remaining <= 0) throw new Error('The operation deadline has elapsed.');
  return Math.min(3000, remaining);
}

/**
 * Bound the entire MongoDB operation, including lazy connection before its native timeout context.
 * @param deadline - Absolute request or delivery deadline in epoch milliseconds.
 * @returns Driver timeout and abort signal using the same remaining allowance in milliseconds.
 * @throws If no operation allowance remains; no connection or command is then started.
 */
function mongoOptions(deadline: number): { timeoutMS: number; signal: AbortSignal } {
  const timeoutMS = timeoutMilliseconds(deadline);
  // The driver creates its timeout context after autoConnect; the signal also bounds that phase.
  // Expiry bounds the caller's wait, but cannot prove the server did not commit a write.
  return { timeoutMS, signal: AbortSignal.timeout(timeoutMS) };
}

/**
 * Wrap store I/O with shared validation and at most three revision-guarded rounds.
 * @param target - Diagnostic identity; never used to prefer one store's shared content.
 * @param operations - Bounded store-specific reads, conditional writes, and read-only probe.
 * @returns A destination whose methods report semantic outcomes instead of leaking driver errors;
 * construction performs no I/O and the caller retains ownership of all underlying clients.
 */
function createDestination(target: Target, operations: StoreOperations): ProductDestination {
  /**
   * Read and validate one destination's evidence without repairing it.
   * @param productId - Exact key of the copy to inspect.
   * @param deadline - Absolute lookup deadline in epoch milliseconds.
   * @returns A timestamped present, missing, invalid, or unavailable observation. Driver failures
   * become unavailable; MongoDB's validated Int32 revision becomes a number in diagnostic output.
   */
  async function read(productId: string, deadline: number): Promise<ProductObservation> {
    try {
      const row = await operations.read(productId, deadline);
      const observedAt = new Date().toISOString();
      if (!row) return { state: 'missing', observedAt };
      const snapshot = storedSnapshot(row, target, productId);
      if (!snapshot)
        return {
          state: 'invalid',
          observedAt,
          diagnostic: {
            code: 'INVALID_STORED_PRODUCT',
            message: 'Stored product fields or types violate the product contract.',
          },
        };
      const stored = target === 'MONGODB' ? { ...row, sourceRevision: snapshot.revision } : row;
      return { state: 'present', observedAt, snapshot, stored };
    } catch (error) {
      const result = failedOperation(error, target);
      return {
        state: 'unavailable',
        observedAt: new Date().toISOString(),
        diagnostic: result.diagnostic ?? {
          code: `${target}_UNAVAILABLE`,
          message: 'The destination read could not be confirmed.',
        },
      };
    }
  }

  return {
    read,
    /**
     * Attempt an immutable snapshot using at most three read/conditional-write rounds.
     * @param snapshot - Selected approved snapshot; its revision never changes during delivery.
     * @param deadline - Absolute delivery deadline in epoch milliseconds, shared by every round.
     * @returns Satisfied for a confirmed write, exact no-op, or newer valid copy; conflict for
     * equal-revision disagreement; failed for invalid evidence or recognized permanent errors;
     * retry for uncertainty/contention.
     * No-ops preserve timestamps and writes preserve existing destination-local fields.
     */
    async deliver(snapshot: Snapshot, deadline: number): Promise<TargetResult> {
      for (let round = 0; round < 3; round += 1) {
        const observed = await read(snapshot.productId, deadline);
        if (observed.state === 'unavailable') {
          const permanent =
            observed.diagnostic.code.endsWith('_CONFIGURATION_INVALID') ||
            observed.diagnostic.code.endsWith('_VALIDATION_FAILED');
          return {
            outcome: permanent ? 'failed' : 'retry',
            action: permanent ? 'invalid' : 'unavailable',
            diagnostic: observed.diagnostic,
          };
        }
        if (observed.state === 'invalid')
          return { outcome: 'failed', action: 'invalid', diagnostic: observed.diagnostic };
        const current = observed.state === 'present' ? observed.snapshot : null;
        if (current && current.revision > snapshot.revision)
          return { outcome: 'satisfied', action: 'superseded' };
        if (current && current.revision === snapshot.revision) {
          if (sameProduct(current.product, snapshot.product))
            return { outcome: 'satisfied', action: 'unchanged' };
          return {
            outcome: 'conflict',
            action: 'conflict',
            diagnostic: {
              code: 'EQUAL_REVISION_CONFLICT',
              message: 'The destination has different shared fields at the selected revision.',
            },
          };
        }
        try {
          if (await operations.write(snapshot, current?.revision ?? null, deadline)) {
            return { outcome: 'satisfied', action: current ? 'updated' : 'inserted' };
          }
        } catch (error) {
          // Timeouts can hide committed writes; only later observations can establish the effect.
          return failedOperation(error, target);
        }
      }
      return {
        outcome: 'retry',
        action: 'contention',
        diagnostic: {
          code: 'REVISION_CONTENTION',
          message: 'Concurrent product changes prevented confirmation within three rounds.',
        },
      };
    },
    /**
     * Probe the existing destination without mutation and sanitize connection failures.
     * @param deadline - Absolute probe deadline in epoch milliseconds.
     * @returns An availability object; failed probes resolve with available=false and a safe diagnostic.
     */
    async ready(deadline) {
      try {
        await operations.probe(deadline);
        return { available: true };
      } catch (error) {
        const diagnostic: Diagnostic = failedOperation(error, target).diagnostic ?? {
          code: `${target}_UNAVAILABLE`,
          message: 'The destination is unavailable.',
        };
        return { available: false, diagnostic };
      }
    },
    /**
     * Satisfy the destination lifecycle contract without closing shared database clients.
     * @returns Resolves immediately; the runtime owner remains responsible for client shutdown.
     */
    async close(): Promise<void> {},
  };
}

/**
 * Build SQL delivery over the existing products table; this adapter never creates schema.
 * @param pool - Runtime-owned PostgreSQL pool shared with admission; it is not closed here.
 * @returns A destination that borrows connections per query, preserves existing internal_note,
 * and leaves timestamps unchanged on no-ops. Construction does not query the database.
 * @example
 * const sql = createSqlDestination(pool);
 * const observation = await sql.read(productId, Date.now() + 3000);
 */
export function createSqlDestination(pool: Pool): ProductDestination {
  return createDestination('SQL', {
    /**
     * Read the documented SQL product columns, including diagnostic and local fields.
     * @param productId - Exact primary key to look up.
     * @param deadline - Absolute query deadline in epoch milliseconds.
     * @returns The stored row, or null after a successful lookup finds no match.
     * @throws If PostgreSQL access or the bounded query fails; never converts failure to absence.
     */
    async read(productId, deadline) {
      const result = await query(
        pool,
        `SELECT product_id, name, description, category,
        source_revision, applied_at, internal_note FROM public.products WHERE product_id=$1`,
        [productId],
        deadline,
      );
      return result.rows[0] ?? null;
    },
    /**
     * Insert a missing row or advance only the exact lower revision previously observed.
     * @param snapshot - Validated approved fields and source revision to persist.
     * @param expected - Observed lower revision, or null to attempt insertion with a null local note.
     * @param deadline - Absolute query deadline in epoch milliseconds.
     * @returns True for one confirmed row; false for a duplicate insert or rejected revision guard.
     * @throws If the write cannot be confirmed; a later read must establish any uncertain effect.
     */
    async write(snapshot, expected, deadline) {
      const values = [
        snapshot.productId,
        snapshot.product.name,
        snapshot.product.description,
        snapshot.product.category,
        snapshot.revision,
      ];
      if (expected === null) {
        const result = await query(
          pool,
          `INSERT INTO public.products
          (product_id, name, description, category, source_revision, internal_note)
          VALUES ($1,$2,$3,$4,$5,NULL) ON CONFLICT (product_id) DO NOTHING RETURNING source_revision`,
          values,
          deadline,
        );
        return result.rowCount === 1;
      }
      // Match the exact observed revision, not merely any lower value, to detect intervening writes.
      const result = await query(
        pool,
        `UPDATE public.products SET name=$2, description=$3,
        category=$4, source_revision=$5, applied_at=clock_timestamp()
        WHERE product_id=$1 AND source_revision=$6 AND source_revision<$5 RETURNING source_revision`,
        [...values, expected],
        deadline,
      );
      return result.rowCount === 1;
    },
    /**
     * Check access to expected product columns without reading rows or changing records.
     * @param deadline - Absolute query deadline in epoch milliseconds.
     * @returns Resolves after PostgreSQL accepts the zero-row projection.
     * @throws If access, expected columns, or the bounded query fails.
     */
    async probe(deadline) {
      await query(
        pool,
        `SELECT product_id, name, description, category, source_revision,
        applied_at, internal_note FROM public.products LIMIT 0`,
        [],
        deadline,
      );
    },
  });
}

/**
 * Build MongoDB delivery using explicit BSON revisions and acknowledged conditional operations.
 * @param client - Runtime-owned client configured with retryWrites=false for the standalone server.
 * @param database - Existing catalog database; never initialized by this adapter.
 * @returns A destination preserving all extra fields during managed-field updates; creating its
 * database/collection handles performs no network I/O and the caller retains client ownership.
 * @example
 * const catalog = createMongoDestination(client, config.mongoDatabase);
 * const availability = await catalog.ready(Date.now() + 3000);
 */
export function createMongoDestination(client: MongoClient, database: string): ProductDestination {
  const db = client.db(database);
  const products = db.collection<ProductDocument>('products');
  return createDestination('MONGODB', {
    /**
     * Read the complete document while retaining native BSON types for subsequent validation.
     * @param productId - Exact string _id to look up.
     * @param deadline - Absolute lookup deadline in epoch milliseconds.
     * @returns The stored document, or null after confirmed absence; Int32 remains distinct from Double.
     * @throws If connection, lookup, or deadline handling fails.
     */
    async read(productId, deadline) {
      return await products.findOne(
        { _id: productId },
        { ...mongoOptions(deadline), promoteValues: false },
      );
    },
    /**
     * Insert a missing document or update managed fields under the observed-revision predicate.
     * @param snapshot - Validated approved fields and source revision, serialized as BSON Int32.
     * @param expected - Observed lower revision, or null to insert with seoTitle initialized to null.
     * @param deadline - Absolute write deadline in epoch milliseconds.
     * @returns True for an acknowledged insert or matching update; false for a duplicate insert
     * or changed revision. Existing local fields survive $set updates.
     * @throws If the write fails or is unacknowledged; timeout/abort can hide a committed write.
     */
    async write(snapshot, expected, deadline) {
      const fields = {
        ...snapshot.product,
        sourceRevision: new Int32(snapshot.revision),
        appliedAt: new Date(),
      };
      const options = { ...mongoOptions(deadline), writeConcern: { w: 1 } };
      if (expected === null) {
        try {
          const result = await products.insertOne(
            { _id: snapshot.productId, ...fields, seoTitle: null },
            options,
          );
          if (!result.acknowledged) throw new Error('The insertion was not acknowledged.');
          return true;
        } catch (error) {
          if (errorCode(error) === 11000) return false;
          throw error;
        }
      }
      // A filtered upsert could confuse a newer existing document with absence; never upsert here.
      const result = await products.updateOne(
        { _id: snapshot.productId, sourceRevision: new Int32(expected) },
        { $set: fields },
        options,
      );
      if (!result.acknowledged) throw new Error('The update was not acknowledged.');
      return result.matchedCount === 1;
    },
    /**
     * Ping the server without creating or rewriting collections or validation rules.
     * @param deadline - Absolute probe deadline in epoch milliseconds.
     * @returns Resolves after a successful ping; this does not verify collection schema.
     * @throws If connection, command, or deadline handling fails.
     */
    async probe(deadline) {
      await db.command({ ping: 1 }, mongoOptions(deadline));
    },
  });
}
