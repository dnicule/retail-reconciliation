/**
 * @file Builds MongoDB product filters and repair documents without executing operations.
 * Callers supply validated snapshots and timestamps; BSON revisions remain explicit Int32 values.
 */
import { Int32 } from 'mongodb';
import type { Filter, FindOptions, InsertOneOptions, UpdateFilter } from 'mongodb';
import type { Snapshot } from '../../model/product.js';

/** Raw product evidence retains unknown BSON field values until the adapter validates them. */
export type ProductDocument = { _id: string; [key: string]: unknown };

/** Existing catalog collection, shared by product operations and read-only readiness checks. */
export const productCollectionName = 'products';

/** Preserve native BSON types while reading the complete stored document without a projection. */
export const productReadOptions = { promoteValues: false } satisfies Pick<
  FindOptions,
  'promoteValues'
>;

/** Require an acknowledged single-server write; operation deadlines remain adapter-owned. */
export const productWriteOptions = { writeConcern: { w: 1 } } satisfies Pick<
  InsertOneOptions,
  'writeConcern'
>;

/**
 * Match one product by its exact source identifier without transforming it.
 * @param productId - Validated shared identifier to read.
 * @returns A new primary-key filter; construction performs no I/O.
 */
export function productIdentityFilter(productId: string): Filter<ProductDocument> {
  return { _id: productId };
}

/**
 * Fence a repair by the exact BSON revision previously observed at this destination.
 * @param productId - Validated shared identifier to repair.
 * @param expected - Observed source revision; the adapter verifies that the selected revision is newer.
 * @returns A new equality filter that cannot match a different concurrent revision.
 */
export function productRevisionFilter(
  productId: string,
  expected: number,
): Filter<ProductDocument> {
  return { _id: productId, sourceRevision: new Int32(expected) };
}

/**
 * Construct a missing product copy from approved fields and the caller's application timestamp.
 * @param snapshot - Validated immutable source evidence selected by reconciliation.
 * @param appliedAt - Timestamp selected by the adapter for this attempted write, never revision authority.
 * @returns A new insertion document with explicit BSON revision and a null local SEO title.
 */
export function productInsertDocument(snapshot: Snapshot, appliedAt: Date): ProductDocument {
  return {
    _id: snapshot.productId,
    ...snapshot.product,
    sourceRevision: new Int32(snapshot.revision),
    appliedAt,
    seoTitle: null,
  };
}

/**
 * Construct a managed-field update that leaves every destination-local field unchanged.
 * @param snapshot - Validated selected evidence whose revision exceeds the observed revision.
 * @param appliedAt - Adapter-supplied timestamp for this attempted advancement.
 * @returns A new $set update containing only shared fields, source revision, and application time.
 */
export function productUpdateDocument(
  snapshot: Snapshot,
  appliedAt: Date,
): UpdateFilter<ProductDocument> {
  return {
    $set: {
      ...snapshot.product,
      sourceRevision: new Int32(snapshot.revision),
      appliedAt,
    },
  };
}
