/**
 * @file Defines immutable product snapshots and validates their shared revision contract.
 * These pure helpers inspect managed fields without reading or writing either destination.
 */
import type { Diagnostic } from './diagnostic.js';

export type SharedProduct = { name: string; description: string; category: string };
export type Snapshot = { productId: string; revision: number; product: SharedProduct };

/**
 * Validate the shared product identifier without normalizing accepted spelling.
 * @param value - Untrusted identifier from persisted product or job evidence.
 * @returns True only for the shared ASCII identifier contract.
 */
export function validProductId(value: unknown): value is string {
  return typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(value);
}

/**
 * Compare only managed content, preserving exact accepted string values.
 * @param a - First already validated product.
 * @param b - Second already validated product.
 * @returns Whether all three shared fields agree; destination metadata is excluded.
 */
export function sameProduct(a: SharedProduct, b: SharedProduct): boolean {
  return a.name === b.name && a.description === b.description && a.category === b.category;
}

/**
 * Check an untrusted snapshot before using it as revision authority.
 * @param value - Candidate projection; destination-local fields belong outside its product.
 * @param expectedProductId - When supplied, reject evidence belonging to another product.
 * @returns A sanitized validation diagnostic, or null when the candidate is valid.
 */
export function validateSnapshot(value: unknown, expectedProductId?: string): Diagnostic | null {
  const diagnostic: Diagnostic = {
    code: 'INVALID_SNAPSHOT',
    message: 'Snapshot violates the product identifier, revision, or shared field contract.',
  };
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return diagnostic;
  const candidate = value as Record<string, unknown>;
  if (
    !validProductId(candidate.productId) ||
    (expectedProductId !== undefined && candidate.productId !== expectedProductId)
  )
    return diagnostic;
  if (
    typeof candidate.revision !== 'number' ||
    !Number.isInteger(candidate.revision) ||
    candidate.revision < 1 ||
    candidate.revision > 2_147_483_647
  )
    return diagnostic;
  if (
    typeof candidate.product !== 'object' ||
    candidate.product === null ||
    Array.isArray(candidate.product)
  )
    return diagnostic;
  const product = candidate.product as Record<string, unknown>;
  if (Object.keys(product).length !== 3) return diagnostic;
  for (const [field, min, max] of [
    ['name', 1, 200],
    ['description', 0, 4_000],
    ['category', 1, 100],
  ] as const) {
    const content = product[field];
    if (typeof content !== 'string') return { ...diagnostic, field };
    // PostgreSQL and MongoDB measure characters, so count Unicode code points rather than UTF-16 units.
    const length = [...content].length;
    if (length < min || length > max) return { ...diagnostic, field };
  }
  return null;
}
