/**
 * @file Checks worker database prerequisites using read-only catalog queries and commands.
 * Probes inspect existing schema, validators, indexes, triggers, and effective access without
 * initializing data or importing the database tools' mutating verifier.
 */
import { isDeepStrictEqual } from 'node:util';
import type { MongoClient } from 'mongodb';
import type { Pool } from 'pg';
import type { Logger } from './model/contracts.js';
import { SystemicError } from './model/diagnostic.js';
import { query } from './postgres.js';
import { productCollectionName } from './resources/mongodb/products.js';
import {
  connectionPrivilegesCommand,
  expectedProductCollection,
  expectedProductIndex,
  mongoValidator,
  productCollectionMetadataCommand,
  productIndexMetadataCommand,
  requiredProductActions,
} from './resources/mongodb/readiness.js';
import { readinessSql } from './resources/postgres/readiness.js';

/**
 * Coordinate read-only startup validation while retaining each confirmed database for this process.
 * @param postgresProbe - Full PostgreSQL compatibility probe accepting an absolute deadline.
 * @param mongoProbe - Full MongoDB compatibility probe accepting an absolute deadline.
 * @param log - Structured logger receiving only confirmed success or controlled transient details.
 * @returns A callback that concurrently checks unconfirmed targets with three-second budgets.
 * @throws SystemicError before any competing transient rejection; otherwise the failed probe reason.
 */
export function createStartupReadiness(
  postgresProbe: (deadline: number) => Promise<void>,
  mongoProbe: (deadline: number) => Promise<void>,
  log: Logger,
): () => Promise<void> {
  const probes = [
    { target: 'postgresql', probe: postgresProbe, validated: false },
    { target: 'mongodb', probe: mongoProbe, validated: false },
  ];
  return /** Validate pending targets; the runtime alone decides when to retry after rejection. */ async () => {
    const results = await Promise.allSettled(
      probes
        .filter(
          /** Never repeat a successfully completed compatibility check. */ (probe) =>
            !probe.validated,
        )
        .map(
          /** Independently capture each result so one failure cannot prevent the other check. */ async (
            entry,
          ) => {
            const started = Date.now();
            try {
              await entry.probe(started + 3000);
              entry.validated = true;
              log('database_validated', {
                target: entry.target,
                outcome: 'ready',
                durationMs: Date.now() - started,
              });
            } catch (error) {
              if (!(error instanceof SystemicError)) {
                // Driver failures may contain credentials; report availability without raw evidence.
                log('startup_check_unavailable', {
                  level: 'warn',
                  target: entry.target,
                  outcome: 'unknown',
                  code: 'STARTUP_CHECK_UNAVAILABLE',
                  message:
                    'Startup compatibility could not be confirmed; this dependency will be checked again.',
                  durationMs: Date.now() - started,
                });
              }
              throw error;
            }
          },
        ),
    );
    // A transient failure in one target must not conceal incompatible schema or credentials in another.
    const systemic = results.find(
      /** Select a failure that requires process shutdown rather than another startup attempt. */ (
        result,
      ) => result.status === 'rejected' && result.reason instanceof SystemicError,
    );
    if (systemic?.status === 'rejected') throw systemic.reason;
    for (const result of results) if (result.status === 'rejected') throw result.reason;
  };
}

/**
 * Bound MongoDB selection, lazy connection, and execution with the same allowance.
 * @param deadline - Absolute Unix millisecond end of the caller's remaining budget.
 * @returns A positive timeout capped at three seconds and an independent abort signal.
 * @throws If no millisecond remains; no database work should then start.
 */
export function mongoOptions(deadline: number): { timeoutMS: number; signal: AbortSignal } {
  const timeoutMS = Math.min(3000, Math.floor(deadline - Date.now()));
  if (timeoutMS <= 0) throw new Error('MongoDB operation deadline exceeded.');
  // The abort signal also covers lazy connection before the driver's operation timeout exists.
  return { timeoutMS, signal: AbortSignal.timeout(timeoutMS) };
}

/**
 * Stop processing for recognized MongoDB authentication, authorization, or missing-schema faults.
 * @param error - Driver error; raw messages and connection strings are never exposed.
 * @throws SystemicError for infrastructure faults; transient and document-validation errors return.
 */
export function rethrowMongoSystemic(error: unknown): void {
  if (error instanceof SystemicError) throw error;
  let current = error;
  for (let depth = 0; depth < 4 && typeof current === 'object' && current !== null; depth += 1) {
    const value = current as Record<string, unknown>;
    if ([13, 18, 26, 73].includes(typeof value.code === 'number' ? value.code : -1)) {
      throw new SystemicError(
        'MONGODB_CONFIGURATION_INVALID',
        'MongoDB authentication, permissions, or schema require attention.',
      );
    }
    current = value.cause;
  }
}

/** Returns a plain object for safely inspecting untrusted metadata, or an empty object. */
function record(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

/** Returns first-batch catalog entries without interpreting missing metadata as valid schema. */
function firstBatch(value: unknown): Record<string, unknown>[] {
  const batch = record(record(value).cursor).firstBatch;
  return Array.isArray(batch)
    ? batch.map(/** Narrows each untrusted catalog entry. */ (entry: unknown) => record(entry))
    : [];
}

/**
 * Check authenticated privileges needed for product reads and repairs.
 * @param response - Read-only connectionStatus result with showPrivileges enabled.
 * @param database - Existing catalog database whose products collection the worker uses.
 * @returns True for sufficient grants, including unrestricted authentication-disabled deployments.
 */
function permitted(response: unknown, database: string): boolean {
  const auth = record(record(response).authInfo);
  if (!Array.isArray(auth.authenticatedUsers)) return false;
  if (auth.authenticatedUsers.length === 0) return true;
  if (!Array.isArray(auth.authenticatedUserPrivileges)) return false;
  const actions = new Set<string>();
  for (const raw of auth.authenticatedUserPrivileges) {
    const privilege = record(raw);
    const resource = record(privilege.resource);
    const applies =
      resource.anyResource === true ||
      ((resource.db === '' || resource.db === database) &&
        (resource.collection === '' || resource.collection === productCollectionName));
    if (applies && Array.isArray(privilege.actions)) {
      for (const action of privilege.actions) if (typeof action === 'string') actions.add(action);
    }
  }
  return (
    actions.has('anyAction') ||
    requiredProductActions.every(
      /** Requires every native product operation's permission. */ (action) => actions.has(action),
    )
  );
}

/**
 * Inspect MongoDB's existing product collection, exact validator, index, and effective access.
 * @param client - Caller-owned MongoDB client, never closed here.
 * @param database - Existing catalog database name.
 * @param deadline - Absolute Unix millisecond limit, capped at three seconds for this whole probe.
 * @returns Resolves after read-only metadata proves worker compatibility.
 * @throws SystemicError for incompatibility/access faults, otherwise a transient driver/deadline error.
 */
export async function assertMongoReady(
  client: MongoClient,
  database: string,
  deadline: number,
): Promise<void> {
  const end = Math.min(deadline, Date.now() + 3000);
  const db = client.db(database);
  try {
    const collections = firstBatch(
      await db.command(productCollectionMetadataCommand(), mongoOptions(end)),
    );
    const collection = collections[0];
    const options = record(collection?.options);
    if (
      collections.length !== 1 ||
      collection?.type !== expectedProductCollection.type ||
      options.validationLevel !== expectedProductCollection.validationLevel ||
      options.validationAction !== expectedProductCollection.validationAction ||
      !isDeepStrictEqual(options.validator, mongoValidator)
    ) {
      throw new SystemicError(
        'MONGODB_SCHEMA_INCOMPATIBLE',
        'The existing products collection or strict validator differs from the supported schema.',
      );
    }
    const indexes = firstBatch(await db.command(productIndexMetadataCommand(), mongoOptions(end)));
    if (
      indexes.length !== 1 ||
      indexes[0]?.name !== expectedProductIndex.name ||
      !isDeepStrictEqual(indexes[0]?.key, expectedProductIndex.key)
    ) {
      throw new SystemicError(
        'MONGODB_SCHEMA_INCOMPATIBLE',
        'The products collection must retain only its built-in identifier index.',
      );
    }
    const privileges = await db.command(connectionPrivilegesCommand(), mongoOptions(end));
    if (!permitted(privileges, database))
      throw new SystemicError(
        'MONGODB_PERMISSION_DENIED',
        'The current MongoDB principal lacks required product read, insert, update, or index-inspection privileges.',
      );
  } catch (error) {
    rethrowMongoSystemic(error);
    throw error;
  }
}

/**
 * Inspect PostgreSQL table shapes, required constraints/indexes/triggers, migration receipt, and access.
 * This checks worker prerequisites, not every possible malicious alteration of database functions.
 * @param pool - Caller-owned PostgreSQL query pool.
 * @param deadline - Absolute Unix millisecond limit for acquisition and the read-only catalog query.
 * @returns Resolves when the required existing schema and privileges are present.
 * @throws SystemicError for schema/access incompatibility, otherwise a transient query/deadline error.
 */
export async function assertPostgresReady(pool: Pool, deadline: number): Promise<void> {
  const result = await query(pool, readinessSql, [], deadline);
  if (result.rows[0]?.ready !== true)
    throw new SystemicError(
      'POSTGRES_SCHEMA_INCOMPATIBLE',
      'The existing PostgreSQL schema, migration receipt, triggers, indexes, or permissions differ from worker prerequisites.',
    );
}
