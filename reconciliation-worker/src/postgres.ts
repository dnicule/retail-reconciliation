/**
 * @file Bounds PostgreSQL acquisition and statements for worker repositories and stores.
 * Borrows caller-owned clients and discards uncertain exchanges without claiming cancellation.
 * Transactions and interpretation of acknowledged results remain the caller's responsibility.
 */
import type { Pool, PoolClient, QueryResult } from 'pg';
import { SystemicError } from './model/diagnostic.js';

export interface BoundedClient {
  /**
   * Execute a parameterized statement on the enclosing action's exclusive connection.
   * @param text - SQL statement, using placeholders for external values.
   * @param values - Placeholder values; defaults to an empty list.
   * @returns The acknowledged result before the shared deadline and three-second statement cap.
   * @throws On deadline or driver failure; submitted writes can already have committed.
   */
  query(text: string, values?: unknown[]): Promise<QueryResult<Record<string, unknown>>>;
}

/** Signals an elapsed local wait; it provides no evidence that a submitted write was cancelled. */
class DatabaseDeadlineError extends Error {
  /** Creates a sanitized local timeout without statement text or connection information. */
  constructor() {
    super('PostgreSQL operation deadline exceeded');
    this.name = 'DatabaseDeadlineError';
  }
}

/**
 * Convert recognized infrastructure failures into a stop-claiming signal.
 * @param error - Untrusted driver failure; its message is never copied into a systemic diagnostic.
 * @returns The original transient error or a sanitized systemic error.
 */
function classified(error: unknown): unknown {
  if (error instanceof SystemicError) return error;
  const code =
    typeof error === 'object' && error !== null && 'code' in error ? error.code : undefined;
  if (
    typeof code === 'string' &&
    ['28P01', '28000', '42501', '42P01', '42703', '42804', '42883', '3D000', '3F000'].includes(code)
  ) {
    return new SystemicError(
      'POSTGRES_CONFIGURATION_INVALID',
      'PostgreSQL authentication, permissions, or schema require attention.',
    );
  }
  return error;
}

/**
 * Await an existing operation until its absolute Unix millisecond deadline.
 * The losing promise remains observed; timer cleanup does not imply server cancellation.
 * @param work - Already-started asynchronous operation.
 * @param deadline - Absolute end of the local wait allowance.
 * @returns The acknowledged operation value before expiry.
 * @throws The operation's failure or a local deadline error.
 */
async function before<T>(work: Promise<T>, deadline: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const expired = new Promise<never>(
    /** Rejects when the bounded local waiting period expires. */ (_resolve, reject) => {
      timer = setTimeout(
        /** Reports timeout while retaining uncertainty about any write. */ () =>
          reject(new DatabaseDeadlineError()),
        Math.max(0, deadline - Date.now()),
      );
    },
  );
  try {
    return await Promise.race([work, expired]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Acquire a connection and release any late result after an abandoned wait.
 * @param pool - Caller-owned connection pool, never closed here.
 * @param deadline - Absolute Unix millisecond limit, additionally capped at three seconds.
 * @returns An exclusive client whose release becomes the caller's responsibility.
 * @throws A systemic error for recognized infrastructure faults, otherwise the acquisition error.
 */
async function acquire(pool: Pool, deadline: number): Promise<PoolClient> {
  if (deadline <= Date.now()) throw new DatabaseDeadlineError();
  let abandoned = false;
  const connecting = pool.connect().then(
    /** Returns an on-time client or releases an unused late arrival. */ (client) => {
      if (abandoned) client.release();
      return client;
    },
  );
  try {
    return await before(connecting, Math.min(deadline, Date.now() + 3000));
  } catch (error) {
    abandoned = true;
    throw classified(error);
  }
}

/**
 * Run an action on one connection, releasing on success and discarding after uncertainty.
 * The callback must finish its transaction before returning. Acquisition and each query are
 * bounded; arbitrary callback computation is owned by the caller's attempt deadline.
 * @param pool - Caller-owned pool, which remains open.
 * @param deadline - Absolute Unix millisecond end shared by acquisition and all statements.
 * @param action - Awaited operation using the exclusively borrowed client.
 * @returns The callback result after its connection is released.
 * @throws On acquisition, statement, or action failure; submitted writes may have committed.
 */
export async function withClient<T>(
  pool: Pool,
  deadline: number,
  action: (client: BoundedClient) => Promise<T>,
): Promise<T> {
  const client = await acquire(pool, deadline);
  let destroy = false;
  const bounded: BoundedClient = {
    /**
     * Execute a bounded statement, marking the connection for disposal after failure.
     * @param text - Parameterized SQL statement.
     * @param values - Placeholder values, or an empty list.
     * @returns The acknowledged result before expiry.
     * @throws A classified driver failure or local deadline error.
     */
    async query(text, values = []) {
      try {
        if (deadline <= Date.now()) throw new DatabaseDeadlineError();
        return await before(
          client.query<Record<string, unknown>>(text, values),
          Math.min(deadline, Date.now() + 3000),
        );
      } catch (error) {
        destroy = true;
        throw classified(error);
      }
    },
  };
  try {
    return await action(bounded);
  } catch (error) {
    destroy = true;
    throw classified(error);
  } finally {
    client.release(destroy);
  }
}

/**
 * Execute one statement with a three-second total acquisition and execution allowance.
 * @param pool - Caller-owned query pool; this helper never closes it.
 * @param text - SQL with placeholders for untrusted data.
 * @param values - Bound placeholder values.
 * @param deadline - Absolute Unix millisecond limit that can shorten the operation allowance.
 * @returns The acknowledged query result after releasing the connection.
 * @throws On acquisition, deadline, or statement failure; write failure can hide a committed write.
 */
export async function query(
  pool: Pool,
  text: string,
  values: unknown[],
  deadline: number,
): Promise<QueryResult<Record<string, unknown>>> {
  return withClient(
    pool,
    Math.min(deadline, Date.now() + 3000),
    /** Uses the operation's exclusively acquired connection. */ (client) =>
      client.query(text, values),
  );
}
