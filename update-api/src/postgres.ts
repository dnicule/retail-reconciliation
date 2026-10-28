/**
 * @file Bounds PostgreSQL connection acquisition and statement execution for API adapters.
 * Acquires and releases caller-owned pool connections, discards connections after errors,
 * and keeps local deadlines distinct from cancellation or proof of a write's outcome.
 * Transaction boundaries and interpretation of database results belong to the caller.
 */

import type { Pool, PoolClient, QueryResult } from 'pg';

export interface BoundedClient {
  /**
   * Execute a statement on the connection reserved for the enclosing action.
   * @param text - SQL statement; use placeholders for external values.
   * @param values - Placeholder values, or an empty array when omitted.
   * @returns The acknowledged result, bounded by three seconds and the enclosing deadline.
   * @throws If the budget expires or the driver rejects; a submitted write may have committed.
   */
  query(text: string, values?: unknown[]): Promise<QueryResult<Record<string, unknown>>>;
}

/** Signals an elapsed local deadline; an already submitted statement can still have committed. */
class DatabaseDeadlineError extends Error {
  /**
   * Create the local timeout error without connection information or SQL text.
   * Construction does not cancel an operation or determine whether its write committed.
   */
  constructor() {
    super('PostgreSQL operation deadline exceeded');
  }
}

/**
 * Wait for an existing operation only until its local wall-clock deadline.
 * The timer is cleared on settlement; Promise.race continues observing the losing promise.
 * @param work - Already-started operation, which can continue after this wrapper rejects.
 * @param deadline - Absolute Unix timestamp in milliseconds for the local wait limit.
 * @returns The operation's value if it fulfills before the deadline timer.
 * @throws If the operation rejects or the timer expires; timeout does not imply cancellation.
 */
async function before<T>(work: Promise<T>, deadline: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const expired = new Promise<never>(
    /** Rejects when the operation's wall-clock allowance expires. */ (_resolve, reject) => {
      timer = setTimeout(
        /** Reports uncertainty rather than pretending the operation was cancelled. */ () =>
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
 * Acquire one pool connection within three seconds and the caller's remaining budget.
 * A connection delivered after timeout is released instead of being left checked out.
 * @param pool - Caller-owned pool, which remains open after acquisition or failure.
 * @param deadline - Absolute Unix timestamp in milliseconds limiting acquisition.
 * @returns An exclusively acquired client that the caller must release or discard.
 * @throws If the deadline has passed, acquisition times out, or the pool rejects.
 */
async function acquire(pool: Pool, deadline: number): Promise<PoolClient> {
  if (deadline <= Date.now()) throw new DatabaseDeadlineError();
  let abandoned = false;
  const connecting = pool.connect().then(
    /** Returns an on-time client or safely releases an unused late one. */ (client) => {
      if (abandoned) client.release();
      return client;
    },
  );
  try {
    return await before(connecting, Math.min(deadline, Date.now() + 3000));
  } catch (error) {
    abandoned = true;
    throw error;
  }
}

/**
 * Run an action on one acquired connection and release it when the action settles.
 * The action must commit or roll back any transaction before returning. Query or action
 * failures cause the connection to be discarded, preventing reuse of an open transaction
 * or uncertain protocol exchange. The deadline bounds acquisition and each statement,
 * not arbitrary work inside the action.
 * @param pool - Caller-owned pool; this helper never closes it.
 * @param deadline - Absolute Unix timestamp in milliseconds shared by acquisition and queries.
 * @param action - Awaited callback receiving a client valid only during this invocation.
 * @returns The callback's result after releasing or discarding its connection.
 * @throws If acquisition, a query, or the callback fails; timed-out writes can be committed.
 * @example
 * const result = await withClient(pool, Date.now() + 3000, async (client) => {
 *   return client.query('SELECT job_id FROM public.reconciliation_jobs WHERE job_id = $1', [jobId]);
 * });
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
     * Submit a bounded statement and mark its connection for disposal on failure.
     * @param text - SQL statement to execute on the acquired connection.
     * @param values - Placeholder values; defaults to no parameters.
     * @returns The acknowledged driver result within three seconds and the shared deadline.
     * @throws On an expired deadline or driver failure; submitted writes remain uncertain.
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
        throw error;
      }
    },
  };
  try {
    return await action(bounded);
  } catch (error) {
    destroy = true;
    throw error;
  } finally {
    client.release(destroy);
  }
}

/**
 * Execute one statement with a three-second budget covering acquisition and execution.
 * The supplied deadline can shorten that budget. The acquired connection is released
 * after success or discarded after failure; the pool remains open.
 * @param pool - Caller-owned pool from which to acquire one connection.
 * @param text - SQL statement using placeholders for externally supplied values.
 * @param values - Parameters bound to the statement's placeholders.
 * @param deadline - Absolute Unix timestamp in milliseconds limiting the entire operation.
 * @returns The acknowledged driver result, including rows and affected-row information.
 * @throws If acquisition or execution fails or times out; a write may already have committed.
 */
export async function query(
  pool: Pool,
  text: string,
  values: unknown[],
  deadline: number,
): Promise<QueryResult<Record<string, unknown>>> {
  const operationDeadline = Math.min(deadline, Date.now() + 3000);
  return withClient(
    pool,
    operationDeadline,
    /** Submits this operation on its exclusively acquired client. */ (client) =>
      client.query(text, values),
  );
}
