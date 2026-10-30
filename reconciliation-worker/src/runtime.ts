/**
 * @file Validates startup compatibility, then schedules leased attempts through queue wakeups.
 * Runtime borrows repositories; the executable owns driver pools and process termination.
 */
import type { Job, JobStatus, Claim } from './model/job.js';
import type { Logger } from './model/contracts.js';
import { SystemicError } from './model/diagnostic.js';
import { listenForJobsSql } from './resources/postgres/connection.js';
export interface RuntimeDependencies {
  jobs: {
    /** Atomically commit ownership just before processing. */ claim(
      deadline: number,
    ): Promise<Claim>;
  };
  /** Process committed ownership within its absolute deadline. */ process(
    job: Job,
    deadline: number,
  ): Promise<{ status: JobStatus | 'lost' | 'unknown'; job: Job | null }>;
  /** Validate startup compatibility; reject unavailable probes for retry before any claims. */ ready(): Promise<void>;
  log: Logger;
}
export interface ListenerConnection {
  /** Connect without subscribing; the driver may return its client. */ connect(): Promise<unknown>;
  /** Execute the fixed LISTEN command. */ query(text: string): Promise<unknown>;
  /** Close the dedicated connection. */ end(): Promise<void>;
  /** Register driver lifecycle handlers before connecting. */ on(
    event: string,
    handler: (...args: unknown[]) => void,
  ): unknown;
}
export type ContinuousOptions = {
  listenerFactory: () => ListenerConnection;
  signal: AbortSignal;
  scanIntervalMs?: number;
  reconnectDelayMs?: number;
  /** Notify the executable before draining, so it can enforce a process-wide shutdown deadline. */
  onStopping?: () => void;
};
/**
 * Subscribe before scanning, validate startup once, and recover listener outages independently of polling.
 * Failed startup checks retry on later wakeups; successful validation remains valid for this run.
 * @param deps - Borrowed queue and processing boundaries; only one claim is processed at a time.
 * @param options - Listener factory and stop signal; small test intervals override production defaults.
 * @returns After abort stops new claims, the current bounded attempt drains and listeners close.
 * @throws SystemicError after cleanup when schema/authentication prevents safe processing.
 */
export async function runContinuous(
  deps: RuntimeDependencies,
  options: ContinuousOptions,
): Promise<void> {
  const startupStarted = Date.now();
  let validated = false;
  let stopped = options.signal.aborted,
    pending = false,
    active: Promise<void> | undefined;
  let connection: ListenerConnection | undefined,
    reconnect: NodeJS.Timeout | undefined,
    polling: NodeJS.Timeout | undefined;
  let connecting: Promise<void> | undefined,
    fatal: SystemicError | undefined,
    failures = 0;
  const closing = new Set<Promise<void>>();
  let finish: () => void = /** Replaced by the completion promise constructor below. */ () => {};
  const finished = new Promise<void>(
    /** Retain only the resolver; lifecycle events own completion. */ (resolve) => {
      finish = resolve;
    },
  );

  /** Stop scheduling immediately; the active attempt remains bounded by its own deadline. */
  function stop(): void {
    if (!stopped) {
      stopped = true;
      // Arm the executable's watchdog before awaiting driver closure, including fatal startup paths.
      options.onStopping?.();
    }
    clearTimeout(reconnect);
    clearInterval(polling);
    finish();
  }

  /** Handle systemic errors as a stop and transient failures as retryable connection uncertainty. */
  function failed(error: unknown): void {
    if (error instanceof SystemicError) {
      fatal = error;
      deps.log('worker_fatal', error.diagnostic);
      stop();
    } else
      deps.log('worker_unavailable', {
        outcome: 'unconfirmed',
        message: 'A dependency operation could not be confirmed; polling will retry.',
      });
  }

  /** Drain due work serially and rescan only when a coalesced signal requests it. */
  async function drain(): Promise<void> {
    while (pending && !stopped) {
      pending = false;
      if (!validated) {
        await deps.ready();
        validated = true;
        // A completed startup probe cannot authorize claims after shutdown was requested.
        if (stopped) return;
        deps.log('worker_ready', { outcome: 'ready', durationMs: Date.now() - startupStarted });
      }
      while (!stopped) {
        const claimed = await deps.jobs.claim(Date.now() + 3000);
        if (claimed.kind === 'none') break;
        if (claimed.kind === 'claimed') await deps.process(claimed.job, Date.now() + 15000);
      }
    }
  }

  /** Start one drain or record that a later scan is required; notification payloads are not claims. */
  function wake(): void {
    if (stopped) return;
    pending = true;
    if (!active) {
      active = drain()
        .catch(failed)
        .finally(
          /** Release the single drain slot after every outcome. */ () => {
            active = undefined;
            if (pending && !stopped) {
              pending = false;
              wake();
            }
          },
        );
    }
  }

  /** Close a listener once and retain its cleanup promise for bounded executable shutdown. */
  function closeListener(client: ListenerConnection): void {
    const closure = client
      .end()
      .catch(
        /** Closure errors are reported without exposing the connection. */ () => {
          deps.log('listener_close_failed', { outcome: 'unconfirmed' });
        },
      )
      .finally(
        /** Remove settled cleanup promises to keep long-running memory bounded. */ () => {
          closing.delete(closure);
        },
      );
    closing.add(closure);
  }

  /** Schedule a fresh listener with capped exponential backoff; polling is independent. */
  function scheduleReconnect(): void {
    if (stopped || reconnect) return;
    const delay = Math.min(
      30000,
      (options.reconnectDelayMs ?? 1000) * 2 ** Math.min(failures++, 5),
    );
    reconnect = setTimeout(
      /** Connect again without starting overlapping attempts. */ () => {
        reconnect = undefined;
        connecting = subscribe();
      },
      delay,
    );
  }

  /** Register error handlers before connecting and acknowledge LISTEN before scanning backlog. */
  async function subscribe(): Promise<void> {
    if (stopped) return;
    const client = options.listenerFactory();
    connection = client;
    /** Release this connection once; duplicate error/end events cannot schedule duplicate reconnects. */
    function disconnected(): void {
      if (connection !== client) return;
      connection = undefined;
      deps.log('listener_disconnected', { outcome: 'polling_continues' });
      closeListener(client);
      scheduleReconnect();
    }
    client.on(
      'error',
      /** Classify permission failures even when the driver reports them as events. */ (error) => {
        if (isAuthenticationError(error))
          failed(
            new SystemicError(
              'POSTGRES_AUTHENTICATION_FAILED',
              'PostgreSQL listener authentication or access failed.',
            ),
          );
        disconnected();
      },
    );
    client.on('end', disconnected);
    client.on(
      'notification',
      /** Treat notifications solely as a request to inspect durable table state. */ () => wake(),
    );
    try {
      await client.connect();
      if (stopped || connection !== client) return;
      await client.query(listenForJobsSql);
      if (stopped || connection !== client) return;
      failures = 0;
      deps.log('listener_subscribed', { outcome: 'listening' });
      wake();
    } catch (error) {
      if (isAuthenticationError(error))
        failed(
          new SystemicError(
            'POSTGRES_AUTHENTICATION_FAILED',
            'PostgreSQL listener authentication or access failed.',
          ),
        );
      else if (error instanceof SystemicError) failed(error);
      disconnected();
    }
  }

  options.signal.addEventListener('abort', stop, { once: true });
  try {
    if (!stopped) {
      connecting = subscribe();
      await connecting;
      if (!stopped) {
        polling = setInterval(wake, options.scanIntervalMs ?? 30000);
        // A failed initial LISTEN must not disable table polling while the query pool is healthy.
        if (!connection) wake();
      }
    } else finish();
    await finished;
  } finally {
    stop();
    options.signal.removeEventListener('abort', stop);
    await connecting;
    await active;
    const current = connection;
    connection = undefined;
    if (current) closeListener(current);
    await Promise.all(closing);
    deps.log('worker_drained', { outcome: fatal ? 'fatal' : 'stopped' });
  }
  if (fatal) throw fatal;
}

/** Check controlled PostgreSQL access error codes without inspecting secret-bearing messages. */
function isAuthenticationError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    typeof error.code === 'string' &&
    (error.code.startsWith('28') || error.code === '42501' || error.code === '3D000')
  );
}
