/**
 * @file Emits structured worker events to stderr with credential and payload filtering.
 * The logger never exposes private lease ownership or raw driver errors.
 */
import type { LogLevel } from './config.js';
import type { Logger } from './model/contracts.js';

/**
 * Recursively remove private values before serializing operational logs.
 * @param value - Structured event fields; callers should supply controlled diagnostics.
 * @returns A sanitized copy without credential, lease, raw error or product payload fields.
 */
function sanitize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sanitize);
  if (value instanceof Error) return { code: 'OPERATION_UNCONFIRMED' };
  if (typeof value === 'string' && /(?:postgres(?:ql)?|mongodb(?:\+srv)?):\/\//iu.test(value))
    return '[redacted]';
  if (!value || typeof value !== 'object') return value;
  const output: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    if (
      /lease_?token|password|credential|connection|postgres_?url|mongo_?uri|approved_?payload|conflict_?details|^product$/iu.test(
        key,
      )
    )
      continue;
    output[key] = sanitize(item);
  }
  return output;
}

/**
 * Build a JSON logger writing one record to stderr per meaningful event.
 * @param level - Minimum severity; silent suppresses all records.
 * @param sink - Optional test sink; defaults to console.error for container log collection.
 * @returns A callback accepting event names and structured context, never raw driver errors.
 */
export function createLogger(
  level: LogLevel,
  sink: (record: string) => void = console.error,
): Logger {
  const rank = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };
  return /** Classify and sanitize one event after its caller confirms the outcome. */ (
    event,
    fields,
  ) => {
    const requested = typeof fields.level === 'string' ? fields.level.toLowerCase() : undefined;
    const severity =
      requested === 'debug' || requested === 'info' || requested === 'warn' || requested === 'error'
        ? requested
        : /failed|invalid|fatal|exhausted/u.test(event)
          ? 'error'
          : /retry|unavailable|conflict|lost|uncertain|disconnected/u.test(event)
            ? 'warn'
            : 'info';
    if (rank[severity] < rank[level]) return;
    const safe = sanitize(fields);
    sink(
      JSON.stringify({
        ...(typeof safe === 'object' && safe !== null ? safe : {}),
        timestamp: new Date().toISOString(),
        level: severity,
        component: 'reconciliation-worker',
        event,
      }),
    );
  };
}
