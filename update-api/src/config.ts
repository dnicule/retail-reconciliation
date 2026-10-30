/**
 * @file Configuration validation and bounded client settings for the standalone API.
 * Reads caller-supplied environment values, applies listener/catalog defaults, and rejects
 * invalid targets before runtime startup. This module does not load .env files or connect.
 */
import { isIP } from 'node:net';
import { MongoClient } from 'mongodb';
import type { MongoClientOptions } from 'mongodb';
import type { PoolConfig } from 'pg';
type LogLevel = 'fatal' | 'error' | 'warn' | 'info' | 'debug' | 'trace' | 'silent';
export type Config = {
  host: string;
  port: number;
  logLevel: LogLevel;
  postgresUrl: string;
  mongoUri: string;
  mongoDatabase: string;
};

/** Marks validation messages whose controlled text can safely appear in startup diagnostics. */
export class ConfigurationError extends Error {
  /**
   * Mark a controlled validation message as safe for startup diagnostics.
   * @param message - Static explanation naming configuration keys, never secret-bearing values.
   */
  constructor(message: string) {
    super(message);
    this.name = 'ConfigurationError';
  }
}

/**
 * Validate an IP address or DNS hostname before it reaches the HTTP listener.
 * @param value - Explicit host value or the caller's loopback default.
 * @returns The original host, retaining a valid DNS trailing dot when present.
 * @throws {ConfigurationError} If the value is neither an IP address nor a valid hostname.
 */
function host(value: string): string {
  if (isIP(value)) return value;
  const normalized = value.endsWith('.') ? value.slice(0, -1) : value;
  if (
    !normalized ||
    normalized.length > 253 ||
    !normalized
      .split('.')
      .every(
        /** Accepts only well-formed DNS labels, including localhost. */ (label) =>
          /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/.test(label),
      )
  )
    throw new ConfigurationError('HOST must be a valid IP address or hostname.');
  return value;
}

/**
 * Validate a configured TCP port; test listeners may use port zero directly with buildApp.
 * @param value - Decimal digits from configuration, without signs or whitespace.
 * @returns An integer listener port from 1 through 65,535.
 * @throws {ConfigurationError} If the value is malformed or outside the listener range.
 */
function port(value: string): number {
  const number = Number(value);
  if (!/^[0-9]+$/.test(value) || !Number.isInteger(number) || number < 1 || number > 65535) {
    throw new ConfigurationError('PORT must be an integer from 1 through 65535.');
  }
  return number;
}

/**
 * Validate the logger level instead of silently falling back after a typo.
 * @param value - Case-sensitive Fastify logger level from configuration.
 * @returns The supported level, including silent when explicitly selected.
 * @throws {ConfigurationError} If the value is not a supported level.
 */
function level(value: string): LogLevel {
  for (const allowed of ['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'] as const)
    if (value === allowed) return allowed;
  throw new ConfigurationError(
    'LOG_LEVEL must be fatal, error, warn, info, debug, trace, or silent.',
  );
}

/**
 * Validate an explicit PostgreSQL database path and reject query overrides of its identity.
 * @param value - Required connection URL; credentials remain private to the caller.
 * @returns The original URL after checking its scheme, host, escapes, and database name.
 * @throws {ConfigurationError} If missing, malformed, or ambiguous; the error omits URL contents.
 */
function postgresUrl(value: string | undefined): string {
  if (!value) throw new ConfigurationError('POSTGRES_URL is required.');
  try {
    const parsed = new URL(value);
    if (!['postgres:', 'postgresql:'].includes(parsed.protocol) || !parsed.hostname || parsed.hash)
      throw new Error();
    // node-postgres decodes these fields at connection time; catch malformed escapes before startup.
    decodeURIComponent(parsed.username);
    decodeURIComponent(parsed.password);
    const database = decodeURIComponent(parsed.pathname.slice(1));
    if (!database || /[/\u0000]/u.test(database)) throw new Error();
    // Match parameter names case-insensitively so alternate casing cannot hide an ambiguous target.
    for (const key of parsed.searchParams.keys()) {
      if (['db', 'dbname', 'database', 'connectionstring'].includes(key.toLowerCase()))
        throw new Error();
    }
  } catch {
    throw new ConfigurationError(
      'POSTGRES_URL must be a valid postgres: or postgresql: URL naming one database in its path, without database query overrides.',
    );
  }
  return value;
}

/**
 * Validate MongoDB connection syntax through the driver without initiating a connection.
 * @param value - Required URI, including any caller-managed authentication settings.
 * @returns The original driver-accepted URI; reachability and permissions are not checked.
 * @throws {ConfigurationError} If missing or rejected by the driver; the error omits URI contents.
 */
function mongoUri(value: string | undefined): string {
  if (!value) throw new ConfigurationError('MONGODB_URI is required.');
  try {
    new MongoClient(value);
  } catch {
    throw new ConfigurationError(
      'MONGODB_URI must be a valid mongodb: or mongodb+srv: connection URI.',
    );
  }
  return value;
}

/**
 * Validate the catalog name against the project's conservative database-name contract.
 * @param value - Explicit catalog name or the caller's retail_catalog default.
 * @returns The original nonempty name of at most 63 UTF-8 bytes, without forbidden characters.
 * @throws {ConfigurationError} If the name violates the length or character restrictions.
 */
function mongoDatabase(value: string): string {
  if (!value || Buffer.byteLength(value) > 63 || /[\s/\\."$\u0000]/u.test(value)) {
    throw new ConfigurationError(
      'MONGODB_DATABASE must be 1–63 bytes without spaces, slashes, backslashes, dots, quotes, dollar signs, or null bytes.',
    );
  }
  return value;
}

/**
 * Read startup settings without changing the environment or opening connections.
 * The runtime loads .env separately; required database URIs have no fallback values.
 * @param env - Environment to read; defaults to the current process environment.
 * @returns Validated targets with default host 127.0.0.1, port 43187, log level info,
 * and MongoDB database retail_catalog when the corresponding settings are absent.
 * @throws {ConfigurationError} If a required URI is absent or any configured value is invalid.
 * @example
 * const config = readConfig();
 * const options = databaseOptions(config);
 */
export function readConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    host: host(env.HOST ?? '127.0.0.1'),
    port: port(env.PORT ?? '43187'),
    logLevel: level(env.LOG_LEVEL ?? 'info'),
    postgresUrl: postgresUrl(env.POSTGRES_URL),
    mongoUri: mongoUri(env.MONGODB_URI),
    mongoDatabase: mongoDatabase(env.MONGODB_DATABASE ?? 'retail_catalog'),
  };
}

/**
 * Build bounded pool and driver options without constructing or connecting clients.
 * Per-operation adapters further shorten the three-second limits to the remaining deadline.
 * @param config - Validated startup settings supplying the PostgreSQL connection URL.
 * @returns Options for pools of at most five connections; MongoDB driver retries are disabled
 * so the API's explicit delivery outcomes retain control of retry decisions.
 */
export function databaseOptions(config: Config): {
  postgres: PoolConfig;
  mongo: MongoClientOptions;
} {
  return {
    postgres: {
      connectionString: config.postgresUrl,
      max: 5,
      connectionTimeoutMillis: 3000,
      idleTimeoutMillis: 10_000,
      statement_timeout: 3000,
      query_timeout: 3000,
      idle_in_transaction_session_timeout: 3000,
      application_name: 'retailer-update-api',
    },
    mongo: {
      maxPoolSize: 5,
      minPoolSize: 0,
      maxConnecting: 2,
      retryWrites: false,
      retryReads: false,
      serverSelectionTimeoutMS: 3000,
      connectTimeoutMS: 3000,
      waitQueueTimeoutMS: 3000,
      socketTimeoutMS: 3000,
      timeoutMS: 3000,
    },
  };
}
