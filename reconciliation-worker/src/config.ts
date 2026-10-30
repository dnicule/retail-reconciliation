/**
 * @file Validates worker configuration and constructs bounded driver options.
 * This module never opens connections or loads credentials from sibling packages.
 */
import { MongoClient } from 'mongodb';
import type { MongoClientOptions } from 'mongodb';
import type { PoolConfig } from 'pg';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';
export type Config = {
  postgresUrl: string;
  mongoUri: string;
  mongoDatabase: string;
  logLevel: LogLevel;
};

/** Controlled configuration errors contain setting names without their secret values. */
export class ConfigurationError extends Error {
  /** Mark a static configuration explanation as safe to print. @param message - Secret-free explanation. */
  constructor(message: string) {
    super(message);
    this.name = 'ConfigurationError';
  }
}

/**
 * Validate explicit database targets before any driver connects.
 * @param env - Environment values; defaults to this process after Node loads its local .env.
 * @returns Required connection settings plus catalog/log defaults.
 * @throws ConfigurationError for absent, ambiguous or invalid settings; no URI is exposed.
 */
export function readConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const postgresUrl = env.POSTGRES_URL,
    mongoUri = env.MONGODB_URI;
  if (!postgresUrl || !mongoUri)
    throw new ConfigurationError('POSTGRES_URL and MONGODB_URI are required.');
  try {
    const url = new URL(postgresUrl);
    const database = decodeURIComponent(url.pathname.slice(1));
    decodeURIComponent(url.username);
    decodeURIComponent(url.password);
    if (
      !['postgres:', 'postgresql:'].includes(url.protocol) ||
      !url.hostname ||
      url.hash ||
      !database ||
      /[/\u0000]/u.test(database)
    )
      throw new Error();
    for (const key of url.searchParams.keys())
      if (['db', 'dbname', 'database', 'connectionstring'].includes(key.toLowerCase()))
        throw new Error();
  } catch {
    throw new ConfigurationError(
      'POSTGRES_URL must name one database in a valid PostgreSQL URI without database query overrides.',
    );
  }
  try {
    new MongoClient(mongoUri);
  } catch {
    throw new ConfigurationError('MONGODB_URI must be a valid MongoDB URI.');
  }
  const mongoDatabase = env.MONGODB_DATABASE ?? 'retail_catalog';
  if (
    !mongoDatabase ||
    Buffer.byteLength(mongoDatabase) > 63 ||
    /[\s/\\."$\u0000]/u.test(mongoDatabase)
  ) {
    throw new ConfigurationError(
      'MONGODB_DATABASE must be a valid nonempty database name of at most 63 bytes.',
    );
  }
  const level = env.LOG_LEVEL ?? 'info';
  if (
    level !== 'debug' &&
    level !== 'info' &&
    level !== 'warn' &&
    level !== 'error' &&
    level !== 'silent'
  ) {
    throw new ConfigurationError('LOG_LEVEL must be debug, info, warn, error, or silent.');
  }
  return { postgresUrl, mongoUri, mongoDatabase, logLevel: level };
}

/**
 * Construct pool settings; adapters further cap each operation by the remaining deadline.
 * @param config - Validated connection settings owned by the caller.
 * @returns Options for five connections each and disabled implicit MongoDB retries.
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
      application_name: 'retailer-reconciliation-worker',
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
