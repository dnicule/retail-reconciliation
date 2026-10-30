/**
 * @file Defines fixed PostgreSQL transaction and notification statements.
 * Callers own connection lifetime and execution order; these statements bind no values.
 */
export const beginTransactionSql = 'BEGIN';
export const commitTransactionSql = 'COMMIT';
export const listenForJobsSql = 'LISTEN reconciliation_ready';
