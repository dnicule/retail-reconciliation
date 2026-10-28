/**
 * @file Defines sanitized diagnostics and the worker's shared error categories.
 * Lifecycle owners stop on systemic faults; malformed snapshots stay isolated to their jobs.
 */
export type Diagnostic = { code: string; message: string; [key: string]: unknown };

/** Signals an infrastructure fault that must stop claiming rather than exhaust product jobs. */
export class SystemicError extends Error {
  readonly diagnostic: Diagnostic;

  /** Preserve a sanitized infrastructure code and explanation for the lifecycle owner. */
  constructor(code: string, message: string) {
    super(message);
    this.name = 'SystemicError';
    this.diagnostic = { code, message };
  }
}

/** Isolates malformed retained product evidence to its job rather than stopping all claims. */
export class InvalidSnapshotError extends Error {
  readonly diagnostic: Diagnostic;

  /** Preserve sanitized validation details for a single job's attention result. */
  constructor(code: string, message: string) {
    super(message);
    this.name = 'InvalidSnapshotError';
    this.diagnostic = { code, message };
  }
}
