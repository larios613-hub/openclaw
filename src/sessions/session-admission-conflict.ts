/** A durable admission comparison failed; execution remains fenced. */
export class SessionAdmissionConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SessionAdmissionConflictError";
  }
}

/**
 * Error messages that indicate an admission conflict even if the typed error
 * was wrapped or coerced into a generic Error. This is a durable fallback so
 * the 3-attempt admission-conflict cap survives error wrapping.
 */
const ADMISSION_CONFLICT_FINGERPRINTS = [
  "restart recovery claim changed before agent adoption",
  "session changed before durable user-turn admission",
  "before_agent_reply checkpoint lost restart recovery ownership",
  "before_agent_reply start lost restart recovery ownership",
  "channel restart recovery requires source-keyed user-turn admission",
];

/**
 * Returns true if the error is a SessionAdmissionConflictError or if its
 * message matches a known admission-conflict fingerprint. This ensures the
 * 3-attempt admission-conflict retry cap applies even when the typed error
 * is wrapped/coerced by an intermediate layer.
 */
export function isAdmissionConflictError(err: unknown): boolean {
  if (err instanceof SessionAdmissionConflictError) {
    return true;
  }
  if (err instanceof Error) {
    const msg = err.message;
    return ADMISSION_CONFLICT_FINGERPRINTS.some((fp) => msg.includes(fp));
  }
  return false;
}
