import { computeBackoff } from "../../infra/backoff.js";
/**
 * Generic ingress retry backoff and dead-letter decisions.
 *
 * Channel-specific non-retryable classification stays out of core; pass it in.
 */
import { isAdmissionConflictError } from "../../sessions/session-admission-conflict.js";

export const DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS = 5;
export const DEFAULT_INGRESS_RETRY_DEAD_LETTER_MIN_AGE_MS = 24 * 60 * 60 * 1000;
export const DEFAULT_INGRESS_RETRY_BASE_MS = 1_000;
export const DEFAULT_INGRESS_RETRY_MAX_MS = 3 * 60_000;

export type IngressRetryPolicyConfig = {
  maxAttempts?: number;
  /** Legacy hint; never delays the hard attempt limit. */
  deadLetterMinAgeMs?: number;
  baseMs?: number;
  maxMs?: number;
};

type IngressRetryEventFacts = {
  receivedAt: number;
  attempts?: number;
  lastAttemptAt?: number;
  lastError?: string;
};

export type IngressNonRetryableFailure = {
  reason: string;
  message: string;
};

type IngressFailureDisposition =
  | {
      kind: "fail";
      reason: string;
      message: string;
      attempt: number;
    }
  | {
      kind: "release";
      attempt: number;
      message: string;
    };

function resolveConfig(config?: IngressRetryPolicyConfig) {
  return {
    maxAttempts: Number.isFinite(config?.maxAttempts)
      ? Math.max(1, Math.min(20, Math.floor(config!.maxAttempts!)))
      : DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS,
    deadLetterMinAgeMs: config?.deadLetterMinAgeMs ?? DEFAULT_INGRESS_RETRY_DEAD_LETTER_MIN_AGE_MS,
    baseMs: config?.baseMs ?? DEFAULT_INGRESS_RETRY_BASE_MS,
    maxMs: config?.maxMs ?? DEFAULT_INGRESS_RETRY_MAX_MS,
  };
}

/** Next attempt number after a failed dispatch (1-based for the attempt just finished). */
function resolveIngressAttemptNumber(event: IngressRetryEventFacts): number {
  return (event.attempts ?? 0) + 1;
}

/** Remaining backoff delay before a released event may be claimed again. */
export function resolveIngressRetryDelayMs(
  event: IngressRetryEventFacts,
  config?: IngressRetryPolicyConfig,
  now = Date.now(),
): number {
  const { baseMs, maxMs } = resolveConfig(config);
  const attempts = event.attempts ?? 0;
  if (!event.lastError || event.lastAttemptAt === undefined || attempts <= 0) {
    return 0;
  }
  const delayMs = computeBackoff(
    { initialMs: baseMs, maxMs, factor: 2, jitter: 0 },
    Math.min(attempts, 9),
  );
  return Math.max(0, event.lastAttemptAt + delayMs - now);
}

/**
 * An age gate must never extend a poison event's hard attempt budget.
 * Failed rows retain the original payload for explicit operator replay.
 *
 * @param _event Unused after the hard attempt limit replaced the age gate.
 * @param _now Unused after the hard attempt limit replaced the age gate.
 */
export function shouldDeadLetterRetryableIngressEvent(
  _event: IngressRetryEventFacts,
  attempt: number,
  config?: IngressRetryPolicyConfig,
  _now = Date.now(),
): boolean {
  const { maxAttempts } = resolveConfig(config);
  return attempt >= maxAttempts;
}

/** Resolve release vs fail for a dispatch error using optional non-retryable hook. */
export function resolveIngressFailureDisposition(params: {
  err: unknown;
  event: IngressRetryEventFacts;
  formatError: (err: unknown) => string;
  resolveNonRetryableFailure?: (err: unknown) => IngressNonRetryableFailure | null;
  config?: IngressRetryPolicyConfig;
  now?: number;
}): IngressFailureDisposition {
  const now = params.now ?? Date.now();
  const attempt = resolveIngressAttemptNumber(params.event);
  const message = params.formatError(params.err);
  const nonRetryable = params.resolveNonRetryableFailure?.(params.err) ?? null;
  if (nonRetryable) {
    return {
      kind: "fail",
      reason: nonRetryable.reason,
      message: nonRetryable.message,
      attempt,
    };
  }
  // Repeated ownership conflicts cannot be repaired by replaying the same admission.
  // Preserve the event after three failures; never bypass the session owner.
  // Use isAdmissionConflictError (fingerprint fallback) so wrapped/coerced errors
  // still receive the 3-attempt cap instead of the generic 5-attempt default.
  const config = isAdmissionConflictError(params.err)
    ? { ...params.config, maxAttempts: Math.min(3, resolveConfig(params.config).maxAttempts) }
    : params.config;
  if (shouldDeadLetterRetryableIngressEvent(params.event, attempt, config, now)) {
    return {
      kind: "fail",
      reason: "retry-limit-exceeded",
      message,
      attempt,
    };
  }
  return { kind: "release", attempt, message };
}
