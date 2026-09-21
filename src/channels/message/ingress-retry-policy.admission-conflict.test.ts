import { describe, it, expect } from "vitest";
import { SessionAdmissionConflictError } from "../../sessions/session-admission-conflict.js";
import {
  resolveIngressFailureDisposition,
  shouldDeadLetterRetryableIngressEvent,
  DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS,
} from "./ingress-retry-policy.js";

const formatError = (err: unknown) => (err instanceof Error ? err.message : String(err));

describe("admission-conflict retry disposition", () => {
  it("dead-letters a typed SessionAdmissionConflictError at attempt 3", () => {
    const result = resolveIngressFailureDisposition({
      err: new SessionAdmissionConflictError(
        "restart recovery claim changed before agent adoption",
      ),
      event: { receivedAt: 100, attempts: 2, lastAttemptAt: 200, lastError: "prev" },
      formatError,
    });
    expect(result.kind).toBe("fail");
    if (result.kind === "fail") {
      expect(result.reason).toBe("retry-limit-exceeded");
      expect(result.attempt).toBe(3);
    }
  });

  it("releases a typed SessionAdmissionConflictError at attempt 2", () => {
    const result = resolveIngressFailureDisposition({
      err: new SessionAdmissionConflictError("session changed before durable user-turn admission"),
      event: { receivedAt: 100, attempts: 1, lastAttemptAt: 200, lastError: "prev" },
      formatError,
    });
    expect(result.kind).toBe("release");
  });

  it("releases a generic Error at attempt 3 (generic cap is 5)", () => {
    const result = resolveIngressFailureDisposition({
      err: new Error("some random dispatch error"),
      event: { receivedAt: 100, attempts: 2, lastAttemptAt: 200, lastError: "prev" },
      formatError,
    });
    expect(result.kind).toBe("release");
  });

  it("dead-letters a generic Error at attempt 5", () => {
    const result = resolveIngressFailureDisposition({
      err: new Error("some random dispatch error"),
      event: { receivedAt: 100, attempts: 4, lastAttemptAt: 200, lastError: "prev" },
      formatError,
    });
    expect(result.kind).toBe("fail");
    if (result.kind === "fail") {
      expect(result.reason).toBe("retry-limit-exceeded");
      expect(result.attempt).toBe(5);
    }
  });

  it("dead-letters a wrapped admission error (fingerprint match) at attempt 3", () => {
    const wrapped = new Error("restart recovery claim changed before agent adoption");
    expect(wrapped).not.toBeInstanceOf(SessionAdmissionConflictError);
    const result = resolveIngressFailureDisposition({
      err: wrapped,
      event: { receivedAt: 100, attempts: 2, lastAttemptAt: 200, lastError: "prev" },
      formatError,
    });
    expect(result.kind).toBe("fail");
    if (result.kind === "fail") {
      expect(result.reason).toBe("retry-limit-exceeded");
      expect(result.attempt).toBe(3);
    }
  });

  it("releases a wrapped admission error (fingerprint match) at attempt 2", () => {
    const wrapped = new Error("session changed before durable user-turn admission");
    expect(wrapped).not.toBeInstanceOf(SessionAdmissionConflictError);
    const result = resolveIngressFailureDisposition({
      err: wrapped,
      event: { receivedAt: 100, attempts: 1, lastAttemptAt: 200, lastError: "prev" },
      formatError,
    });
    expect(result.kind).toBe("release");
  });

  it("dead-letters a wrapped before_agent_reply error at attempt 3", () => {
    const wrapped = new Error("before_agent_reply checkpoint lost restart recovery ownership");
    const result = resolveIngressFailureDisposition({
      err: wrapped,
      event: { receivedAt: 100, attempts: 2, lastAttemptAt: 200, lastError: "prev" },
      formatError,
    });
    expect(result.kind).toBe("fail");
    if (result.kind === "fail") {
      expect(result.attempt).toBe(3);
    }
  });
  it("keeps a typed conflict bounded through a nested cause without a fingerprint", () => {
    const wrapped = new Error("middleware failed", {
      cause: new SessionAdmissionConflictError("opaque admission conflict"),
    });
    expect(
      resolveIngressFailureDisposition({
        err: wrapped,
        event: { receivedAt: 100, attempts: 2 },
        formatError,
      }),
    ).toMatchObject({ kind: "fail", attempt: 3, reason: "retry-limit-exceeded" });
  });

  it("honors a configured budget lower than the admission conflict ceiling", () => {
    expect(
      resolveIngressFailureDisposition({
        err: new SessionAdmissionConflictError("opaque conflict"),
        event: { receivedAt: 100, attempts: 0 },
        config: { maxAttempts: 1 },
        formatError,
      }),
    ).toMatchObject({ kind: "fail", attempt: 1 });
  });
});

describe("opaque nested admission error candidates", () => {
  it.each(["null-prototype", "throwing-message", "throwing-toString"] as const)(
    "keeps %s causes inside the generic retry budget",
    (kind) => {
      const opaque =
        kind === "throwing-message"
          ? Object.defineProperty(new Error("opaque"), "message", {
              get() {
                throw new Error("message unavailable");
              },
            })
          : kind === "throwing-toString"
            ? {
                toString() {
                  throw new Error("conversion unavailable");
                },
              }
            : Object.create(null);
      const error = new Error("transport failed", { cause: opaque });
      for (const [attempts, expectedKind] of [
        [0, "release"],
        [4, "fail"],
      ] as const) {
        expect(
          resolveIngressFailureDisposition({
            err: error,
            event: { receivedAt: 100, attempts },
            formatError,
            now: 100,
          }),
        ).toMatchObject({ kind: expectedKind, attempt: attempts + 1 });
      }
    },
  );
});

describe("maxAttempts normalization via shouldDeadLetterRetryableIngressEvent", () => {
  // Test normalization indirectly: if maxAttempts is normalized correctly,
  // the dead-letter boundary shifts accordingly.
  const cases: Array<[string, number | undefined, number]> = [
    ["0 → 1 (dead-letters at attempt 1)", 0, 1],
    ["-5 → 1 (dead-letters at attempt 1)", -5, 1],
    ["NaN → default 5", Number.NaN, DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS],
    ["Infinity → default 5", Number.POSITIVE_INFINITY, DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS],
    ["0.5 → 1 (dead-letters at attempt 1)", 0.5, 1],
    ["21 → 20", 21, 20],
    ["3.7 → 3", 3.7, 3],
    ["undefined → default 5", undefined, DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS],
  ];
  for (const [label, input, expectedMax] of cases) {
    it(`normalizes maxAttempts: ${label}`, () => {
      // At the normalized limit, should dead-letter
      const atLimit = shouldDeadLetterRetryableIngressEvent(
        { receivedAt: 100, attempts: expectedMax - 1 },
        expectedMax,
        { maxAttempts: input },
      );
      expect(atLimit).toBe(true);
      // Below the normalized limit, should NOT dead-letter
      const belowLimit = shouldDeadLetterRetryableIngressEvent(
        { receivedAt: 100, attempts: expectedMax - 2 },
        expectedMax - 1,
        { maxAttempts: input },
      );
      expect(belowLimit).toBe(false);
    });
  }
});
