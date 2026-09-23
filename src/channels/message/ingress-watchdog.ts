/**
 * Direct-user message watchdog.
 *
 * FIX 6: Monitors direct-user messages in the ingress spool. If a direct-user
 * message remains undispatched beyond configured thresholds, the watchdog:
 *   - Logs WARNING at > 60s
 *   - Logs ERROR and triggers recovery at > 300s
 *
 * Recovery is rate-limited: max 1 restart per 5 minutes.
 * The watchdog runs every 30 seconds.
 *
 * Security boundaries preserved:
 * - Does NOT bypass C1-C4 protections or A0-A4 authority levels
 * - Does NOT modify audit fail-closed behavior
 * - Does NOT affect REQUIRE_ERIC stickiness
 * - Does NOT interfere with duplicate-dispatch prevention
 * - Respects uncertain-execution HOLD semantics
 */

import { formatErrorMessage } from "../../infra/errors.js";
import { isDirectUserMessage, type IngressPriorityMetadata } from "./ingress-priority.js";
import type { ChannelIngressQueue } from "./ingress-queue.js";

/** Watchdog configuration. */
export type DirectUserWatchdogConfig = {
  /** Interval between watchdog checks. Default: 30 seconds. */
  checkIntervalMs?: number;
  /** Threshold for WARNING log. Default: 60 seconds. */
  warningThresholdMs?: number;
  /** Threshold for ERROR log + recovery trigger. Default: 300 seconds. */
  errorThresholdMs?: number;
  /** Minimum time between recovery actions. Default: 5 minutes. */
  recoveryRateLimitMs?: number;
  /** Clock function for testing. */
  now?: () => number;
  /** Logger for watchdog output. */
  onLog?: (level: "warn" | "error", message: string, meta?: Record<string, unknown>) => void;
  /**
   * Recovery action triggered when a direct-user message is undispatched
   * beyond the error threshold. Implementations should perform session
   * rotation, then gateway restart if needed.
   *
   * The watchdog rate-limits calls to this function.
   */
  onRecovery?: (stuckMessage: DirectUserStuckMessage) => void | Promise<void>;
  /**
   * Set of sender IDs whose messages are treated as direct-user priority.
   * When set, events whose metadata.senderId matches are monitored.
   * If unset, only events with metadata.priority === "direct" are monitored.
   */
  prioritySenders?: ReadonlySet<string>;
};

/** Information about a stuck direct-user message. */
export type DirectUserStuckMessage = {
  eventId: string;
  channelId: string;
  accountId: string;
  queueName: string;
  receivedAt: number;
  ageMs: number;
  laneKey?: string;
  senderId?: string;
};

/** Internal state for the watchdog. */
type WatchdogState = {
  lastRecoveryAt: number;
  timer?: ReturnType<typeof setInterval>;
  running: boolean;
};

/** Default configuration values. */
const DEFAULT_CHECK_INTERVAL_MS = 30_000;
const DEFAULT_WARNING_THRESHOLD_MS = 60_000;
const DEFAULT_ERROR_THRESHOLD_MS = 300_000;
const DEFAULT_RECOVERY_RATE_LIMIT_MS = 5 * 60_000;

/**
 * Create a direct-user message watchdog.
 *
 * The watchdog periodically scans the ingress queue for undispatched direct-user
 * messages. When a message exceeds the warning or error threshold, it logs and
 * optionally triggers recovery.
 *
 * Usage:
 *   const watchdog = createDirectUserWatchdog({
 *     queue: myQueue,
 *     onLog: (level, msg, meta) => logger[level](msg, meta),
 *     onRecovery: (stuck) => restartGateway(),
 *   });
 *   watchdog.start();
 *   // ... later
 *   watchdog.stop();
 */
export function createDirectUserWatchdog<
  TPayload,
  TMetadata extends IngressPriorityMetadata | undefined,
>(
  config: {
    queue: ChannelIngressQueue<TPayload, TMetadata>;
  } & DirectUserWatchdogConfig,
) {
  const checkIntervalMs = config.checkIntervalMs ?? DEFAULT_CHECK_INTERVAL_MS;
  const warningThresholdMs = config.warningThresholdMs ?? DEFAULT_WARNING_THRESHOLD_MS;
  const errorThresholdMs = config.errorThresholdMs ?? DEFAULT_ERROR_THRESHOLD_MS;
  const recoveryRateLimitMs = config.recoveryRateLimitMs ?? DEFAULT_RECOVERY_RATE_LIMIT_MS;
  const now = config.now ?? Date.now;
  const prioritySenders = config.prioritySenders;

  const state: WatchdogState = {
    lastRecoveryAt: 0,
    running: false,
  };

  const log = (level: "warn" | "error", message: string, meta?: Record<string, unknown>) => {
    config.onLog?.(level, message, meta);
  };

  /**
   * Scan the ingress queue for undispatched direct-user messages.
   * Returns stuck messages that exceeded the warning or error threshold.
   */
  const scanForStuckMessages = async (): Promise<DirectUserStuckMessage[]> => {
    const pending = await config.queue.listPending({ limit: "all" });
    const currentTime = now();
    const stuck: DirectUserStuckMessage[] = [];

    for (const event of pending) {
      if (!isDirectUserMessage(event.metadata, prioritySenders)) {
        continue;
      }
      const ageMs = currentTime - event.receivedAt;
      if (ageMs >= warningThresholdMs) {
        stuck.push({
          eventId: event.id,
          channelId: event.channelId,
          accountId: event.accountId,
          queueName: event.queueName,
          receivedAt: event.receivedAt,
          ageMs,
          ...(event.laneKey !== undefined ? { laneKey: event.laneKey } : {}),
          ...(event.metadata?.senderId !== undefined ? { senderId: event.metadata.senderId } : {}),
        });
      }
    }

    return stuck;
  };

  /**
   * Handle a stuck direct-user message.
   * Logs WARNING or ERROR based on age, triggers recovery if past error threshold
   * and rate limit allows.
   */
  const handleStuckMessage = async (stuck: DirectUserStuckMessage): Promise<void> => {
    const displayId = stuck.eventId.replace(/^0+(?=\d)/, "") || stuck.eventId;

    if (stuck.ageMs >= errorThresholdMs) {
      log(
        "error",
        `Direct-user message ${displayId} undispatched for ${Math.floor(stuck.ageMs / 1000)}s; triggering recovery`,
        {
          eventId: stuck.eventId,
          channelId: stuck.channelId,
          accountId: stuck.accountId,
          ageMs: stuck.ageMs,
          laneKey: stuck.laneKey,
          senderId: stuck.senderId,
        },
      );

      // Rate-limited recovery: max 1 restart per recoveryRateLimitMs.
      const currentTime = now();
      if (currentTime - state.lastRecoveryAt >= recoveryRateLimitMs) {
        state.lastRecoveryAt = currentTime;
        try {
          await config.onRecovery?.(stuck);
        } catch (err) {
          log("error", `Direct-user watchdog recovery action failed: ${formatErrorMessage(err)}`, {
            eventId: stuck.eventId,
          });
        }
      } else {
        log(
          "warn",
          `Direct-user watchdog recovery suppressed (rate limit: ${Math.floor(recoveryRateLimitMs / 1000)}s)`,
          {
            eventId: stuck.eventId,
            lastRecoveryAt: state.lastRecoveryAt,
          },
        );
      }
    } else {
      // Warning threshold (60s <= age < 300s).
      log(
        "warn",
        `Direct-user message ${displayId} undispatched for ${Math.floor(stuck.ageMs / 1000)}s`,
        {
          eventId: stuck.eventId,
          channelId: stuck.channelId,
          accountId: stuck.accountId,
          ageMs: stuck.ageMs,
          laneKey: stuck.laneKey,
          senderId: stuck.senderId,
        },
      );
    }
  };

  /**
   * Run one watchdog check cycle.
   */
  const checkOnce = async (): Promise<void> => {
    try {
      const stuckMessages = await scanForStuckMessages();
      // Sort by age descending — oldest first so recovery targets the worst case.
      stuckMessages.sort((a, b) => b.ageMs - a.ageMs);
      for (const stuck of stuckMessages) {
        await handleStuckMessage(stuck);
      }
    } catch (err) {
      log("error", `Direct-user watchdog check failed: ${formatErrorMessage(err)}`);
    }
  };

  return {
    /** Start the watchdog. Runs periodic checks until stopped. */
    start: () => {
      if (state.running) {
        return;
      }
      state.running = true;
      state.timer = setInterval(() => {
        void checkOnce();
      }, checkIntervalMs);
      state.timer.unref?.();
    },

    /** Stop the watchdog. */
    stop: () => {
      state.running = false;
      if (state.timer) {
        clearInterval(state.timer);
        state.timer = undefined;
      }
    },

    /** Run a single check cycle. Useful for testing. */
    checkOnce,

    /** Whether the watchdog is currently running. */
    isRunning: () => state.running,

    /** Expose internal state for testing. */
    _getRecoveryState: () => ({
      lastRecoveryAt: state.lastRecoveryAt,
      running: state.running,
    }),
  };
}

export type DirectUserWatchdog = ReturnType<typeof createDirectUserWatchdog>;
