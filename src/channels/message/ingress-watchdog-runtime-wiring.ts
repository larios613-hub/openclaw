/**
 * Watchdog runtime integration for ingress-monitor.ts
 * 
 * This file contains the integration code to wire the direct-user message
 * watchdog (FIX 6) into the channel ingress monitor lifecycle.
 * 
 * Integration point: src/channels/message/ingress-monitor.ts
 * The monitor's start() and stop() functions are the lifecycle hooks.
 * 
 * This is a SPEC + IMPLEMENTATION GUIDE for Codex to apply.
 * It should NOT be applied directly to the production dist — it requires
 * a full build via GitHub Actions and Eric's deployment authorization.
 */

// === INTEGRATION CODE ===
// 
// Add to the imports section of ingress-monitor.ts:
//
// import { createDirectUserWatchdog } from "./ingress-watchdog.js";
// import type { DirectUserWatchdogConfig } from "./ingress-watchdog.js";
//
// Add to the IngressMonitorOptions type:
//
// /** Optional direct-user message watchdog configuration. */
// watchdog?: DirectUserWatchdogConfig & {
//   /** Whether the watchdog is enabled. Default: false (opt-in). */
//   enabled?: boolean;
// };
//
// Add inside the createIngressMonitor function body, after the drain creation:
//
// let watchdog: ReturnType<typeof createDirectUserWatchdog> | undefined;
//
// const ensureWatchdog = () => {
//   if (watchdog || !options.watchdog?.enabled) return;
//   watchdog = createDirectUserWatchdog({
//     queue: getQueue(),
//     ...options.watchdog,
//     onLog: (level, message, meta) => {
//       reportError(new Error(`[watchdog] ${message}`));
//       if (level === "warn") {
//         options.onLog?.(`[watchdog:warn] ${message}`);
//       } else {
//         options.onLog?.(`[watchdog:error] ${message}`);
//       }
//     },
//     onRecovery: async (stuck) => {
//       // Recovery: send SIGUSR1 to trigger gateway restart
//       // The rate limiter in the watchdog prevents restart loops
//       process.kill(process.pid, "SIGUSR1");
//     },
//   });
// };
//
// In the start() function, after `running = true;`:
//
//   ensureWatchdog();
//   watchdog?.start();
//
// In the stop() function, before `drain?.dispose();`:
//
//   watchdog?.stop();
//   watchdog = undefined;
//
// === REGRESSION TEST ===
//
// Test that the watchdog starts and stops with the ingress monitor lifecycle:
//
// 1. Create an ingress monitor with watchdog enabled
// 2. Start the monitor → verify watchdog.start() was called
// 3. Stop the monitor → verify watchdog.stop() was called
// 4. Create a monitor without watchdog → verify no watchdog is created
// 5. Start → stop → start again → verify watchdog is recreated
//
// === SECURITY BOUNDARIES ===
//
// - Watchdog is opt-in (enabled: false by default)
// - Does NOT bypass C1-C4 or A0-A4
// - Recovery action goes through normal SIGUSR1 restart path
// - Rate-limited: max 1 restart per 5 minutes
// - Cannot be disabled by untrusted input (config comes from trusted channel config)
// - Does NOT interfere with duplicate-dispatch prevention or audit fail-closed