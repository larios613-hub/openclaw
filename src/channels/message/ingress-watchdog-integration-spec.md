# Watchdog Runtime Integration Specification

## Status: READY FOR CODEX IMPLEMENTATION

## Problem

The direct-user message watchdog (FIX 6) is implemented as a standalone library (`ingress-watchdog.ts`) but is not wired into any runtime component. Felix's M2 finding confirmed: no imports found outside its own module and test file. The watchdog delivers zero operational value until integrated.

## Integration Point

The watchdog must be started when the Telegram channel (or any channel with direct-user messages) becomes live, and stopped when the channel shuts down.

## Required Wiring

### 1. Channel Live Runtime Integration

In the channel's live runtime module (where `createChannelIngressDrain` is called):

```typescript
import { createDirectUserWatchdog } from "./ingress-watchdog.js";

// After creating the ingress drain and queue:
const watchdog = createDirectUserWatchdog({
  queue: ingressQueue,
  prioritySenders: configuredPrioritySenders, // from channel config
  onLog: (level, message, meta) => {
    gatewayLog[level](`[watchdog] ${message}`, meta);
  },
  onRecovery: async (stuck) => {
    // Recovery: session rotation, then gateway restart
    gatewayLog.error(`[watchdog] Recovery triggered for message ${stuck.eventId}`);
    // 1. Attempt session rotation for the affected session
    // 2. If rotation fails, trigger gateway restart via SIGUSR1
    process.kill(process.pid, "SIGUSR1");
  },
});

// Start watchdog when channel goes live
watchdog.start();

// Stop watchdog on channel shutdown
// (add to cleanup/dispose chain)
```

### 2. Configuration

Add to channel configuration:

```json
{
  "ingress": {
    "watchdog": {
      "enabled": true,
      "checkIntervalMs": 30000,
      "warningThresholdMs": 60000,
      "errorThresholdMs": 300000,
      "recoveryRateLimitMs": 300000
    },
    "prioritySenders": ["7855174845"]
  }
}
```

### 3. Security Boundaries

- Watchdog does NOT bypass C1-C4 or A0-A4
- Watchdog does NOT modify audit fail-closed behavior
- Watchdog does NOT affect REQUIRE_ERIC stickiness
- Watchdog does NOT interfere with duplicate-dispatch prevention
- Recovery action (session rotation / gateway restart) goes through normal restart path
- Rate-limited: max 1 restart per 5 minutes

### 4. Test Requirements

- Unit test: watchdog starts and stops with channel lifecycle
- Integration test: watchdog detects stuck message and triggers recovery
- Regression test: watchdog does not trigger on dispatched messages
- Adversarial test: watchdog cannot be disabled by untrusted input
- Rate-limit test: recovery does not fire more than once per 5 minutes

### 5. Rollback

- Watchdog is opt-in (disabled by default)
- Can be disabled via config without affecting other ingress functionality
- No schema changes required

## Implementation Priority

P2 — needed before P1/P2 production deployment but not blocking E1 gate evaluation (gates already pass via CI/synthetic tests).
