# ADR 0002: Structural Isolation of Replay Side Effects

## Status
Accepted

## Context
Replaying historical transaction events must reproduce logic and failures without ever executing real financial debits or external notifications.

## Decision
1. Event handlers (`replay.Handler`) are pure functions taking `(ControlledDeps, TransactionState, Event)` and returning `(newState, effects, error)` without performing I/O.
2. Time and IDs come from injected `Clock` and `IDGenerator` interfaces (`EventClock` and `DeterministicIDGenerator` during replay; `SystemClock` and `RandomIDGenerator` during live execution).
3. `replay.Engine` and `replay.RecoveryManager` instantiate `SimulatedEffectSink` directly and accept no `LiveEffectSink`, enforcing isolation at compile time.

## Consequences
- Repeated replays of identical inputs produce identical canonical `TransactionState.Hash()` digests.
- Live external gateways cannot be triggered from replay paths even by misconfiguration.
