# Clean Code and SRP Audit

## Summary

- **Highest-leverage future split:** separate WebSocket connection/reconnect
  state from offline queue and subscription orchestration in
  `StreamlineClient`.
- `AdminClient` changes for topics, groups, schemas, query, semantic search, and
  HTTP transport, but public route compatibility must be decided first.
- `Models.kt` is a public serialization contract; length alone is not an SRP
  violation.
- Current lifecycle tests do not pin reconnect cancellation, queue drain
  ordering, and concurrent close strongly enough for a safe extraction.
- Compiler warnings identify tautological type assertions in tests; those can
  be removed without weakening behavior coverage.

## Findings

| ID | Location | Category | Severity | Actors in conflict | Cost | Size | Behavior risk |
|---|---|---|---|---|---|---|---|
| KOTLIN-SRP-1 | `StreamlineClient.kt` | SRP, stateful client | P1 | WebSocket platform; reconnect reliability; offline product; subscriptions; transactions | Independent lifecycle actors share mutexes, scopes, queue, and connection state. | L | High |
| KOTLIN-SRP-2 | `AdminClient.kt` | SRP, mixed admin surface | P2 | topic/group ops; schemas; query/search; HTTP transport | Route and response changes for unrelated APIs edit one client. | L | High |
| KOTLIN-CC-1 | test sources reported by compiler | Tautological assertions | P2 | test maintainers | `is` checks on statically known values add noise and emit warnings without testing behavior. | S | None |
| KOTLIN-D-1 | topic admin routes | Cross-repo contract | P1 | core API owner; Kotlin consumers | `/v1/topics` differs from the core `/api/v1/topics` surface. | M | High |

## Ordered Refactor Sequence

1. Remove tautological test assertions while retaining value/error assertions.
2. Add virtual-time tests for reconnect cancellation, queue drain order,
   concurrent close, and resubscription.
3. Move WebSocket session state unchanged into an internal transport unit.
4. Modify transport state only after the move is green.
5. Defer Admin route consolidation until the HTTP contract is approved.

## Deferred

- Stateful client extraction lacks sufficient lifecycle characterization.
- Admin route changes require the org API decision.
- Public ErrorCode/model consolidation requires a versioning decision.

## Out of Scope

- `Models.kt`: public serialized model contract.
- Telemetry wrappers: one observability actor.
- Circuit breaker: one reliability state machine.
