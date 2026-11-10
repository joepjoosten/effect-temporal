---
"@effect-temporal/client": minor
"@effect-temporal/workflow": minor
"@effect-temporal/testing": minor
---

Upgrade effect to 4.1.3 and the Temporal SDK to 1.25.0.

Applications must upgrade to `effect@4.1.3` and `@temporalio/*@^1.25.0`. Temporal 1.25 added package `exports` maps, so the client now imports SDK internals through the exported `@temporalio/common/internal/internal-non-workflow` and `@temporalio/client/lib/helpers` paths.
