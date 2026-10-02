---
"@effect-temporal/client": minor
"@effect-temporal/workflow": minor
"@effect-temporal/testing": minor
---

Upgrade effect to 4.0.0.

Effect 4.0.0 promotes the workflow modules out of `unstable`, so imports move from `effect/unstable/workflow/*` to `effect/workflow/*` (e.g. `effect/workflow/Workflow`, `effect/workflow/DurableDeferred`). Applications defining workflows must upgrade to `effect@4.0.0` and update these import paths.
