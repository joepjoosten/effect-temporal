---
"@effect-temporal/workflow": patch
---

`TemporalVersioning`: patch-based versioning for evolving workflow code
while old executions are still replaying. `patched` / `deprecatePatch` wrap
Temporal's markers as Effects, and `match` runs version chains: declare an
ordered list of `{ id, run }` branches (plus an optional `legacy` branch for
histories predating every version) — a new execution records the newest
marker and takes its branch, a replaying execution follows the branch its
history recorded.
