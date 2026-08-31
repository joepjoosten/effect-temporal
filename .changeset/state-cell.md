---
"@effect-temporal/workflow": patch
---

`TemporalStateCell`: queryable published workflow state. Define a cell once
with `TemporalStateCell.make(name, { value: Schema })`; the workflow body
publishes with `TemporalStateCell.set`, and clients read a typed snapshot
with `TemporalWorkflowInteractions.readStateCell` — served by a reserved
query, so it also works after the run has closed. Introduces the
`TemporalSandboxRun` service, which exposes the per-run runtime state to
workflow-side primitives and is provided automatically by `makeWorkflow`.
