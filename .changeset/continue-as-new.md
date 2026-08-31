---
"@effect-temporal/workflow": patch
---

`TemporalContinueAsNew.continueAsNew(workflow, payload, { memo? })`: close
the current run and continue the workflow as a new run under the same
workflow id, with the new payload encoded through the workflow's schema.
The runtime lets Temporal's continuation marker escape the Effect runtime
untouched, and clients awaiting the execution follow the run chain to the
final result. Per-run state (mailbox logs, update logs, state cells) starts
fresh in the new run.
