---
"@effect-temporal/workflow": patch
---

`TemporalDurableUpdate`: typed request/response interactions with running
workflows. Define an update once with
`TemporalDurableUpdate.make(name, { payload, success, error })` — the
failure channel is a schema too. The workflow body serves requests with
`take`, answering each through a one-shot typed `respond`/`succeed`/`fail`
capability; clients call `TemporalWorkflowInteractions.executeUpdate` and
receive the typed success in the success channel or the schema'd failure in
the error channel (with an optional stable `requestId` for idempotent
retries and a configurable poll/timeout).

Also fixes idempotent re-execution: engine starts now use the
`REJECT_DUPLICATE` workflow-id reuse policy, so executing a completed
execution id attaches to the recorded result instead of starting a fresh
run; child-workflow results are memoized per execution id so a resumed pass
of the body does not start duplicate children, and re-issued discard starts
tolerate an already-running child.
