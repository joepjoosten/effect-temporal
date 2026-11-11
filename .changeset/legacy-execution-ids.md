---
"@effect-temporal/workflow": minor
---

Add `TemporalWorkflowProtocol.legacyExecutionId` and the `legacyChildExecutionIds` runtime option for workflows started before the Effect execution-id digest change.

`effect@4.0.0-rc.118` changed the `workflow.executionId(payload)` digest, and 0.2.0 adopted it without a compatibility path: executions started on 0.1.x are no longer reachable by payload, and parent workflows recorded on 0.1.x fail replay with a nondeterminism error at their child starts. `legacyExecutionId(workflow, payload)` returns the old id for addressing those executions. `makeWorkflow({ legacyChildExecutionIds: true })` replays pre-marker child starts under the old id; leave it unset for histories recorded on 0.2.0 or 0.3.0, which already use the new id. Child starts now record the `effect-temporal/child-execution-ids-v2` patch marker, adding one marker event to runs that start a child.
