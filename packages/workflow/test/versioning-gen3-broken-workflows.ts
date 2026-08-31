import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalVersioning from "../src/TemporalVersioning.js"
import * as TemporalWorkflowRuntime from "../src/TemporalWorkflowRuntime.js"

export const versionedWorkflow = Workflow.make("VersioningE2EWorkflow", {
  payload: {
    runId: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ runId }) => runId
})

// Negative control: the v1 branch dropped its timer without a patch marker —
// replaying a generation-1 history against this code must fail as
// non-deterministic.
export const VersioningE2EWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: versionedWorkflow,
  execute: () =>
    TemporalVersioning.match({
      versions: [
        {
          id: "v1",
          run: Effect.succeed("v1-result")
        },
        {
          id: "v2",
          run: Effect.succeed("v2-result")
        }
      ],
      legacy: Effect.succeed("legacy-result")
    })
})
