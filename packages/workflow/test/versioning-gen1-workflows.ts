import { sleep } from "@temporalio/workflow"
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

// Generation 1: a single version whose branch starts a durable timer.
export const VersioningE2EWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: versionedWorkflow,
  execute: () =>
    TemporalVersioning.match({
      versions: [
        {
          id: "v1",
          run: Effect.gen(function*() {
            yield* Effect.promise(() => sleep(50))
            return "v1-result"
          })
        }
      ],
      legacy: Effect.succeed("legacy-result")
    })
})
