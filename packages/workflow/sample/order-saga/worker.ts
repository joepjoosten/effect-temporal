/**
 * The worker process: runs the workflow bundle, implements the typed
 * activities against Effect, and hosts the Nexus service handler.
 *
 * Run it against a dev server: `temporal server start-dev`, then
 * `pnpm tsx sample/order-saga/worker.ts`.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { fileURLToPath } from "node:url"
import * as TemporalNexusService from "../../src/TemporalNexusService.js"
import * as TemporalTypedActivity from "../../src/TemporalTypedActivity.js"
import * as TemporalWorker from "../../src/TemporalWorker.js"
import { ChargeDeclined, chargeCard, getQuote, quoteWorkflow, refundCharge } from "./definitions.js"

// Activity implementations are `(payload) => Effect`; use the `provide`
// option of `implement` to give them application services via Layers.
const activities = TemporalTypedActivity.implement([
  TemporalTypedActivity.handle(chargeCard, (payload) =>
    payload.amountCents <= 0
      ? Effect.fail(new ChargeDeclined({ code: 51 }))
      : Effect.succeed({ receiptId: `receipt-${payload.orderId}` })),
  TemporalTypedActivity.handle(refundCharge, (payload) =>
    Effect.log(`refunding ${payload.receiptId}`))
])

const workerLayer = Layer.provide(
  TemporalWorker.layer({
    activities,
    // Back the Nexus operation with the quote workflow; duplicate requests
    // for the same payload digest attach to the existing run.
    nexusServices: [TemporalNexusService.workflowRunOperation(getQuote, quoteWorkflow)],
    taskQueue: "order-saga",
    workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url))
  }),
  TemporalWorker.connectionLayer({ address: "localhost:7233" })
)

const runWorker = Effect.gen(function*() {
  const worker = yield* TemporalWorker.TemporalWorker
  yield* worker.run
})

Effect.runPromise(Effect.scoped(Effect.provide(runWorker, workerLayer)))
