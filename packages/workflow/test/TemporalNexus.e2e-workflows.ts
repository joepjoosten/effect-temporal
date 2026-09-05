import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalNexusOperation from "../src/TemporalNexusOperation.js"
import * as TemporalWorkflowRuntime from "../src/TemporalWorkflowRuntime.js"

export class QuoteRejected extends Schema.TaggedError<QuoteRejected>()("QuoteRejected", {
  reason: Schema.String
}) {}

const quotePayload = Schema.Struct({ quantity: Schema.Number, sku: Schema.String })
const quoteSuccess = Schema.Struct({ totalCents: Schema.Number })

export const quoteWorkflow = Workflow.make("NexusE2EQuoteWorkflow", {
  payload: {
    quantity: Schema.Number,
    sku: Schema.String
  },
  success: quoteSuccess,
  error: QuoteRejected,
  idempotencyKey: ({ quantity, sku }) => `${sku}/${quantity}`
})

export const NexusE2EQuoteWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: quoteWorkflow,
  execute: (payload: { readonly quantity: number; readonly sku: string }) =>
    payload.quantity <= 0
      ? Effect.fail(new QuoteRejected({ reason: `invalid quantity ${payload.quantity}` }))
      : Effect.succeed({ totalCents: payload.quantity * 100 })
})

export const quoteOperation = TemporalNexusOperation.make("quote-service", "getQuote", {
  payload: quotePayload,
  success: quoteSuccess,
  error: QuoteRejected
})

export const approval = DurableDeferred.make("nexus-approval", { success: Schema.String })

export const purchaserWorkflow = Workflow.make("NexusE2EPurchaserWorkflow", {
  payload: {
    endpoint: Schema.String,
    sku: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ sku }) => sku
})

export const NexusE2EPurchaserWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: purchaserWorkflow,
  execute: (payload: { readonly endpoint: string; readonly sku: string }) =>
    Effect.gen(function*() {
      const quote = yield* TemporalNexusOperation.call(
        quoteOperation,
        { endpoint: payload.endpoint },
        { quantity: 3, sku: payload.sku }
      )
      const rejection = yield* TemporalNexusOperation.call(
        quoteOperation,
        { endpoint: payload.endpoint },
        { quantity: 0, sku: payload.sku }
      ).pipe(
        Effect.catchTag("QuoteRejected", (error) => Effect.succeed(`rejected(${error.reason})`))
      )
      yield* DurableDeferred.await(approval)
      return `${quote.totalCents}:${rejection}`
    })
})
