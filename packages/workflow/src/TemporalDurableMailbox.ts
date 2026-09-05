/**
 * Durable, schema-typed inbound message mailboxes for entity workflows.
 *
 * A mailbox is a named channel of repeated inbound messages. Messages arrive
 * through a reserved signal and are appended to a per-run log; the workflow
 * body consumes them with `take` (durably waits) or `poll` (non-blocking).
 * Consumption is tracked by a cursor that resets on every pass of the body,
 * so a resumed run replays already-consumed messages deterministically.
 *
 * This module is sandbox-safe: definitions, `take`, `poll`, and the
 * workflow-to-workflow `offer` can be imported from the workflow bundle. The
 * client-side offer lives in `TemporalWorkflowInteractions.offerMailbox`.
 *
 * @since 1.0.0
 */
import { condition, getExternalWorkflowHandle } from "@temporalio/workflow"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import type * as Schema from "effect/Schema"
import { type MailboxSignal, mailboxSignalName, workflowIdFor } from "./TemporalWorkflowProtocol.js"
import { memoizeOutboundCommand, TemporalSandboxRun } from "./TemporalWorkflowRuntime.js"
import { type TemporalValueCodecs, valueCodecsFor } from "./TemporalWorkflowWire.js"

/**
 * A named, schema-typed durable mailbox.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalDurableMailbox<Payload extends Schema.Top = Schema.Top> {
  readonly name: string
  readonly payloadSchema: Payload
  readonly codecs: TemporalValueCodecs
}

/**
 * Creates a mailbox definition, shared by the workflow bundle, the worker,
 * and every client so all sides encode and decode with the same schema.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const make = <Payload extends Schema.Top>(
  name: string,
  options: {
    readonly payload: Payload
  }
): TemporalDurableMailbox<Payload> => ({
  name,
  payloadSchema: options.payload,
  codecs: valueCodecsFor(options.payload)
})

const nextMessage = (
  state: TemporalSandboxRun,
  mailbox: TemporalDurableMailbox<any>
): Option.Option<unknown> => {
  const log = state.mailboxLogs.get(mailbox.name)
  const cursor = state.mailboxCursors.get(mailbox.name) ?? 0
  if (log === undefined || cursor >= log.length) {
    return Option.none()
  }
  state.mailboxCursors.set(mailbox.name, cursor + 1)
  return Option.some(mailbox.codecs.decode(log[cursor]))
}

/**
 * Takes the next message from the mailbox, durably waiting until one is
 * available. Waiting blocks on a Temporal condition — it does not suspend the
 * Effect workflow — so the run stays open and replay-safe.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const take = <Payload extends Schema.Top>(
  mailbox: TemporalDurableMailbox<Payload>
): Effect.Effect<Payload["Type"], never, TemporalSandboxRun> =>
  Effect.gen(function*() {
    const state = yield* TemporalSandboxRun
    while (true) {
      const message = nextMessage(state, mailbox)
      if (Option.isSome(message)) {
        return message.value as Payload["Type"]
      }
      const cursor = state.mailboxCursors.get(mailbox.name) ?? 0
      yield* Effect.promise(() => condition(() => (state.mailboxLogs.get(mailbox.name)?.length ?? 0) > cursor))
    }
  })

/**
 * Takes the next message when one is already available, without waiting.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const poll = <Payload extends Schema.Top>(
  mailbox: TemporalDurableMailbox<Payload>
): Effect.Effect<Option.Option<Payload["Type"]>, never, TemporalSandboxRun> =>
  Effect.gen(function*() {
    const state = yield* TemporalSandboxRun
    return nextMessage(state, mailbox) as Option.Option<Payload["Type"]>
  })

/**
 * Offers a message to another workflow's mailbox from inside a workflow body
 * (an external signal). Best-effort: the target execution must be open.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const offer = <Payload extends Schema.Top>(
  mailbox: TemporalDurableMailbox<Payload>,
  target: {
    readonly workflow: { readonly _tag: string }
    readonly executionId: string
    readonly workflowIdPrefix?: string | undefined
  },
  message: Payload["Type"]
): Effect.Effect<void, never, TemporalSandboxRun> =>
  Effect.gen(function*() {
    const state = yield* TemporalSandboxRun
    const workflowId = workflowIdFor(target.workflow._tag, target.executionId, target.workflowIdPrefix)
    yield* Effect.promise(() =>
      memoizeOutboundCommand(
        state,
        JSON.stringify(["mailbox", workflowId, mailbox.name]),
        () =>
          getExternalWorkflowHandle(workflowId).signal(
            mailboxSignalName,
            {
              name: mailbox.name,
              payload: mailbox.codecs.encode(message)
            } satisfies MailboxSignal
          )
      )
    )
  })
