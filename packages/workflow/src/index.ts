/**
 * Re-exported Temporal workflow client service.
 *
 * @since 1.0.0
 */
export * as TemporalClient from "./TemporalClient.js"

/**
 * Re-exported Temporal client connection service.
 *
 * @since 1.0.0
 */
export * as TemporalConnection from "./TemporalConnection.js"

/**
 * Continue-as-new support for long-lived Effect workflows.
 *
 * Entity-style workflows accumulate history indefinitely; Temporal's answer
 * is continue-as-new: close the current run and start a fresh one under the
 * same workflow id with new arguments. `continueAsNew` encodes the new
 * payload with the workflow's schema and triggers Temporal's continuation —
 * the effect never returns. The runtime lets the continuation marker escape
 * the Effect runtime untouched, and clients awaiting the execution follow the
 * run chain to the final result.
 *
 * This module is sandbox-safe.
 *
 * @since 1.0.0
 */
export * as TemporalContinueAsNew from "./TemporalContinueAsNew.js"

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
export * as TemporalDurableMailbox from "./TemporalDurableMailbox.js"

/**
 * Typed request/response interactions with running workflows.
 *
 * A durable update is a named channel of schema-typed requests, each of which
 * the workflow answers with a typed success or a typed failure. Requests
 * arrive through a reserved signal into a per-run log; the workflow body
 * serves them with `take`, and each request carries a one-shot typed
 * `respond` capability whose exit is recorded per request id and served by
 * the reserved update-result query. Clients call
 * `TemporalWorkflowInteractions.executeUpdate` to send a request and await
 * the typed response.
 *
 * This module is sandbox-safe.
 *
 * @since 1.0.0
 */
export * as TemporalDurableUpdate from "./TemporalDurableUpdate.js"

/**
 * @since 1.0.0
 */
export * as TemporalError from "./TemporalError.js"

/**
 * An ESLint plugin (flat-config compatible) enforcing workflow-sandbox
 * safety for Effect workflow bundles. The rules catch the mistakes that
 * otherwise only surface as non-deterministic replay failures in production.
 *
 * Usage (flat config), applied to the files that make up workflow bundles:
 *
 * ```ts
 * import { plugin as effectTemporal } from "@effect-temporal/workflow/TemporalLint"
 *
 * export default [
 *   {
 *     files: ["src/workflows/**"],
 *     plugins: { "effect-temporal": effectTemporal },
 *     rules: effectTemporal.configs.recommended.rules
 *   }
 * ]
 * ```
 *
 * @since 1.0.0
 */
export * as TemporalLint from "./TemporalLint.js"

/**
 * Effect runtime configuration for the Temporal workflow sandbox.
 *
 * The polyfill import below must stay first so the globals are installed
 * before any `effect` module evaluates inside the workflow bundle.
 *
 * @since 1.0.0
 */
export * as TemporalSandbox from "./TemporalSandbox.js"

/**
 * Global polyfills that make the Temporal workflow sandbox safe for Effect
 * programs.
 *
 * The workflow isolate lacks `crypto`, `TextEncoder`, and `performance`, all
 * of which Effect touches: workflow execution ids are derived with
 * `crypto.subtle.digest("SHA-256", new TextEncoder().encode(...))`, the
 * default random service seeds from `crypto.getRandomValues`, and the clock
 * captures `performance.now` (with module-level fallback state) the moment the
 * `effect` module evaluates.
 *
 * This module must therefore evaluate before any `effect` module inside the
 * workflow bundle and must not import `effect` itself. Importing
 * `TemporalWorkflowRuntime` (or `TemporalSandbox`) first in the workflow
 * entrypoint is sufficient; the polyfills are also re-installed defensively on
 * every workflow run.
 *
 * @since 1.0.0
 */
export * as TemporalSandboxPolyfills from "./TemporalSandboxPolyfills.js"

/**
 * Queryable published workflow state.
 *
 * A state cell is a named, schema-typed value a workflow publishes for
 * observers. The workflow body writes it with `set`; clients read a typed
 * snapshot at any time through the reserved state-cell query — including
 * after the run has closed, since Temporal serves queries on closed
 * workflows.
 *
 * This module is sandbox-safe: definitions and `set` can be imported from the
 * workflow bundle. The client-side read lives in
 * `TemporalWorkflowInteractions`.
 *
 * @since 1.0.0
 */
export * as TemporalStateCell from "./TemporalStateCell.js"

/**
 * @since 1.0.0
 */
export * as TemporalWorker from "./TemporalWorker.js"

/**
 * @since 1.0.0
 */
export * as TemporalWorkflowEngine from "./TemporalWorkflowEngine.js"

/**
 * Client-side interactions with running (or closed) Effect workflows beyond
 * the `WorkflowEngine` contract: reading published state cells, and — as more
 * primitives land — offering mailbox messages and executing updates.
 *
 * This module uses the Temporal client and must not be imported from a
 * workflow bundle.
 *
 * @since 1.0.0
 */
export * as TemporalWorkflowInteractions from "./TemporalWorkflowInteractions.js"

/**
 * @since 1.0.0
 */
export * as TemporalWorkflowProtocol from "./TemporalWorkflowProtocol.js"

/**
 * @since 1.0.0
 */
export * as TemporalWorkflowRuntime from "./TemporalWorkflowRuntime.js"

/**
 * Wire codecs for every value that crosses a Temporal boundary: workflow
 * payloads, workflow results, and durable-deferred exits.
 *
 * Everything on the wire is schema-encoded JSON. Typed workflow failures fail
 * the Temporal run with the full encoded `Workflow.Result` carried in
 * `ApplicationFailure.details[0]` under the {@link workflowExitFailureType}
 * failure type, so every reading side can decode the exit back into the
 * workflow's typed success and error channels.
 *
 * This module is imported inside the workflow sandbox, so it must only depend
 * on `effect` and `@temporalio/common`.
 *
 * @since 1.0.0
 */
export * as TemporalWorkflowWire from "./TemporalWorkflowWire.js"
