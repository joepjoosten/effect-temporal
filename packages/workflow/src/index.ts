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
 * @since 1.0.0
 */
export * as TemporalError from "./TemporalError.js"

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
 * @since 1.0.0
 */
export * as TemporalWorker from "./TemporalWorker.js"

/**
 * @since 1.0.0
 */
export * as TemporalWorkflowEngine from "./TemporalWorkflowEngine.js"

/**
 * @since 1.0.0
 */
export * as TemporalWorkflowProtocol from "./TemporalWorkflowProtocol.js"

/**
 * @since 1.0.0
 */
export * as TemporalWorkflowRuntime from "./TemporalWorkflowRuntime.js"
