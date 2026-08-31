/**
 * Effect runtime configuration for the Temporal workflow sandbox.
 *
 * The polyfill import below must stay first so the globals are installed
 * before any `effect` module evaluates inside the workflow bundle.
 *
 * @since 1.0.0
 */
import "./TemporalSandboxPolyfills.js"
import * as Scheduler from "effect/Scheduler"

export { ensureSandboxPolyfills } from "./TemporalSandboxPolyfills.js"

const setMicrotask = (f: () => void): () => void => {
  let cancelled = false
  void Promise.resolve().then(() => {
    if (!cancelled) {
      f()
    }
  })
  return () => {
    cancelled = true
  }
}

/**
 * Creates a fiber scheduler that is safe inside the Temporal workflow
 * sandbox.
 *
 * Effect's default scheduler flushes queued fiber tasks with `setImmediate`,
 * falling back to `setTimeout(f, 0)` when `setImmediate` is missing. The
 * sandbox has no `setImmediate`, and its `setTimeout` is a durable Temporal
 * timer, so every scheduler flush would append a timer to workflow history
 * and tie replay to scheduler internals. This scheduler flushes on the
 * microtask queue instead, so fiber scheduling never touches history.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const makeSandboxScheduler = (): Scheduler.Scheduler => new Scheduler.MixedScheduler("async", setMicrotask)

/**
 * The shared sandbox scheduler used by `TemporalWorkflowRuntime`. It holds no
 * per-run state, so sharing it across workflow instances (including under
 * `reuseV8Context`) is safe.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const sandboxScheduler: Scheduler.Scheduler = makeSandboxScheduler()
