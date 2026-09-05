/**
 * @since 1.0.0
 */
export * from "./TemporalTesting.js"

/**
 * @since 1.0.0
 */
export * from "./TemporalStubClient.js"

/**
 * @since 1.0.0
 */
export * from "./TemporalWorkflowHarness.js"

/**
 * A recording stub (test double) for the Temporal workflow client, for fast
 * unit tests of client-side code with zero servers.
 *
 * Every interaction is recorded for assertions. Any member of the client or
 * handle surface that the stub does not implement — and that the test did not
 * override — throws a descriptive error naming the member, instead of
 * returning `undefined`.
 *
 * @since 1.0.0
 */
export * as TemporalStubClient from "./TemporalStubClient.js"

/**
 * @since 1.0.0
 */
export * as TemporalTesting from "./TemporalTesting.js"

/**
 * A one-call, scope-bound test harness for integration tests of Effect
 * workflows: boots a Temporal test environment, runs a worker for the given
 * workflow bundle and activities for the lifetime of the scope, and hands the
 * test a ready client, engine, and a `provide` helper that wires both into
 * any Effect.
 *
 * @since 1.0.0
 */
export * as TemporalWorkflowHarness from "./TemporalWorkflowHarness.js"
