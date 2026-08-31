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
import * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"
import { TemporalSandboxRun } from "./TemporalWorkflowRuntime.js"
import { type TemporalValueCodecs, valueCodecsFor } from "./TemporalWorkflowWire.js"

/**
 * A named, schema-typed cell of observable workflow state.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalStateCell<Value extends Schema.Top = Schema.Top> {
  readonly name: string
  readonly valueSchema: Value
  readonly codecs: TemporalValueCodecs
}

/**
 * Creates a state-cell definition, shared by the workflow bundle and every
 * client so both sides encode and decode with the same schema.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const make = <Value extends Schema.Top>(
  name: string,
  options: {
    readonly value: Value
  }
): TemporalStateCell<Value> => ({
  name,
  valueSchema: options.value,
  codecs: valueCodecsFor(options.value)
})

/**
 * Publishes the cell's current value from inside a workflow body. The value
 * is schema-encoded immediately and served by the reserved state-cell query.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const set = <Value extends Schema.Top>(
  cell: TemporalStateCell<Value>,
  value: Value["Type"]
): Effect.Effect<void, never, TemporalSandboxRun> =>
  Effect.gen(function*() {
    const state = yield* TemporalSandboxRun
    state.stateCells.set(cell.name, cell.codecs.encode(value))
  })
