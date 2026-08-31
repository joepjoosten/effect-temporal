/**
 * Patch-based versioning for evolving workflow code while old executions are
 * still replaying.
 *
 * Temporal's patch markers let changed workflow code stay deterministic
 * against histories recorded by earlier deployments: a new execution records
 * the marker and takes the new path, a replaying execution follows whatever
 * its history recorded. This module wraps the raw markers as Effects and adds
 * `match`, a version-chain combinator for code that has evolved more than
 * once.
 *
 * Patch markers must be evaluated deterministically on the main workflow
 * fiber: never call these from forked fibers, inside `Effect.race`, or behind
 * non-deterministic conditions.
 *
 * This module is sandbox-safe.
 *
 * @since 1.0.0
 */
import { deprecatePatch as temporalDeprecatePatch, patched as temporalPatched } from "@temporalio/workflow"
import * as Effect from "effect/Effect"

/**
 * Returns `true` when this execution should take the patched (new) code
 * path: always for new executions — recording the marker — and for replays
 * whose history recorded the marker.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const patched = (patchId: string): Effect.Effect<boolean> => Effect.sync(() => temporalPatched(patchId))

/**
 * Marks a patch as deprecated once no open histories rely on the old path:
 * replays of marker-less histories fail loudly instead of silently taking
 * the old branch, allowing the dead code to be deleted next.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const deprecatePatch = (patchId: string): Effect.Effect<void> =>
  Effect.sync(() => temporalDeprecatePatch(patchId))

/**
 * One version in a `match` chain.
 *
 * @since 1.0.0
 * @category Models
 */
export interface VersionBranch<A, E, R> {
  readonly id: string
  readonly run: Effect.Effect<A, E, R>
}

/**
 * Runs the branch for this execution's recorded version. `versions` is
 * ordered oldest to newest: a new execution records the newest version's
 * marker and runs its branch; a replaying execution runs the branch whose
 * marker its history recorded; a history predating every version runs
 * `legacy`.
 *
 * Deploy lifecycle: append a new `{ id, run }` for each behavioral change;
 * once no open executions predate a version, move its predecessors' ids to
 * `deprecatePatch` and delete their branches.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const match = <A, E, R>(options: {
  readonly versions: readonly [VersionBranch<A, E, R>, ...Array<VersionBranch<A, E, R>>]
  /**
   * Behavior for histories recorded before the first version. Omit for
   * workflows that have used `match` from their first deployment.
   */
  readonly legacy?: Effect.Effect<A, E, R> | undefined
}): Effect.Effect<A, E, R> =>
  Effect.gen(function*() {
    // Newest first: a new execution matches (and records) only the newest
    // marker; replays match the marker their history recorded.
    for (let index = options.versions.length - 1; index >= 0; index--) {
      const version = options.versions[index]
      if (yield* patched(version.id)) {
        return yield* version.run
      }
    }
    if (options.legacy !== undefined) {
      return yield* options.legacy
    }
    return yield* Effect.die(
      new Error(
        "TemporalVersioning.match: history predates every declared version and no legacy branch was provided"
      )
    )
  })
