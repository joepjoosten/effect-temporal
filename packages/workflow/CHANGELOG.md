# @effect-temporal/workflow

## 0.1.7

### Patch Changes

- Updated dependencies [[`e6b2d8c`](https://github.com/joepjoosten/effect-temporal/commit/e6b2d8c336a5a9902b8a241174045f5fd7c45ccb)]:
  - @effect-temporal/client@0.2.3

## 0.1.6

### Patch Changes

- [#48](https://github.com/joepjoosten/effect-temporal/pull/48) [`89b43db`](https://github.com/joepjoosten/effect-temporal/commit/89b43db72a530eb3f7dc1a24351ee5de06cd7277) Thanks [@joepjoosten](https://github.com/joepjoosten)! - `TemporalLint`: an ESLint plugin (flat-config compatible) enforcing
  workflow-sandbox safety on workflow bundle files, with a `recommended`
  preset. Rules: `no-module-level-mutable` (mutable module-scope state breaks
  V8 context reuse and replay), `no-mixed-halves` (no client/worker-side or
  Node imports in bundle files; forbidden patterns configurable),
  `prefer-typed-activity` (flag direct `proxyActivities` usage), and
  `zero-arity-effect-promise` (thunks passed to `Effect.promise` /
  `Effect.tryPromise` must take no parameters).

- [#45](https://github.com/joepjoosten/effect-temporal/pull/45) [`77edd20`](https://github.com/joepjoosten/effect-temporal/commit/77edd20a137852388e3c9816eefa26432692ebe6) Thanks [@joepjoosten](https://github.com/joepjoosten)! - `TemporalTypedActivity`: schema-typed activities with explicit payloads and
  per-activity Temporal options. Define once with
  `TemporalTypedActivity.make(name, { payload, success, error, options })`;
  the workflow body calls with `TemporalTypedActivity.call(activity, payload)`
  — payloads are encoded across the worker boundary and the schema'd failure
  is decoded into the Effect error channel — and the worker binds
  implementations with `handle` + `implement` (with a `provide` seam for
  Effect services). Typed failures complete the Temporal activity without
  retries; defects fail it so the activity's retry policy applies. Completed
  exits are memoized per call position, so resumed passes of the body replay
  results instead of re-scheduling. Options support timeouts, retry policies,
  and local activities.

- [#49](https://github.com/joepjoosten/effect-temporal/pull/49) [`b7d7ce8`](https://github.com/joepjoosten/effect-temporal/commit/b7d7ce80313e6cbfd79d037f7690bb0032a855ba) Thanks [@joepjoosten](https://github.com/joepjoosten)! - First-class typed Nexus support in both directions:

  - `TemporalNexusOperation.make(service, operation, { payload, success, error })`
    defines a schema-typed Nexus operation shared by handler and callers;
    workflow-side `call` encodes the payload, runs the operation in its own
    cancellation scope, and decodes the backing workflow's typed failure into
    the Effect error channel.
  - `TemporalNexusService.workflowRunOperation(operation, workflow)` builds the
    worker `nexusServices` handler backing the operation with an Effect
    workflow — requests start it under its deterministic execution id, with
    `USE_EXISTING` conflict semantics for idempotent duplicate requests.
  - The client package's Nexus modules are now typed against the real
    `@temporalio/client` Nexus surface (SDK 1.23) instead of structural
    `unknown` shapes, and construct `NexusClient` directly.

- [#47](https://github.com/joepjoosten/effect-temporal/pull/47) [`3fdb54e`](https://github.com/joepjoosten/effect-temporal/commit/3fdb54ec256499c597f6b3ff50d60819cba64ede) Thanks [@joepjoosten](https://github.com/joepjoosten)! - `TemporalVersioning`: patch-based versioning for evolving workflow code
  while old executions are still replaying. `patched` / `deprecatePatch` wrap
  Temporal's markers as Effects, and `match` runs version chains: declare an
  ordered list of `{ id, run }` branches (plus an optional `legacy` branch for
  histories predating every version) — a new execution records the newest
  marker and takes its branch, a replaying execution follows the branch its
  history recorded.
- Updated dependencies [[`b7d7ce8`](https://github.com/joepjoosten/effect-temporal/commit/b7d7ce80313e6cbfd79d037f7690bb0032a855ba)]:
  - @effect-temporal/client@0.2.2

## 0.1.5

### Patch Changes

- [#42](https://github.com/joepjoosten/effect-temporal/pull/42) [`37b27e1`](https://github.com/joepjoosten/effect-temporal/commit/37b27e1ab08e7873ebe1dbf38ab6de0972ac8c45) Thanks [@joepjoosten](https://github.com/joepjoosten)! - `TemporalContinueAsNew.continueAsNew(workflow, payload, { memo? })`: close
  the current run and continue the workflow as a new run under the same
  workflow id, with the new payload encoded through the workflow's schema.
  The runtime lets Temporal's continuation marker escape the Effect runtime
  untouched, and clients awaiting the execution follow the run chain to the
  final result. Per-run state (mailbox logs, update logs, state cells) starts
  fresh in the new run.

- [#44](https://github.com/joepjoosten/effect-temporal/pull/44) [`32b5cea`](https://github.com/joepjoosten/effect-temporal/commit/32b5cead7752bc8e4e42917b07d9f851093e9278) Thanks [@joepjoosten](https://github.com/joepjoosten)! - `TemporalDurableMailbox`: schema-typed inbound message mailboxes for entity
  workflows. Define a mailbox once with
  `TemporalDurableMailbox.make(name, { payload: Schema })`; the workflow body
  consumes with `take` (durably waits on a Temporal condition) or `poll`
  (non-blocking), clients send with
  `TemporalWorkflowInteractions.offerMailbox`, and workflows can signal each
  other with `TemporalDurableMailbox.offer`. Messages arrive through a
  reserved signal into an append-only per-run log; consumption cursors reset
  on every pass of the body, so a resumed run replays already-consumed
  messages deterministically.

- [#41](https://github.com/joepjoosten/effect-temporal/pull/41) [`fc6007f`](https://github.com/joepjoosten/effect-temporal/commit/fc6007fa5b132039a2adac6dd9efc1e535479ce0) Thanks [@joepjoosten](https://github.com/joepjoosten)! - `TemporalDurableUpdate`: typed request/response interactions with running
  workflows. Define an update once with
  `TemporalDurableUpdate.make(name, { payload, success, error })` — the
  failure channel is a schema too. The workflow body serves requests with
  `take`, answering each through a one-shot typed `respond`/`succeed`/`fail`
  capability; clients call `TemporalWorkflowInteractions.executeUpdate` and
  receive the typed success in the success channel or the schema'd failure in
  the error channel (with an optional stable `requestId` for idempotent
  retries and a configurable poll/timeout).

  Also fixes idempotent re-execution: engine starts now use the
  `REJECT_DUPLICATE` workflow-id reuse policy, so executing a completed
  execution id attaches to the recorded result instead of starting a fresh
  run; child-workflow results are memoized per execution id so a resumed pass
  of the body does not start duplicate children, and re-issued discard starts
  tolerate an already-running child.

- [#39](https://github.com/joepjoosten/effect-temporal/pull/39) [`1e9d168`](https://github.com/joepjoosten/effect-temporal/commit/1e9d168f7c99a5f3fca649195a8db4bdd6deac86) Thanks [@joepjoosten](https://github.com/joepjoosten)! - `TemporalStateCell`: queryable published workflow state. Define a cell once
  with `TemporalStateCell.make(name, { value: Schema })`; the workflow body
  publishes with `TemporalStateCell.set`, and clients read a typed snapshot
  with `TemporalWorkflowInteractions.readStateCell` — served by a reserved
  query, so it also works after the run has closed. Introduces the
  `TemporalSandboxRun` service, which exposes the per-run runtime state to
  workflow-side primitives and is provided automatically by `makeWorkflow`.

## 0.1.4

### Patch Changes

- [#36](https://github.com/joepjoosten/effect-temporal/pull/36) [`b491a87`](https://github.com/joepjoosten/effect-temporal/commit/b491a8712c72e9f34e34f8e9b348e595d966ac0a) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Cancellation and compensation semantics for the workflow runtime:

  - The workflow body now runs inside a non-cancellable Temporal scope; a
    Temporal cancellation is observed by the Effect runtime — the handler fiber
    is interrupted so finalizers and `withCompensation` handlers run — and the
    run then closes as Cancelled by rethrowing the `CancelledFailure`.
  - The reserved interrupt signal now interrupts the live workflow fiber
    (matching Effect's in-memory engine semantics: `instance.interrupted` set,
    suspension cleared), so an interrupt lands as a `Complete` result with an
    interrupted exit after compensation has run, instead of waiting for the
    next suspension point.
  - Every activity call gets its own cancellation scope, tracked per run: an
    interrupt or cancellation cancels in-flight scheduled activities, while
    compensation activities scheduled afterwards run in fresh scopes parented
    to the non-cancellable run scope.
  - Completed activity results are memoized per attempt in run state, so a
    resumed pass of the workflow body no longer re-schedules (and re-executes)
    activities that already finished.

- [#34](https://github.com/joepjoosten/effect-temporal/pull/34) [`4daf050`](https://github.com/joepjoosten/effect-temporal/commit/4daf050a6e7bc5954a209c0b0751959fe176c9b1) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Make the workflow runtime deterministic inside the Temporal sandbox:

  - `TemporalWorkflowRuntime.makeWorkflow` now runs workflow bodies on a
    microtask-based fiber scheduler, so fiber yields never fall back to
    `setTimeout` — which is a durable Temporal timer in the sandbox — and no
    longer append timer events to workflow history.
  - New `TemporalSandboxPolyfills` module (installed automatically by the
    runtime) provides `crypto.subtle.digest` (SHA-256), `crypto.getRandomValues`,
    and `TextEncoder` inside the workflow isolate, byte-pinned against Node so
    execution ids hashed in the sandbox match ids computed client-side, and pins
    `performance` to the deterministic sandbox clock.
  - Fixed workflow identification: the engine and runtime now key workflows by
    `workflow._tag` instead of `workflow.name`, which always evaluated to the
    string `"Workflow"` and broke workflow-type routing end to end.
  - Added the first live end-to-end test covering `TemporalWorkflowEngine` +
    `TemporalWorkflowRuntime`: an Effect workflow body with an activity call and
    a child workflow, asserting no scheduler timers appear in history.

- [#35](https://github.com/joepjoosten/effect-temporal/pull/35) [`5d318df`](https://github.com/joepjoosten/effect-temporal/commit/5d318df5906b1e29e39615ada2df38f961b819f3) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Typed wire protocol across all Temporal boundaries. Every value crossing the
  isolate boundary is now schema-encoded JSON derived from the workflow's own
  payload, success, and error schemas (`TemporalWorkflowWire`):

  - Workflow payloads are encoded before start / child-workflow calls and
    decoded inside the sandbox, so schema types (tagged errors, transformations)
    survive the boundary instead of being mangled by the payload converter.
  - A failing workflow now fails the Temporal run with a non-retryable
    `ApplicationFailure` of type `EffectWorkflowExit` carrying the encoded
    `Workflow.Result` in `details[0]`. The client engine and the child-workflow
    path decode it back, so typed errors land in the Effect error channel as
    real schema class instances, and runs show as failed in the Temporal UI.
  - Durable-deferred exits ride signals and queries as canonical JSON
    (`encodeDeferredExit` / `decodeDeferredExit`) instead of raw structural
    `Exit` objects.
  - `poll` now decodes the suspended workflow state, returning
    `Workflow.Suspended` including its cause instead of always `undefined`.

  This changes the reserved query/signal payload formats, so workers and clients
  must be upgraded together; in-flight workflow runs started on the previous
  protocol are not compatible.

## 0.1.3

### Patch Changes

- [#33](https://github.com/joepjoosten/effect-temporal/pull/33) [`d401a0a`](https://github.com/joepjoosten/effect-temporal/commit/d401a0a1f6acc6e59a066e1d2c2758453e0802c1) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Upgrade effect to 4.0.0-rc.112

- [#33](https://github.com/joepjoosten/effect-temporal/pull/33) [`6fd7326`](https://github.com/joepjoosten/effect-temporal/commit/6fd7326688569f45f4df710aed74d94419c50a8f) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Upgrade Temporal SDK packages to ^1.23.0

- Updated dependencies [[`d401a0a`](https://github.com/joepjoosten/effect-temporal/commit/d401a0a1f6acc6e59a066e1d2c2758453e0802c1), [`6fd7326`](https://github.com/joepjoosten/effect-temporal/commit/6fd7326688569f45f4df710aed74d94419c50a8f)]:
  - @effect-temporal/client@0.2.1

## 0.1.2

### Patch Changes

- Updated dependencies [[`0c0b8ec`](https://github.com/joepjoosten/effect-temporal/commit/0c0b8ec1bc7f3f5cd3ce9ceae788b05879288d60)]:
  - @effect-temporal/client@0.2.0

## 0.1.1

### Patch Changes

- [#13](https://github.com/joepjoosten/effect-temporal/pull/13) [`102b696`](https://github.com/joepjoosten/effect-temporal/commit/102b6965a6126cdbb146c5236aa2408360916dba) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Update the packages to Effect 4.0.0-rc.111 and migrate the test workspace to the Vitest 4 projects configuration.

- Updated dependencies [[`102b696`](https://github.com/joepjoosten/effect-temporal/commit/102b6965a6126cdbb146c5236aa2408360916dba)]:
  - @effect-temporal/client@0.1.1

## 0.1.0

### Minor Changes

- [`6553294`](https://github.com/joepjoosten/effect-temporal/commit/6553294a68b8a8e05321394b785295fd78caac83) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Publish the first public release of the Effect integrations for Temporal.

  - Add Effect services for Temporal connections, clients, workflow handles, and data conversion.
  - Add a Temporal-backed Effect workflow engine, worker lifecycle, workflow runtime, activities, durable deferreds, durable clocks, and child workflows.
  - Add scoped Temporal test environments, workflow engine layers, task queue helpers, and worker helpers.

### Patch Changes

- [#11](https://github.com/joepjoosten/effect-temporal/pull/11) [`e49ad71`](https://github.com/joepjoosten/effect-temporal/commit/e49ad71c6099d5173adc2e4b048d42d14cc8d7bf) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Update all packages to Effect 4 beta.99, adapt workflow definitions to the latest beta API, and keep the testing entry point stable during code generation.

- Updated dependencies [[`e49ad71`](https://github.com/joepjoosten/effect-temporal/commit/e49ad71c6099d5173adc2e4b048d42d14cc8d7bf), [`6553294`](https://github.com/joepjoosten/effect-temporal/commit/6553294a68b8a8e05321394b785295fd78caac83)]:
  - @effect-temporal/client@0.1.0
