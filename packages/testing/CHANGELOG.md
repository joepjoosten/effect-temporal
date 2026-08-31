# @effect-temporal/testing

## 0.1.8

### Patch Changes

- [#53](https://github.com/joepjoosten/effect-temporal/pull/53) [`b2cbddb`](https://github.com/joepjoosten/effect-temporal/commit/b2cbddb11122f59757002e9488dc8c1676d55179) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Documentation refresh: the READMEs now describe the current feature set —
  the deterministic sandbox runtime, typed wire protocol, entity primitives,
  typed activities, versioning, Nexus, lint plugin, and testing utilities —
  and a complete type-checked `sample/order-saga` (definitions, bundle,
  worker, client) ships with the workflow package.
- Updated dependencies [[`b2cbddb`](https://github.com/joepjoosten/effect-temporal/commit/b2cbddb11122f59757002e9488dc8c1676d55179)]:
  - @effect-temporal/workflow@0.1.8
  - @effect-temporal/client@0.2.4

## 0.1.7

### Patch Changes

- Updated dependencies [[`e6b2d8c`](https://github.com/joepjoosten/effect-temporal/commit/e6b2d8c336a5a9902b8a241174045f5fd7c45ccb)]:
  - @effect-temporal/client@0.2.3
  - @effect-temporal/workflow@0.1.7

## 0.1.6

### Patch Changes

- [#46](https://github.com/joepjoosten/effect-temporal/pull/46) [`4e22580`](https://github.com/joepjoosten/effect-temporal/commit/4e225807211bf2fa54c11c2f16784dfd64cb8e1b) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Two new testing utilities:

  - `makeStubTemporalClient` — a recording stub (test double) for the Temporal
    workflow client, for unit tests with zero servers. Every start, signal,
    query, termination, and cancellation is recorded for assertions;
    unconfigured surface members throw a descriptive error naming the member;
    `alreadyStartedWorkflowIds` simulates already-started executions so the
    engine's attach path can be exercised. Ships a ready `layer`.
  - `makeWorkflowTestHarness` — a one-call, scope-bound integration harness:
    boots the test environment (time-skipping by default, `local` selectable),
    runs a worker for the given workflow bundle and activities for the
    lifetime of the scope with ordered teardown, and returns a ready client,
    engine, and a `provide` helper wiring both into any Effect.

- Updated dependencies [[`89b43db`](https://github.com/joepjoosten/effect-temporal/commit/89b43db72a530eb3f7dc1a24351ee5de06cd7277), [`77edd20`](https://github.com/joepjoosten/effect-temporal/commit/77edd20a137852388e3c9816eefa26432692ebe6), [`b7d7ce8`](https://github.com/joepjoosten/effect-temporal/commit/b7d7ce80313e6cbfd79d037f7690bb0032a855ba), [`3fdb54e`](https://github.com/joepjoosten/effect-temporal/commit/3fdb54ec256499c597f6b3ff50d60819cba64ede)]:
  - @effect-temporal/workflow@0.1.6
  - @effect-temporal/client@0.2.2

## 0.1.5

### Patch Changes

- Updated dependencies [[`37b27e1`](https://github.com/joepjoosten/effect-temporal/commit/37b27e1ab08e7873ebe1dbf38ab6de0972ac8c45), [`32b5cea`](https://github.com/joepjoosten/effect-temporal/commit/32b5cead7752bc8e4e42917b07d9f851093e9278), [`fc6007f`](https://github.com/joepjoosten/effect-temporal/commit/fc6007fa5b132039a2adac6dd9efc1e535479ce0), [`1e9d168`](https://github.com/joepjoosten/effect-temporal/commit/1e9d168f7c99a5f3fca649195a8db4bdd6deac86)]:
  - @effect-temporal/workflow@0.1.5

## 0.1.4

### Patch Changes

- Updated dependencies [[`b491a87`](https://github.com/joepjoosten/effect-temporal/commit/b491a8712c72e9f34e34f8e9b348e595d966ac0a), [`4daf050`](https://github.com/joepjoosten/effect-temporal/commit/4daf050a6e7bc5954a209c0b0751959fe176c9b1), [`5d318df`](https://github.com/joepjoosten/effect-temporal/commit/5d318df5906b1e29e39615ada2df38f961b819f3)]:
  - @effect-temporal/workflow@0.1.4

## 0.1.3

### Patch Changes

- [#33](https://github.com/joepjoosten/effect-temporal/pull/33) [`d401a0a`](https://github.com/joepjoosten/effect-temporal/commit/d401a0a1f6acc6e59a066e1d2c2758453e0802c1) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Upgrade effect to 4.0.0-rc.112

- [#33](https://github.com/joepjoosten/effect-temporal/pull/33) [`6fd7326`](https://github.com/joepjoosten/effect-temporal/commit/6fd7326688569f45f4df710aed74d94419c50a8f) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Upgrade Temporal SDK packages to ^1.23.0

- Updated dependencies [[`d401a0a`](https://github.com/joepjoosten/effect-temporal/commit/d401a0a1f6acc6e59a066e1d2c2758453e0802c1), [`6fd7326`](https://github.com/joepjoosten/effect-temporal/commit/6fd7326688569f45f4df710aed74d94419c50a8f)]:
  - @effect-temporal/client@0.2.1
  - @effect-temporal/workflow@0.1.3

## 0.1.2

### Patch Changes

- Updated dependencies [[`0c0b8ec`](https://github.com/joepjoosten/effect-temporal/commit/0c0b8ec1bc7f3f5cd3ce9ceae788b05879288d60)]:
  - @effect-temporal/client@0.2.0
  - @effect-temporal/workflow@0.1.2

## 0.1.1

### Patch Changes

- [#13](https://github.com/joepjoosten/effect-temporal/pull/13) [`102b696`](https://github.com/joepjoosten/effect-temporal/commit/102b6965a6126cdbb146c5236aa2408360916dba) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Update the packages to Effect 4.0.0-rc.111 and migrate the test workspace to the Vitest 4 projects configuration.

- Updated dependencies [[`102b696`](https://github.com/joepjoosten/effect-temporal/commit/102b6965a6126cdbb146c5236aa2408360916dba)]:
  - @effect-temporal/client@0.1.1
  - @effect-temporal/workflow@0.1.1

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
  - @effect-temporal/workflow@0.1.0
