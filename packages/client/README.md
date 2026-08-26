# `@effect-temporal/client`

Effect services and helpers around `@temporalio/client`.

```ts
import * as TemporalClient from "@effect-temporal/client/TemporalClient"
import * as TemporalConnection from "@effect-temporal/client/TemporalConnection"
import * as Effect from "effect/Effect"

const program = Effect.gen(function*() {
  const client = yield* TemporalClient.TemporalClient

  return yield* client.workflow.execute("ExampleWorkflow", {
    args: ["hello"],
    taskQueue: "example",
    workflowId: "example-workflow"
  })
})

await Effect.runPromise(
  Effect.scoped(program).pipe(
    Effect.provide(TemporalClient.layer()),
    Effect.provide(TemporalConnection.layer())
  )
)
```

The package keeps the underlying Temporal SDK objects available through
`unsafeConnection`, `unsafeClient`, and `unsafeHandle` fields so advanced APIs
remain accessible while common promise-returning operations are mapped into
typed Effect failures.

Schema validation can be applied at Temporal boundaries:

```ts
import * as Schema from "effect/Schema"

const result = yield* client.workflow.executeWithSchema(
  "ExampleWorkflow",
  {
    args: ["hello"],
    taskQueue: "example",
    workflowId: "example-workflow"
  },
  {
    args: Schema.Tuple([Schema.String]),
    result: Schema.String
  }
)
```

Input validation runs before the Temporal call. Output validation runs after
the SDK returns and fails with `TemporalValidationError` when decoding fails.

Payload encryption can be installed through an Effect-backed key provider:

```ts
import * as TemporalClient from "@effect-temporal/client/TemporalClient"
import * as TemporalConnection from "@effect-temporal/client/TemporalConnection"
import * as TemporalDataConverter from "@effect-temporal/client/TemporalDataConverter"
import * as Effect from "effect/Effect"
import * as Redacted from "effect/Redacted"

const keyProvider = TemporalDataConverter.keyProviderLayer({
  generateDataKey: Effect.succeed({
    key: Redacted.make(new Uint8Array(32)),
    metadata: {
      [TemporalDataConverter.METADATA_ENCRYPTED_DATA_KEY]: encryptedDataKey
    }
  }),
  decryptDataKey: (metadata) => fetchPlaintextKey(metadata)
})

await Effect.runPromise(
  program.pipe(
    Effect.provide(TemporalClient.layerWithDataConverter()),
    Effect.provide(TemporalDataConverter.layerWithCodec()),
    Effect.provide(keyProvider),
    Effect.provide(TemporalConnection.layer())
  )
)
```

The codec is AWS-independent. AWS KMS or another key store should live in the
`TemporalCodecKeyProvider` layer and return the plaintext data key as an Effect
when encoding or decoding payloads.

Low-level visibility and history access is available on the workflow client for
tooling that needs raw pagination control or decoded history payloads:

```ts
const client = yield * TemporalClient.TemporalClient

// Page-by-page listing with progress reporting and an item limit
const workflows = client.workflow.listPaged({
  query: "ExecutionStatus=\"Failed\"",
  pageSize: 100,
  limit: 1000,
  onPage: (progress) => Effect.log(`page ${progress.page}: ${progress.totalEmittedItems} workflows`)
})

// Decoded start input and raw history events
const input = yield * client.workflow.startInput(workflowId, runId)
const events = yield * client.workflow.fetchHistoryEvents(workflowId, runId)

// Decode payloads or failures found in raw history events
const args = yield * client.workflow.decodePayloads(
  events[0]?.activityTaskScheduledEventAttributes?.input?.payloads
)
```

All of these use the client's loaded data converter, so encrypted payloads are
decrypted when a payload codec is configured.
