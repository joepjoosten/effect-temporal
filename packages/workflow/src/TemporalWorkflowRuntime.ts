/**
 * @since 1.0.0
 */
import { ensureSandboxPolyfills, sandboxScheduler } from "./TemporalSandbox.js"

import { WorkflowExecutionAlreadyStartedError } from "@temporalio/common"
import {
  CancellationScope,
  condition,
  ContinueAsNew,
  defineQuery,
  defineSignal,
  executeChild,
  getExternalWorkflowHandle,
  patched,
  proxyActivities,
  proxyLocalActivities,
  setHandler,
  sleep,
  startChild,
  workflowInfo
} from "@temporalio/workflow"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Activity from "effect/unstable/workflow/Activity"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import type {
  CompleteDeferredSignal,
  MailboxSignal,
  ScheduleClockSignal,
  TemporalDeferredResult,
  TemporalStateCellResult,
  TemporalUpdateResult,
  TemporalWorkflowState,
  UpdateSignal
} from "./TemporalWorkflowProtocol.js"
import {
  completeDeferredSignalName,
  deferredResultQueryName,
  interruptSignalName,
  mailboxSignalName,
  resumeSignalName,
  scheduleClockSignalName,
  stateCellQueryName,
  updateResultQueryName,
  updateSignalName,
  workflowIdFor,
  workflowStateQueryName
} from "./TemporalWorkflowProtocol.js"
import {
  decodeDeferredExit,
  decodeWorkflowFailure,
  encodeDeferredExit,
  encodeWorkflowFailure,
  wireCodecsFor
} from "./TemporalWorkflowWire.js"

/**
 * @since 1.0.0
 * @category Protocol
 */
export const workflowStateQuery = defineQuery<TemporalWorkflowState>(workflowStateQueryName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const deferredResultQuery = defineQuery<TemporalDeferredResult, [name: string]>(deferredResultQueryName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const interruptSignal = defineSignal(interruptSignalName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const resumeSignal = defineSignal(resumeSignalName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const completeDeferredSignal = defineSignal<[CompleteDeferredSignal]>(completeDeferredSignalName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const scheduleClockSignal = defineSignal<[ScheduleClockSignal]>(scheduleClockSignalName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const stateCellQuery = defineQuery<TemporalStateCellResult, [name: string]>(stateCellQueryName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const mailboxSignal = defineSignal<[MailboxSignal]>(mailboxSignalName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const updateSignal = defineSignal<[UpdateSignal]>(updateSignalName)

/**
 * @since 1.0.0
 * @category Protocol
 */
export const updateResultQuery = defineQuery<TemporalUpdateResult, [requestId: string]>(updateResultQueryName)

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalWorkflowRuntimeState {
  readonly executionId: string
  readonly workflowIdPrefix?: string | undefined
  status: TemporalWorkflowState["status"]
  interrupted: boolean
  resumed: boolean
  result: TemporalWorkflowState["result"]
  readonly deferreds: Map<string, unknown>
  readonly clocks: Map<string, ScheduleClockSignal>
  readonly activityResults: Map<string, Workflow.ResultEncoded<unknown, unknown>>
  readonly inFlight: Set<CancellationScope>
  readonly stateCells: Map<string, unknown>
  /**
   * Append-only log of received mailbox messages per mailbox name; consumed
   * positions are tracked separately in `mailboxCursors` so a resumed pass of
   * the workflow body replays takes deterministically.
   */
  readonly mailboxLogs: Map<string, Array<unknown>>
  readonly mailboxCursors: Map<string, number>
  /**
   * Append-only log of received update requests per update name, consumed
   * through `updateCursors` like mailbox messages; responses are recorded per
   * request id and served by the update-result query.
   */
  readonly updateLogs: Map<string, Array<UpdateSignal>>
  readonly updateCursors: Map<string, number>
  readonly updateRequests: Map<string, UpdateSignal>
  readonly updateResults: Map<string, unknown>
  /**
   * Completed child-workflow results keyed by child execution id, so a
   * resumed pass of the body does not start a duplicate child run.
   */
  readonly childResults: Map<string, Workflow.Result<unknown, unknown>>
  /**
   * Memoized typed-activity exits keyed by activity name and per-pass call
   * sequence; `typedActivitySeq` resets each pass so calls replay in order.
   */
  readonly typedActivityResults: Map<string, unknown>
  readonly typedActivitySeq: Map<string, number>
  readonly outboundCalls: Map<string, Promise<unknown>>
  readonly outboundSeq: Map<string, number>
  runScope?: CancellationScope | undefined
}

/**
 * Service exposing the per-run runtime state to workflow-side primitives
 * (state cells, mailboxes, updates). Provided automatically by
 * `makeWorkflow`; only available inside a Temporal workflow run.
 *
 * @since 1.0.0
 * @category Tags
 */
export const TemporalSandboxRun = Context.Service<
  TemporalSandboxRun,
  TemporalWorkflowRuntimeState
>("@effect-temporal/workflow/TemporalSandboxRun")

/**
 * @since 1.0.0
 * @category Models
 */
export type TemporalSandboxRun = TemporalWorkflowRuntimeState

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeRuntimeState = (
  executionId: string,
  workflowIdPrefix?: string
): TemporalWorkflowRuntimeState => ({
  executionId,
  workflowIdPrefix,
  status: "running",
  interrupted: false,
  resumed: false,
  result: undefined,
  deferreds: new Map(),
  clocks: new Map(),
  activityResults: new Map(),
  inFlight: new Set(),
  stateCells: new Map(),
  mailboxLogs: new Map(),
  mailboxCursors: new Map(),
  updateLogs: new Map(),
  updateCursors: new Map(),
  updateRequests: new Map(),
  updateResults: new Map(),
  childResults: new Map(),
  typedActivityResults: new Map(),
  typedActivitySeq: new Map(),
  outboundCalls: new Map(),
  outboundSeq: new Map()
})

/**
 * Reuses an outbound command at the same position across suspended passes.
 * Both successful and failing outcomes are retained: explicit retries advance
 * the sequence and remain new commands. Pending promises are shared too.
 *
 * @since 1.0.0
 * @category Runtime
 */
export const memoizeOutboundCommand = <A>(
  state: TemporalWorkflowRuntimeState,
  key: string,
  run: () => Promise<A>
): Promise<A> => {
  const legacy = !patched("effect-temporal/outbound-command-cache-v1")
  const sequence = state.outboundSeq.get(key) ?? 0
  state.outboundSeq.set(key, sequence + 1)
  const memoKey = JSON.stringify([key, sequence])
  let result = state.outboundCalls.get(memoKey)
  if (legacy || result === undefined) {
    result = run()
    state.outboundCalls.set(memoKey, result)
  }
  return result as Promise<A>
}

/**
 * Propagates Effect fiber interruption to a scheduled Temporal command before
 * removing its scope from root-cancellation tracking.
 *
 * @since 1.0.0
 * @category Runtime
 */
export const releaseCancellationScope = (
  state: TemporalWorkflowRuntimeState,
  scope: CancellationScope,
  exit: Exit.Exit<unknown, unknown>
): void => {
  if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause) && patched("effect-temporal/fiber-cancellation-v1")) {
    scope.cancel()
  }
  state.inFlight.delete(scope)
}

const scheduleRuntimeClock = (state: TemporalWorkflowRuntimeState, clock: ScheduleClockSignal): void => {
  if (state.clocks.has(clock.name)) {
    return
  }
  state.clocks.set(clock.name, clock)
  void CancellationScope.nonCancellable(() => sleep(clock.durationMs)).then(() => {
    state.deferreds.set(clock.deferredName ?? `DurableClock/${clock.name}`, encodeDeferredExit(Exit.void))
    if (!state.interrupted) {
      state.resumed = true
      state.status = "running"
    }
  })
}

/**
 * @since 1.0.0
 * @category Runtime
 */
export const installBaseHandlers = (
  state: TemporalWorkflowRuntimeState
): void => {
  setHandler(workflowStateQuery, () => ({
    executionId: state.executionId,
    status: state.status,
    result: state.result
  }))
  setHandler(deferredResultQuery, (name) => {
    const exit = state.deferreds.get(name)
    return exit === undefined
      ? { found: false }
      : { found: true, exit }
  })
  setHandler(interruptSignal, () => {
    state.interrupted = true
    state.resumed = false
    state.status = "suspended"
  })
  setHandler(resumeSignal, () => {
    state.interrupted = false
    state.resumed = true
    state.status = "running"
  })
  setHandler(completeDeferredSignal, ({ exit, name }) => {
    state.deferreds.set(name, exit)
    if (!state.interrupted) {
      state.resumed = true
      state.status = "running"
    }
  })
  setHandler(scheduleClockSignal, (clock) => {
    if (patched("effect-temporal/signal-clock-v1")) {
      scheduleRuntimeClock(state, clock)
    } else {
      state.clocks.set(clock.name, clock)
    }
  })
  setHandler(stateCellQuery, (name) => {
    const value = state.stateCells.get(name)
    return value === undefined
      ? { found: false }
      : { found: true, value }
  })
  setHandler(mailboxSignal, ({ name, payload }) => {
    let log = state.mailboxLogs.get(name)
    if (log === undefined) {
      log = []
      state.mailboxLogs.set(name, log)
    }
    log.push(payload)
  })
  setHandler(updateSignal, (request) => {
    if (state.updateRequests.has(request.requestId)) {
      return
    }
    state.updateRequests.set(request.requestId, request)
    let log = state.updateLogs.get(request.name)
    if (log === undefined) {
      log = []
      state.updateLogs.set(request.name, log)
    }
    log.push(request)
  })
  setHandler(updateResultQuery, (requestId) => {
    const exit = state.updateResults.get(requestId)
    return exit === undefined
      ? { found: false, request: state.updateRequests.get(requestId) }
      : { found: true, exit, request: state.updateRequests.get(requestId) }
  })
}

/**
 * @since 1.0.0
 * @category Runtime
 */
export const waitForResume = async (
  state: TemporalWorkflowRuntimeState
): Promise<void> => {
  await condition(() => state.resumed || state.interrupted)
  if (state.resumed) {
    state.resumed = false
  }
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalActivityInvocation {
  readonly executionId: string
  readonly attempt: number
}

/**
 * @since 1.0.0
 * @category Models
 */
export type TemporalActivityProxy =
  | {
    readonly local?: false | undefined
    readonly options?: Parameters<typeof proxyActivities>[0] | undefined
  }
  | {
    readonly local: true
    readonly options?: Parameters<typeof proxyLocalActivities>[0] | undefined
  }

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalWorkflowRuntimeOptions<
  Payload,
  Success,
  Error,
  R
> {
  readonly workflow: Workflow.Workflow<any, any, any, any>
  readonly execute: (
    payload: Payload,
    executionId: string
  ) => Effect.Effect<Success, Error, R>
  /** Must match the client engine prefix when starting child workflows. */
  readonly workflowIdPrefix?: string | undefined
  readonly activityProxy?: TemporalActivityProxy | undefined
  readonly provide?: ((<A, E, R2>(effect: Effect.Effect<A, E, R2>) => Effect.Effect<A, E, never>)) | undefined
}

const defaultActivityOptions = {
  startToCloseTimeout: "10 minutes"
} satisfies Parameters<typeof proxyActivities>[0]

const AnyOrVoid = Schema.Union([Schema.Any, Schema.Void])
const WorkflowResultSchema = Workflow.Result({
  success: AnyOrVoid,
  error: AnyOrVoid
})
const WorkflowResultCodec = Schema.toCodecJson(WorkflowResultSchema)
const encodeWorkflowResult = Schema.encodeUnknownSync(WorkflowResultCodec)
const decodeWorkflowResult = Schema.decodeUnknownSync(WorkflowResultCodec)

const executionIdFromWorkflowId = (
  workflowId: string
): string => {
  const index = workflowId.lastIndexOf("/")
  return index === -1 ? workflowId : workflowId.slice(index + 1)
}

const unsupported = (message: string): Effect.Effect<never, never> => Effect.die(new Error(message))

const isWorkflowExecutionAlreadyStartedError = (u: unknown): u is WorkflowExecutionAlreadyStartedError =>
  u instanceof WorkflowExecutionAlreadyStartedError

const findContinueAsNew = (cause: Cause.Cause<unknown>): ContinueAsNew | undefined => {
  for (const reason of cause.reasons) {
    if (Cause.isDieReason(reason) && reason.defect instanceof ContinueAsNew) {
      return reason.defect
    }
  }
  return undefined
}

const makeActivityCaller = (
  activityProxy: TemporalActivityProxy | undefined
): Record<string, (input: TemporalActivityInvocation) => Promise<Workflow.ResultEncoded<unknown, unknown>>> =>
  activityProxy?.local === true
    ? proxyLocalActivities<
      Record<string, (input: TemporalActivityInvocation) => Promise<Workflow.ResultEncoded<unknown, unknown>>>
    >(
      activityProxy.options ?? defaultActivityOptions
    )
    : proxyActivities<
      Record<string, (input: TemporalActivityInvocation) => Promise<Workflow.ResultEncoded<unknown, unknown>>>
    >(
      activityProxy?.options ?? defaultActivityOptions
    )

const makeRuntimeEngine = (
  state: TemporalWorkflowRuntimeState,
  activityCaller: Record<
    string,
    (input: TemporalActivityInvocation) => Promise<Workflow.ResultEncoded<unknown, unknown>>
  >,
  workflowName: string
): WorkflowEngine.WorkflowEngine["Service"] => {
  const isLocal = (name: string, executionId: string): boolean =>
    (name === workflowName && executionId === state.executionId)
    || !patched("effect-temporal/target-workflow-routing-v1")
  const signalTarget = (name: string, executionId: string, signal: string, ...args: Array<unknown>) =>
    Effect.promise(() =>
      getExternalWorkflowHandle(
        workflowIdFor(name, executionId, state.workflowIdPrefix)
      ).signal(signal, ...args)
    )
  const control = (signal: string, change: () => void) => (workflow: Workflow.Any, executionId: string) =>
    Effect.suspend(() =>
      isLocal(workflow._tag, executionId)
        ? Effect.sync(change)
        : signalTarget(workflow._tag, executionId, signal)
    )
  return WorkflowEngine.makeUnsafe({
    register: () => unsupported("Workflow registration is not available inside a Temporal workflow runtime"),
    execute: (workflow: Workflow.Any, options: any) =>
      options.discard
        ? Effect.tryPromise({
          try: () =>
            startChild(workflow._tag, {
              workflowId: patched("effect-temporal/child-workflow-ids-v1")
                ? workflowIdFor(workflow._tag, options.executionId, state.workflowIdPrefix)
                : options.executionId,
              args: [wireCodecsFor(workflow).encodePayload(options.payload)]
            }),
          catch: (cause) => cause
        }).pipe(
          // A resumed pass re-issues the start; an already-running child is
          // not an error.
          Effect.catch((cause) => isWorkflowExecutionAlreadyStartedError(cause) ? Effect.void : Effect.die(cause)),
          Effect.asVoid
        ) as any
        : Effect.suspend(() => {
          const cached = state.childResults.get(options.executionId)
          if (cached !== undefined) {
            return Effect.succeed(cached)
          }
          return Effect.tryPromise({
            try: () =>
              executeChild(workflow._tag, {
                workflowId: patched("effect-temporal/child-workflow-ids-v1")
                  ? workflowIdFor(workflow._tag, options.executionId, state.workflowIdPrefix)
                  : options.executionId,
                args: [wireCodecsFor(workflow).encodePayload(options.payload)]
              }),
            catch: (cause) => cause
          }).pipe(
            Effect.match({
              onFailure: (cause) =>
                decodeWorkflowFailure(workflow, cause)
                  ?? new Workflow.Complete({ exit: Exit.die(cause) }),
              onSuccess: (value) =>
                new Workflow.Complete({
                  exit: Exit.succeed(wireCodecsFor(workflow).decodeSuccess(value))
                })
            }),
            Effect.tap((result) =>
              Effect.sync(() => {
                state.childResults.set(options.executionId, result)
              })
            )
          )
        }) as any,
    poll: () => unsupported("Workflow polling is not available inside a Temporal workflow runtime"),
    interrupt: control(interruptSignalName, () => {
      state.interrupted = true
      state.resumed = false
      state.status = "suspended"
    }),
    interruptUnsafe: control(interruptSignalName, () => {
      state.interrupted = true
      state.resumed = false
      state.status = "suspended"
    }),
    resume: control(resumeSignalName, () => {
      state.interrupted = false
      state.resumed = true
      state.status = "running"
    }),
    activityExecute: Effect.fnUntraced(function*(activity, attempt) {
      const invoke = activityCaller[activity.name]
      if (invoke === undefined) {
        return yield* unsupported(`Temporal activity "${activity.name}" is not registered on the worker`)
      }
      // Memoize completed results so a resumed run does not re-schedule
      // activities that already finished in an earlier pass of the body.
      const memoKey = `${activity.name}/${attempt}`
      const cached = state.activityResults.get(memoKey)
      if (cached !== undefined) {
        return decodeWorkflowResult(cached) as Workflow.Result<unknown, unknown>
      }
      const instance = yield* WorkflowEngine.WorkflowInstance
      // Each call gets its own cancellation scope so an interrupt or a
      // Temporal cancellation can cancel the scheduled activity itself. The
      // parent is the non-cancellable run scope: compensation activities
      // scheduled after a cancellation must still be able to run.
      const scope = new CancellationScope(
        state.runScope === undefined
          ? undefined
          : { cancellable: true, parent: state.runScope }
      )
      state.inFlight.add(scope)
      const encoded = yield* Effect.tryPromise({
        try: () =>
          scope.run(() =>
            invoke({
              executionId: instance.executionId,
              attempt
            })
          ),
        catch: (cause) => new Error(`Temporal activity "${activity.name}" failed`, { cause })
      }).pipe(
        Effect.orDie,
        Effect.onExit((exit) => Effect.sync(() => releaseCancellationScope(state, scope, exit)))
      )
      const result = decodeWorkflowResult(encoded) as Workflow.Result<unknown, unknown>
      if (result._tag === "Complete") {
        state.activityResults.set(memoKey, encoded)
      }
      return result
    }),
    deferredResult: (deferred) =>
      Effect.sync(() => {
        const exit = state.deferreds.get(deferred.name)
        return exit === undefined ? Option.none() : Option.some(decodeDeferredExit(exit))
      }),
    deferredDone: ({ deferredName, executionId, exit, workflowName }) =>
      Effect.suspend(() => {
        if (!isLocal(workflowName, executionId)) {
          return signalTarget(
            workflowName,
            executionId,
            completeDeferredSignalName,
            {
              name: deferredName,
              exit: encodeDeferredExit(exit)
            } satisfies CompleteDeferredSignal
          )
        }
        return Effect.sync(() => {
          state.deferreds.set(deferredName, encodeDeferredExit(exit))
          if (!state.interrupted) {
            state.resumed = true
            state.status = "running"
          }
        })
      }),
    scheduleClock: (workflow, options) =>
      Effect.suspend(() => {
        const clock = {
          name: options.clock.name,
          durationMs: Duration.toMillis(options.clock.duration),
          deferredName: options.clock.deferred.name
        } satisfies ScheduleClockSignal
        return isLocal(workflow._tag, options.executionId)
          ? Effect.sync(() => scheduleRuntimeClock(state, clock))
          : signalTarget(workflow._tag, options.executionId, scheduleClockSignalName, clock)
      })
  })
}

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeWorkflow = <Payload, Success, Error, R>(
  options: TemporalWorkflowRuntimeOptions<Payload, Success, Error, R>
): (payload: Payload) => Promise<Success> => {
  const activityCaller = makeActivityCaller(options.activityProxy)

  return async (payload) => {
    ensureSandboxPolyfills()
    const codecs = wireCodecsFor(options.workflow)
    const executionId = executionIdFromWorkflowId(workflowInfo().workflowId)
    const state = makeRuntimeState(executionId, options.workflowIdPrefix)
    installBaseHandlers(state)
    const decodedPayload = codecs.decodePayload(payload) as Payload

    // The run executes inside a non-cancellable scope so a Temporal
    // cancellation is observed by the Effect runtime (finalizers and
    // compensation run) instead of tearing down awaited commands directly.
    return CancellationScope.nonCancellable(async () => {
      state.runScope = CancellationScope.current()
      let cancelledFailure: unknown
      let activeInstance: WorkflowEngine.WorkflowInstance["Service"] | undefined
      let activeFiber: Fiber.Fiber<Workflow.Result<Success, Error>, never> | undefined

      const onInterrupted = (): void => {
        if (activeInstance !== undefined) {
          activeInstance.interrupted = true
          activeInstance.suspended = false
        }
        for (const scope of state.inFlight) {
          scope.cancel()
        }
        activeFiber?.interruptUnsafe()
      }

      void CancellationScope.current().cancelRequested.catch((failure) => {
        cancelledFailure = failure
        state.interrupted = true
        state.resumed = false
        state.status = "suspended"
        onInterrupted()
      })
      void condition(() => state.interrupted).then(onInterrupted)

      while (true) {
        // Each pass of the body replays consumed mailbox messages, update
        // requests, and typed-activity calls from the start of their logs.
        state.mailboxCursors.clear()
        state.updateCursors.clear()
        state.typedActivitySeq.clear()
        state.outboundSeq.clear()
        const instance = WorkflowEngine.WorkflowInstance.initial(options.workflow, executionId)
        instance.interrupted = state.interrupted
        activeInstance = instance
        const engine = makeRuntimeEngine(state, activityCaller, options.workflow._tag)
        const base = options.execute(decodedPayload, executionId).pipe(
          Effect.onExit(() => {
            if (!state.interrupted) {
              return Effect.void
            }
            instance.interrupted = true
            instance.suspended = false
            return Effect.withFiber((fiber) => Effect.interruptible(Fiber.interrupt(fiber)))
          }),
          Workflow.intoResult,
          Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
          Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
          Effect.provideService(TemporalSandboxRun, state)
        )
        const program = options.provide === undefined ? base : options.provide(base)
        const fiber = Effect.runFork(
          program as Effect.Effect<Workflow.Result<Success, Error>, never, never>,
          { scheduler: sandboxScheduler }
        )
        activeFiber = fiber
        const exit = await new Promise<Exit.Exit<Workflow.Result<Success, Error>, never>>((resolve) =>
          fiber.addObserver(resolve)
        )
        activeFiber = undefined
        activeInstance = undefined

        const result: Workflow.Result<Success, Error> = Exit.isSuccess(exit)
          ? exit.value
          : new Workflow.Complete({ exit: Exit.failCause(exit.cause) }) as Workflow.Result<Success, Error>

        if (result._tag === "Complete") {
          if (result.exit._tag === "Failure") {
            // A continue-as-new marker is not a completion: let it escape
            // untouched so Temporal continues the run chain.
            const marker = findContinueAsNew(result.exit.cause)
            if (marker !== undefined) {
              throw marker
            }
          }
          state.status = "completed"
          state.result = codecs.encodeResult(result)
          if (cancelledFailure === undefined && result.exit._tag === "Success") {
            return codecs.encodeSuccess(result.exit.value) as Success
          }
          if (cancelledFailure !== undefined) {
            throw cancelledFailure
          }
          throw encodeWorkflowFailure(options.workflow, result, (result.exit as Exit.Failure<Success, Error>).cause)
        }

        state.status = "suspended"
        state.result = codecs.encodeResult(result)
        await waitForResume(state)
        state.result = undefined
        if (!state.interrupted) {
          state.status = "running"
        }
      }
    })
  }
}

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeActivities = (
  workflow: Workflow.Any,
  activities: ReadonlyArray<Activity.Any>,
  options?: {
    readonly provide?: ((<A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, never>)) | undefined
  } | undefined
): Record<string, (input: TemporalActivityInvocation) => Promise<Workflow.ResultEncoded<unknown, unknown>>> =>
  Object.fromEntries(
    activities.map((activity) => [
      activity.name,
      async ({ attempt, executionId }: TemporalActivityInvocation) => {
        const instance = WorkflowEngine.WorkflowInstance.initial(workflow, executionId)
        const base = activity.executeEncoded.pipe(
          Workflow.intoResult,
          Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
          Effect.provideService(Activity.CurrentAttempt, attempt)
        )
        const program = options?.provide === undefined ? base : options.provide(base)
        return encodeWorkflowResult(
          await Effect.runPromise(program as Effect.Effect<Workflow.Result<any, any>, never, never>)
        ) as unknown as Workflow.ResultEncoded<unknown, unknown>
      }
    ])
  )
