import { describe, expect, it } from "@effect/vitest"
import type { Workflow, WorkflowClient } from "@temporalio/client"
import { defaultPayloadConverter } from "@temporalio/common"
import { loadDataConverter } from "@temporalio/common/lib/internal-non-workflow/data-converter-helpers.js"
import { temporal } from "@temporalio/proto"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TemporalWorkflowClient from "../src/TemporalWorkflowClient.js"

const loadedDataConverter = loadDataConverter(undefined)

const rawExecution = (workflowId: string) =>
  temporal.api.workflow.v1.WorkflowExecutionInfo.fromObject({
    type: { name: "MyWorkflow" },
    execution: { workflowId, runId: "run-id" },
    taskQueue: "task-queue",
    status: temporal.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
    historyLength: 10,
    startTime: { seconds: 1700000000, nanos: 0 }
  })

const clientWithWorkflowService = (workflowService: Record<string, unknown>): WorkflowClient =>
  ({
    options: { namespace: "test-namespace", loadedDataConverter },
    workflowService
  }) as unknown as WorkflowClient

describe("TemporalWorkflowClient", () => {
  it.effect("validates workflow args and result with Schema", () =>
    Effect.gen(function*() {
      const unsafeClient = {
        execute: async (_workflow: string | Workflow, options: { readonly args?: ReadonlyArray<unknown> }) =>
          `${options.args?.[0]}:result`
      } as unknown as WorkflowClient
      const client = TemporalWorkflowClient.fromUnsafe(unsafeClient)

      const result = yield* client.executeWithSchema(
        "Workflow",
        {
          args: ["input"],
          taskQueue: "queue",
          workflowId: "workflow-id"
        },
        {
          args: Schema.Tuple([Schema.String]),
          result: Schema.String
        }
      )

      expect(result).toBe("input:result")
    }))

  it.effect("does not call Temporal when workflow args fail Schema validation", () =>
    Effect.gen(function*() {
      let called = false
      const unsafeClient = {
        execute: async () => {
          called = true
          return "should-not-run"
        }
      } as unknown as WorkflowClient
      const client = TemporalWorkflowClient.fromUnsafe(unsafeClient)

      const exit = yield* Effect.exit(
        client.executeWithSchema(
          "Workflow",
          {
            args: [123] as any,
            taskQueue: "queue",
            workflowId: "workflow-id"
          },
          {
            args: Schema.Tuple([Schema.String]),
            result: Schema.String
          }
        )
      )

      expect(called).toBe(false)
      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") {
        expect(String(exit.cause)).toContain("TemporalValidationError")
        expect(String(exit.cause)).toContain("WorkflowClient.execute.args")
      }
    }))

  it.effect("listPaged paginates raw workflow executions and reports page progress", () =>
    Effect.gen(function*() {
      const requests: Array<{ pageSize: number; nextPageToken: Uint8Array }> = []
      const progress: Array<TemporalWorkflowClient.ListPagedProgress> = []
      const unsafeClient = clientWithWorkflowService({
        listWorkflowExecutions: async (request: { pageSize: number; nextPageToken: Uint8Array }) => {
          requests.push(request)
          return request.nextPageToken.length === 0
            ? {
              executions: [rawExecution("workflow-1"), rawExecution("workflow-2")],
              nextPageToken: new Uint8Array([1])
            }
            : {
              executions: [rawExecution("workflow-3")],
              nextPageToken: new Uint8Array()
            }
        }
      })
      const client = TemporalWorkflowClient.fromUnsafe(unsafeClient)

      const workflows = yield* Stream.runCollect(
        client.listPaged({
          pageSize: 2,
          query: "ExecutionStatus=\"Running\"",
          onPage: (page) => Effect.sync(() => void progress.push(page))
        })
      )

      expect(Array.from(workflows).map((workflow) => workflow.workflowId)).toEqual([
        "workflow-1",
        "workflow-2",
        "workflow-3"
      ])
      expect(Array.from(workflows)[0]?.type).toBe("MyWorkflow")
      expect(Array.from(workflows)[0]?.status.name).toBe("RUNNING")
      expect(requests).toHaveLength(2)
      expect(progress).toEqual([
        {
          page: 1,
          requestedPageSize: 2,
          receivedItems: 2,
          emittedItems: 2,
          totalEmittedItems: 2,
          hasNextPage: true
        },
        {
          page: 2,
          requestedPageSize: 2,
          receivedItems: 1,
          emittedItems: 1,
          totalEmittedItems: 3,
          hasNextPage: false
        }
      ])
    }))

  it.effect("listPaged stops at the limit without fetching further pages", () =>
    Effect.gen(function*() {
      let calls = 0
      const unsafeClient = clientWithWorkflowService({
        listWorkflowExecutions: async (_request: { pageSize: number }) => {
          calls += 1
          return {
            executions: [rawExecution(`workflow-${calls}-a`), rawExecution(`workflow-${calls}-b`)],
            nextPageToken: new Uint8Array([calls])
          }
        }
      })
      const client = TemporalWorkflowClient.fromUnsafe(unsafeClient)

      const workflows = yield* Stream.runCollect(client.listPaged({ pageSize: 2, limit: 3 }))

      expect(Array.from(workflows)).toHaveLength(3)
      expect(calls).toBe(2)
    }))

  it.effect("startInput decodes the WorkflowExecutionStarted input payloads", () =>
    Effect.gen(function*() {
      const unsafeClient = clientWithWorkflowService({
        getWorkflowExecutionHistory: async () => ({
          history: {
            events: [
              {
                workflowExecutionStartedEventAttributes: {
                  input: { payloads: [defaultPayloadConverter.toPayload({ customerId: "123" })] }
                }
              }
            ]
          }
        })
      })
      const client = TemporalWorkflowClient.fromUnsafe(unsafeClient)

      const input = yield* client.startInput("workflow-id", "run-id")

      expect(input).toEqual([{ customerId: "123" }])
    }))

  it.effect("startInput fails with TemporalClientError when the started event is missing", () =>
    Effect.gen(function*() {
      const unsafeClient = clientWithWorkflowService({
        getWorkflowExecutionHistory: async () => ({ history: { events: [] } })
      })
      const client = TemporalWorkflowClient.fromUnsafe(unsafeClient)

      const exit = yield* Effect.exit(client.startInput("workflow-id"))

      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") {
        expect(String(exit.cause)).toContain("TemporalClientError")
        expect(String(exit.cause)).toContain("WorkflowClient.startInput")
      }
    }))

  it.effect("fetchHistoryEvents follows history pagination", () =>
    Effect.gen(function*() {
      const unsafeClient = clientWithWorkflowService({
        getWorkflowExecutionHistory: async (request: { nextPageToken?: Uint8Array | null }) =>
          request.nextPageToken == null || request.nextPageToken.length === 0
            ? {
              history: { events: [{ eventId: 1 }, { eventId: 2 }] },
              nextPageToken: new Uint8Array([1])
            }
            : {
              history: { events: [{ eventId: 3 }] },
              nextPageToken: new Uint8Array()
            }
      })
      const client = TemporalWorkflowClient.fromUnsafe(unsafeClient)

      const events = yield* client.fetchHistoryEvents("workflow-id", "run-id", { pageSize: 2 })

      expect(events.map((event) => event.eventId)).toEqual([1, 2, 3])
    }))

  it.effect("decodePayloads and decodeFailure decode raw history data", () =>
    Effect.gen(function*() {
      const client = TemporalWorkflowClient.fromUnsafe(clientWithWorkflowService({}))

      const values = yield* client.decodePayloads([
        defaultPayloadConverter.toPayload("hello"),
        defaultPayloadConverter.toPayload(42)
      ])
      const error = yield* client.decodeFailure({
        message: "boom",
        applicationFailureInfo: { type: "MyError" }
      })
      const missing = yield* client.decodeFailure(undefined)

      expect(values).toEqual(["hello", 42])
      expect(error?.message).toBe("boom")
      expect(missing).toBeUndefined()
    }))
})
