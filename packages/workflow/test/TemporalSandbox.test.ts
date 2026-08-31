import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { makeSandboxScheduler } from "../src/TemporalSandbox.js"
import { sandboxSubtleDigest, SandboxTextEncoder } from "../src/TemporalSandboxPolyfills.js"

const trickyStrings = [
  "",
  "plain ascii",
  "héllo wörld",
  "中文と日本語",
  "emoji \u{1F600}\u{1F680} astral",
  "lone high surrogate \ud800 mid",
  "lone low surrogate \udc00 mid",
  "\ud800",
  "\udc00",
  "\ud800a",
  "pair at end 😀"
]

describe("TemporalSandboxPolyfills", () => {
  it("SandboxTextEncoder matches the platform TextEncoder byte-for-byte", () => {
    const platform = new TextEncoder()
    const sandbox = new SandboxTextEncoder()
    for (const input of trickyStrings) {
      expect(Array.from(sandbox.encode(input))).toEqual(Array.from(platform.encode(input)))
    }
  })

  it("sandboxSubtleDigest matches the platform crypto.subtle SHA-256", async () => {
    const encoder = new TextEncoder()
    for (const input of trickyStrings) {
      const data = encoder.encode(input)
      const expected = new Uint8Array(await crypto.subtle.digest("SHA-256", data))
      const actual = new Uint8Array(await sandboxSubtleDigest("SHA-256", data))
      expect(Array.from(actual)).toEqual(Array.from(expected))
    }
  })

  it("sandboxSubtleDigest rejects unsupported algorithms", async () => {
    await expect(sandboxSubtleDigest("SHA-1", new Uint8Array())).rejects.toThrow("SHA-256")
  })
})

describe("makeSandboxScheduler", () => {
  it("flushes scheduled tasks on the microtask queue", async () => {
    const dispatcher = makeSandboxScheduler().makeDispatcher()
    let ran = false
    dispatcher.scheduleTask(() => {
      ran = true
    }, 0)
    expect(ran).toBe(false)
    await Promise.resolve()
    expect(ran).toBe(true)
  })

  it("runs Effect programs with many fiber yields without touching timers", async () => {
    const scheduler = makeSandboxScheduler()
    const program = Effect.gen(function*() {
      let count = 0
      for (let i = 0; i < 100; i++) {
        yield* Effect.yieldNow
        count++
      }
      return count
    })
    await expect(Effect.runPromise(program, { scheduler })).resolves.toBe(100)
  })
})
