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

  it("encodeInto matches platform offsets and bytes at every buffer boundary", () => {
    for (const input of trickyStrings) {
      const platform = new TextEncoder()
      const sandbox = new SandboxTextEncoder()
      for (let size = 0; size <= platform.encode(input).length + 1; size++) {
        const expected = new Uint8Array(size).fill(0xff)
        const actual = new Uint8Array(size).fill(0xff)
        expect(sandbox.encodeInto(input, actual), `${JSON.stringify(input)} / ${size}`)
          .toEqual(platform.encodeInto(input, expected))
        expect(actual).toEqual(expected)
      }
    }
  })

  it("streams text without skipping input or writing partial characters", () => {
    const input = "a€😀\ud800z"
    const encoder = new SandboxTextEncoder()
    const output: Array<number> = []
    let offset = 0
    while (offset < input.length) {
      const buffer = new Uint8Array(4)
      const { read, written } = encoder.encodeInto(input.slice(offset), buffer)
      expect(read).toBeGreaterThan(0)
      for (const byte of buffer.subarray(0, written)) output.push(byte)
      offset += read
    }
    expect(output).toEqual(Array.from(new TextEncoder().encode(input)))
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

  it("sandboxSubtleDigest matches the platform across block and padding boundaries", async () => {
    // 55/56/63/64 bytes straddle the single-block padding boundary; the rest
    // exercise multi-block hashing.
    for (const length of [0, 1, 31, 32, 55, 56, 57, 63, 64, 65, 119, 120, 121, 127, 128, 1000]) {
      const data = new Uint8Array(length)
      for (let i = 0; i < length; i++) {
        data[i] = (i * 37 + 11) % 256
      }
      const expected = new Uint8Array(await crypto.subtle.digest("SHA-256", data))
      const actual = new Uint8Array(await sandboxSubtleDigest("SHA-256", data))
      expect(Array.from(actual), `length ${length}`).toEqual(Array.from(expected))
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
