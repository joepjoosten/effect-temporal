/**
 * Global polyfills that make the Temporal workflow sandbox safe for Effect
 * programs.
 *
 * The workflow isolate lacks `crypto`, `TextEncoder`, and `performance`, all
 * of which Effect touches: workflow execution ids are derived with
 * `crypto.subtle.digest("SHA-256", new TextEncoder().encode(...))`, the
 * default random service seeds from `crypto.getRandomValues`, and the clock
 * captures `performance.now` (with module-level fallback state) the moment the
 * `effect` module evaluates.
 *
 * This module must therefore evaluate before any `effect` module inside the
 * workflow bundle and must not import `effect` itself. Importing
 * `TemporalWorkflowRuntime` (or `TemporalSandbox`) first in the workflow
 * entrypoint is sufficient; the polyfills are also re-installed defensively on
 * every workflow run.
 *
 * @since 1.0.0
 */
import { sha256 } from "@noble/hashes/sha2.js"
import { inWorkflowContext } from "@temporalio/workflow"

/**
 * A `TextEncoder` replacement producing byte-identical UTF-8 output to the
 * platform encoder, including lone surrogates encoded as U+FFFD.
 *
 * @since 1.0.0
 * @category Polyfills
 */
export class SandboxTextEncoder {
  readonly encoding = "utf-8"

  encode(input = ""): Uint8Array {
    const bytes: Array<number> = []
    for (let i = 0; i < input.length; i++) {
      let code = input.charCodeAt(i)
      if (code >= 0xd800 && code <= 0xdbff) {
        const next = i + 1 < input.length ? input.charCodeAt(i + 1) : 0
        if (next >= 0xdc00 && next <= 0xdfff) {
          code = 0x10000 + ((code - 0xd800) << 10) + (next - 0xdc00)
          i++
        } else {
          code = 0xfffd
        }
      } else if (code >= 0xdc00 && code <= 0xdfff) {
        code = 0xfffd
      }
      if (code <= 0x7f) {
        bytes.push(code)
      } else if (code <= 0x7ff) {
        bytes.push(0xc0 | (code >> 6), 0x80 | (code & 0x3f))
      } else if (code <= 0xffff) {
        bytes.push(0xe0 | (code >> 12), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f))
      } else {
        bytes.push(
          0xf0 | (code >> 18),
          0x80 | ((code >> 12) & 0x3f),
          0x80 | ((code >> 6) & 0x3f),
          0x80 | (code & 0x3f)
        )
      }
    }
    return Uint8Array.from(bytes)
  }

  encodeInto(source: string, destination: Uint8Array): { read: number; written: number } {
    const encoded = this.encode(source)
    const written = Math.min(encoded.length, destination.length)
    destination.set(encoded.subarray(0, written))
    return { read: source.length, written }
  }
}

const toBytes = (data: ArrayBuffer | ArrayBufferView): Uint8Array =>
  data instanceof Uint8Array
    ? data
    : ArrayBuffer.isView(data)
    ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    : new Uint8Array(data)

/**
 * The `crypto.subtle.digest` replacement installed in the sandbox. Exported so
 * tests can pin it byte-for-byte against the platform implementation:
 * execution ids hashed inside the sandbox must equal the ones computed in
 * Node.
 *
 * @since 1.0.0
 * @category Polyfills
 */
export const sandboxSubtleDigest = async (
  algorithm: string | { readonly name: string },
  data: ArrayBuffer | ArrayBufferView
): Promise<ArrayBuffer> => {
  const name = typeof algorithm === "string" ? algorithm : algorithm.name
  if (name.toUpperCase() !== "SHA-256") {
    throw new Error(`Temporal sandbox crypto.subtle.digest only supports SHA-256, received "${name}"`)
  }
  const digest = sha256(toBytes(data))
  return digest.buffer.slice(digest.byteOffset, digest.byteOffset + digest.byteLength) as ArrayBuffer
}

const getRandomValues = <T extends ArrayBufferView>(array: T): T => {
  // Math.random is patched deterministically by the Temporal sandbox, so
  // filling from it keeps replay stable.
  const view = new Uint8Array(array.buffer, array.byteOffset, array.byteLength)
  for (let i = 0; i < view.length; i++) {
    view[i] = Math.floor(Math.random() * 256)
  }
  return array
}

/**
 * Installs the sandbox polyfills when running inside a Temporal workflow
 * isolate. Outside the isolate this is a no-op. Existing globals are never
 * replaced, except `performance`, which is pinned to the deterministic
 * sandbox `Date.now` so Effect's monotonic clock cannot capture real time or
 * leak an origin across workflow instances under `reuseV8Context`.
 *
 * @since 1.0.0
 * @category Polyfills
 */
export const ensureSandboxPolyfills = (): void => {
  if (!inWorkflowContext()) {
    return
  }
  const globals = globalThis as any
  if (globals.TextEncoder === undefined) {
    globals.TextEncoder = SandboxTextEncoder
  }
  if (globals.crypto === undefined) {
    globals.crypto = {}
  }
  if (globals.crypto.getRandomValues === undefined) {
    globals.crypto.getRandomValues = getRandomValues
  }
  if (globals.crypto.subtle === undefined) {
    globals.crypto.subtle = {}
  }
  if (globals.crypto.subtle.digest === undefined) {
    globals.crypto.subtle.digest = sandboxSubtleDigest
  }
  globals.performance = {
    now: () => Date.now(),
    timeOrigin: 0
  }
}

ensureSandboxPolyfills()
