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
import { inWorkflowContext } from "@temporalio/workflow"

// Self-contained SHA-256 (FIPS 180-4) so the sandbox needs no external
// dependency; byte-pinned against the platform WebCrypto implementation in
// TemporalSandbox.test.ts.
// dprint-ignore
const SHA256_K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
])

const rotr = (x: number, n: number): number => (x >>> n) | (x << (32 - n))

const sha256 = (data: Uint8Array): Uint8Array => {
  const state = new Uint32Array([
    0x6a09e667,
    0xbb67ae85,
    0x3c6ef372,
    0xa54ff53a,
    0x510e527f,
    0x9b05688c,
    0x1f83d9ab,
    0x5be0cd19
  ])
  const length = data.length
  const paddedLength = (((length + 8) >> 6) + 1) << 6
  const padded = new Uint8Array(paddedLength)
  padded.set(data)
  padded[length] = 0x80
  const view = new DataView(padded.buffer)
  view.setUint32(paddedLength - 8, Math.floor(length / 0x20000000))
  view.setUint32(paddedLength - 4, (length << 3) >>> 0)

  const w = new Uint32Array(64)
  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let i = 0; i < 16; i++) {
      w[i] = view.getUint32(offset + i * 4)
    }
    for (let i = 16; i < 64; i++) {
      const s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >>> 3)
      const s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >>> 10)
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0
    }
    let a = state[0]
    let b = state[1]
    let c = state[2]
    let d = state[3]
    let e = state[4]
    let f = state[5]
    let g = state[6]
    let h = state[7]
    for (let i = 0; i < 64; i++) {
      const t1 = (h + (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) + ((e & f) ^ (~e & g)) + SHA256_K[i] + w[i]) >>> 0
      const t2 = ((rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) >>> 0
      h = g
      g = f
      f = e
      e = (d + t1) >>> 0
      d = c
      c = b
      b = a
      a = (t1 + t2) >>> 0
    }
    state[0] = (state[0] + a) >>> 0
    state[1] = (state[1] + b) >>> 0
    state[2] = (state[2] + c) >>> 0
    state[3] = (state[3] + d) >>> 0
    state[4] = (state[4] + e) >>> 0
    state[5] = (state[5] + f) >>> 0
    state[6] = (state[6] + g) >>> 0
    state[7] = (state[7] + h) >>> 0
  }

  const digest = new Uint8Array(32)
  const digestView = new DataView(digest.buffer)
  for (let i = 0; i < 8; i++) {
    digestView.setUint32(i * 4, state[i])
  }
  return digest
}

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
    let read = 0
    let written = 0
    for (const codePoint of source) {
      const encoded = this.encode(codePoint)
      if (encoded.length > destination.length - written) {
        break
      }
      destination.set(encoded, written)
      read += codePoint.length
      written += encoded.length
    }
    return { read, written }
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
