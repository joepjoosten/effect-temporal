import { describe, expect, it } from "@effect/vitest"
import tsParser from "@typescript-eslint/parser"
import { Linter } from "eslint"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { plugin } from "../src/TemporalLint.js"

const lint = (code: string, ruleName: string, options?: unknown): Array<Linter.LintMessage> => {
  const linter = new Linter()
  return linter.verify(code, {
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module"
    },
    plugins: { "effect-temporal": plugin as never },
    rules: {
      [`effect-temporal/${ruleName}`]: options === undefined ? "error" : ["error", options]
    } as Linter.RulesRecord
  })
}

describe("TemporalLint", () => {
  describe("no-module-level-mutable", () => {
    it("reports let and var at module scope", () => {
      expect(lint("let counter = 0", "no-module-level-mutable")).toHaveLength(1)
      expect(lint("var cache = {}", "no-module-level-mutable")).toHaveLength(1)
      expect(lint("export let total = 0", "no-module-level-mutable")).toHaveLength(1)
    })

    it("allows const at module scope and let inside functions", () => {
      expect(lint("const config = { a: 1 }", "no-module-level-mutable")).toHaveLength(0)
      expect(lint("function run() { let local = 0; return local }", "no-module-level-mutable")).toHaveLength(0)
    })
  })

  describe("no-mixed-halves", () => {
    it("reports client, worker, testing, and node imports", () => {
      for (
        const source of [
          "@temporalio/client",
          "@temporalio/worker",
          "@effect-temporal/client/TemporalWorkflowClient",
          "@effect-temporal/testing",
          "../src/TemporalWorkflowInteractions.js",
          "./TemporalWorkflowEngine.js",
          "node:fs"
        ]
      ) {
        expect(lint(`import "${source}"`, "no-mixed-halves"), source).toHaveLength(1)
      }
    })

    it("allows sandbox-safe imports", () => {
      for (
        const source of [
          "effect/Effect",
          "@temporalio/workflow",
          "@effect-temporal/workflow/TemporalDurableMailbox",
          "./definitions.js"
        ]
      ) {
        expect(lint(`import "${source}"`, "no-mixed-halves"), source).toHaveLength(0)
      }
    })

    it("supports custom forbidden patterns", () => {
      expect(
        lint("import \"my-internal-client\"", "no-mixed-halves", { forbidden: ["^my-internal-client$"] })
      ).toHaveLength(1)
    })
  })

  describe("prefer-typed-activity", () => {
    it("reports direct proxyActivities calls", () => {
      expect(lint("const a = proxyActivities({})", "prefer-typed-activity")).toHaveLength(1)
      expect(lint("const a = workflow.proxyLocalActivities({})", "prefer-typed-activity")).toHaveLength(1)
    })

    it("allows typed activity calls", () => {
      expect(lint("const a = TemporalTypedActivity.call(charge, {})", "prefer-typed-activity")).toHaveLength(0)
    })
  })

  describe("zero-arity-effect-promise", () => {
    it("reports parameterized thunks", () => {
      expect(lint("Effect.promise((signal) => fetch(url, { signal }))", "zero-arity-effect-promise")).toHaveLength(1)
      expect(
        lint("Effect.tryPromise({ try: (x) => go(x), catch: (e) => e })", "zero-arity-effect-promise")
      ).toHaveLength(1)
    })

    it("allows zero-arity thunks", () => {
      expect(lint("Effect.promise(() => sleep(10))", "zero-arity-effect-promise")).toHaveLength(0)
      expect(
        lint("Effect.tryPromise({ try: () => go(), catch: (e) => e })", "zero-arity-effect-promise")
      ).toHaveLength(0)
    })
  })

  it("passes over the sample workflow bundles with the recommended preset", () => {
    for (
      const sample of [
        "../sample/effect-workflow-example.ts",
        "../sample/order-saga/definitions.ts",
        "../sample/order-saga/workflows.ts"
      ]
    ) {
      const samplePath = fileURLToPath(new URL(sample, import.meta.url))
      const linter = new Linter()
      const messages = linter.verify(readFileSync(samplePath, "utf8"), {
        languageOptions: {
          parser: tsParser as never,
          sourceType: "module"
        },
        plugins: { "effect-temporal": plugin as never },
        rules: plugin.configs.recommended.rules as Linter.RulesRecord
      })
      expect(messages, sample).toEqual([])
    }
  })

  it("exposes a recommended preset covering every rule", () => {
    expect(Object.keys(plugin.configs.recommended.rules).sort()).toEqual([
      "effect-temporal/no-mixed-halves",
      "effect-temporal/no-module-level-mutable",
      "effect-temporal/prefer-typed-activity",
      "effect-temporal/zero-arity-effect-promise"
    ])
  })
})
