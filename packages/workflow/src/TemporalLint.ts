/**
 * An ESLint plugin (flat-config compatible) enforcing workflow-sandbox
 * safety for Effect workflow bundles. The rules catch the mistakes that
 * otherwise only surface as non-deterministic replay failures in production.
 *
 * Usage (flat config), applied to the files that make up workflow bundles:
 *
 * ```ts
 * import { plugin as effectTemporal } from "@effect-temporal/workflow/TemporalLint"
 *
 * export default [
 *   {
 *     files: ["src/workflows/**"],
 *     plugins: { "effect-temporal": effectTemporal },
 *     rules: effectTemporal.configs.recommended.rules
 *   }
 * ]
 * ```
 *
 * @since 1.0.0
 */

interface RuleContext {
  report(
    descriptor: { readonly node: unknown; readonly messageId: string; readonly data?: Record<string, string> }
  ): void
  readonly options: ReadonlyArray<any>
}

interface RuleModule {
  readonly meta: {
    readonly type: "problem" | "suggestion"
    readonly docs: { readonly description: string }
    readonly messages: Record<string, string>
    readonly schema: ReadonlyArray<unknown>
  }
  create(context: RuleContext): Record<string, (node: any) => void>
}

/**
 * Mutable module-scope state (`let` / `var` at the top level) breaks V8
 * context reuse and replay inside the workflow sandbox: the state persists
 * across workflow instances sharing a context and is silently reset by
 * others.
 *
 * @since 1.0.0
 * @category Rules
 */
export const noModuleLevelMutable: RuleModule = {
  meta: {
    type: "problem",
    docs: {
      description: "Disallow mutable module-scope state in workflow bundle files"
    },
    messages: {
      mutable: "Module-level `{{kind}}` binding \"{{name}}\" is mutable state that leaks across workflow "
        + "instances under reuseV8Context and breaks replay. Keep per-run state inside the workflow body."
    },
    schema: []
  },
  create(context) {
    const check = (node: any) => {
      if (node.kind !== "let" && node.kind !== "var") {
        return
      }
      for (const declarator of node.declarations) {
        context.report({
          data: {
            kind: node.kind,
            name: declarator.id.type === "Identifier" ? declarator.id.name : "(destructured)"
          },
          messageId: "mutable",
          node: declarator
        })
      }
    }
    return {
      "Program > VariableDeclaration": check,
      "Program > ExportNamedDeclaration > VariableDeclaration": check
    }
  }
}

const defaultForbiddenImports: ReadonlyArray<string> = [
  "^@temporalio/client",
  "^@temporalio/worker",
  "^@temporalio/testing",
  "^@effect-temporal/client",
  "^@effect-temporal/testing",
  "TemporalWorkflowInteractions(\\.js)?$",
  "TemporalWorkflowEngine(\\.js)?$",
  "TemporalWorker(\\.js)?$",
  "^node:"
]

/**
 * Workflow bundle modules must not import client- or worker-side modules
 * (and vice versa): the bundle runs inside the deterministic sandbox where
 * Node APIs and gRPC clients do not exist, and mixing the halves drags them
 * into the bundle.
 *
 * @since 1.0.0
 * @category Rules
 */
export const noMixedHalves: RuleModule = {
  meta: {
    type: "problem",
    docs: {
      description: "Disallow importing client/worker-side modules from workflow bundle files"
    },
    messages: {
      mixed: "\"{{source}}\" is a client/worker-side module and must not be imported from a workflow "
        + "bundle file: the sandbox has no Node or gRPC runtime, and the import drags one into the bundle."
    },
    schema: [
      {
        additionalProperties: false,
        properties: {
          forbidden: { items: { type: "string" }, type: "array" }
        },
        type: "object"
      }
    ]
  },
  create(context) {
    const patterns = ((context.options[0]?.forbidden as ReadonlyArray<string> | undefined) ?? defaultForbiddenImports)
      .map((pattern) => new RegExp(pattern))
    return {
      ImportDeclaration(node: any) {
        const source = node.source.value as string
        if (patterns.some((pattern) => pattern.test(source))) {
          context.report({ data: { source }, messageId: "mixed", node })
        }
      }
    }
  }
}

/**
 * Direct `proxyActivities` calls bypass schema validation and the typed
 * failure channel; `TemporalTypedActivity` keeps both sides of the activity
 * boundary on one schema'd definition.
 *
 * @since 1.0.0
 * @category Rules
 */
export const preferTypedActivity: RuleModule = {
  meta: {
    type: "suggestion",
    docs: {
      description: "Prefer TemporalTypedActivity over direct proxyActivities usage"
    },
    messages: {
      prefer: "Prefer TemporalTypedActivity.make + call over {{callee}}: typed activities validate "
        + "payloads and land schema'd failures in the Effect error channel."
    },
    schema: []
  },
  create(context) {
    return {
      CallExpression(node: any) {
        const callee = node.callee
        const name = callee.type === "Identifier"
          ? callee.name
          : callee.type === "MemberExpression" && callee.property.type === "Identifier"
          ? callee.property.name
          : undefined
        if (name === "proxyActivities" || name === "proxyLocalActivities") {
          context.report({ data: { callee: name }, messageId: "prefer", node })
        }
      }
    }
  }
}

/**
 * `Effect.promise` / `Effect.tryPromise` thunks must take no parameters — a
 * parameterized function passed by reference is almost always a mistake that
 * captures non-deterministic behavior or never receives its arguments.
 *
 * @since 1.0.0
 * @category Rules
 */
export const zeroArityEffectPromise: RuleModule = {
  meta: {
    type: "problem",
    docs: {
      description: "Require zero-arity thunks for Effect.promise and Effect.tryPromise"
    },
    messages: {
      arity: "The thunk passed to Effect.{{method}} must take no parameters; it is invoked with none, "
        + "so declared parameters stay undefined."
    },
    schema: []
  },
  create(context) {
    return {
      CallExpression(node: any) {
        const callee = node.callee
        if (
          callee.type !== "MemberExpression"
          || callee.object.type !== "Identifier"
          || callee.object.name !== "Effect"
          || callee.property.type !== "Identifier"
          || (callee.property.name !== "promise" && callee.property.name !== "tryPromise")
        ) {
          return
        }
        const argument = node.arguments[0]
        const thunk = argument === undefined
          ? undefined
          : argument.type === "ArrowFunctionExpression" || argument.type === "FunctionExpression"
          ? argument
          : argument.type === "ObjectExpression"
          ? argument.properties.find((property: any) => property.type === "Property" && property.key?.name === "try")
            ?.value
          : undefined
        if (
          thunk !== undefined
          && (thunk.type === "ArrowFunctionExpression" || thunk.type === "FunctionExpression")
          && thunk.params.length > 0
        ) {
          context.report({ data: { method: callee.property.name }, messageId: "arity", node: thunk })
        }
      }
    }
  }
}

/**
 * All rules by name.
 *
 * @since 1.0.0
 * @category Plugin
 */
export const rules = {
  "no-mixed-halves": noMixedHalves,
  "no-module-level-mutable": noModuleLevelMutable,
  "prefer-typed-activity": preferTypedActivity,
  "zero-arity-effect-promise": zeroArityEffectPromise
} as const

/**
 * The ESLint plugin object, with a `recommended` flat-config preset. Apply
 * it to the files that make up workflow bundles.
 *
 * @since 1.0.0
 * @category Plugin
 */
export const plugin = {
  configs: {
    recommended: {
      rules: {
        "effect-temporal/no-mixed-halves": "error",
        "effect-temporal/no-module-level-mutable": "error",
        "effect-temporal/prefer-typed-activity": "warn",
        "effect-temporal/zero-arity-effect-promise": "error"
      }
    }
  },
  meta: {
    name: "@effect-temporal/workflow"
  },
  rules
}
