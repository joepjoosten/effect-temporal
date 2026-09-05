import * as Fs from "node:fs"
import * as Path from "node:path"
import process from "node:process"
import { fileURLToPath, URL } from "node:url"
import ts from "typescript"

const root = fileURLToPath(new URL("../", import.meta.url))
const readme = Fs.readFileSync(Path.join(root, "README.md"), "utf8")
const quickStart = readme.split("## Quick start\n")[1]?.split("## Samples\n")[0]
const snippets = [...(quickStart ?? "").matchAll(/```ts\n([\s\S]*?)```/g)]
const files = ["workflows/definitions.ts", "workflows/bundle.ts", "worker.ts", "client.ts"]
if (snippets.length !== files.length) {
  throw new Error("Expected four TypeScript quick-start snippets in README.md")
}

const directory = Fs.mkdtempSync(Path.join(root, ".readme-check-"))
try {
  const roots = files.map((file, index) => {
    const target = Path.join(directory, file)
    Fs.mkdirSync(Path.dirname(target), { recursive: true })
    Fs.writeFileSync(target, snippets[index][1])
    return target
  })
  const config = ts.readConfigFile(Path.join(root, "tsconfig.base.json"), ts.sys.readFile)
  if (config.error) throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, "\n"))
  const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, root, {
    composite: false,
    incremental: false,
    noEmit: true,
    types: ["node"]
  })
  const program = ts.createProgram(roots, parsed.options)
  const diagnostics = [...parsed.errors, ...ts.getPreEmitDiagnostics(program)]
  if (diagnostics.length > 0) {
    process.stderr.write(ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCanonicalFileName: (file) => file,
      getCurrentDirectory: () => root,
      getNewLine: () => "\n"
    }))
    process.exitCode = 1
  } else {
    process.stdout.write("README quick-start snippets typecheck successfully\n")
  }
} finally {
  Fs.rmSync(directory, { recursive: true, force: true })
}
