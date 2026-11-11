import { spawnSync } from "node:child_process"
import * as Fs from "node:fs"
import * as Path from "node:path"
import process from "node:process"
import { fileURLToPath, URL } from "node:url"

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
  files.forEach((file, index) => {
    const target = Path.join(directory, file)
    Fs.mkdirSync(Path.dirname(target), { recursive: true })
    Fs.writeFileSync(target, snippets[index][1])
  })
  Fs.writeFileSync(
    Path.join(directory, "tsconfig.json"),
    JSON.stringify({
      extends: "../tsconfig.base.json",
      include: files,
      compilerOptions: {
        composite: false,
        incremental: false,
        noEmit: true,
        types: ["node"]
      }
    })
  )
  const result = spawnSync(Path.join(root, "node_modules/.bin/tsc"), ["-p", Path.join(directory, "tsconfig.json"), "--pretty"], {
    cwd: root,
    stdio: "inherit"
  })
  if (result.error) throw result.error
  if (result.status !== 0) {
    process.exitCode = 1
  } else {
    process.stdout.write("README quick-start snippets typecheck successfully\n")
  }
} finally {
  Fs.rmSync(directory, { recursive: true, force: true })
}
