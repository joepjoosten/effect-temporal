import path from "path"
import { defineConfig } from "vitest/config"

export default defineConfig({
  plugins: [],
  test: {
    projects: ["packages/*"],
    include: ["./test/**/*.test.ts"],
    globals: true
  },
  resolve: {
    alias: {
      "@template/basic/test": path.join(__dirname, "test"),
      "@template/basic": path.join(__dirname, "src")
    }
  }
})
