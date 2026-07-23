import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";
import { defineConfig } from "vitest/config";

// The wasm-bindgen bundler-target module imports the `.wasm` as an ESM module;
// these plugins let Vite/Vitest resolve and instantiate it under Node.
export default defineConfig({
  plugins: [wasm(), topLevelAwait()],
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
  },
});
