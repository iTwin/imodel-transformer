/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { defineConfig } from "vitest/config";
import * as path from "node:path";
import { tmpdir } from "node:os";

// Forks isolate process-global native state; the cap limits native memory use in CI.
const MAX_FORKS = 4;
process.env.TRANSFORMER_TEST_OUTPUT_ROOT = path.join(
  tmpdir(),
  "imodel-transformer-tests",
  `run-${process.pid}`
);

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["src/test/**/*.test.ts"],
    globalSetup: ["./src/test/globalSetupVitest.ts"],
    setupFiles: ["./src/test/setupVitest.ts"],
    // Schema imports and transformations can exceed Vitest's default timeout.
    testTimeout: 0,
    hookTimeout: 0,
    pool: "forks",
    maxWorkers: MAX_FORKS,
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary", "html"],
      include: ["src/**/*.ts"],
      exclude: ["src/test/**", "**/*.d.ts"],
      thresholds: {
        statements: 85,
        branches: 80,
        functions: 85,
        lines: 90,
      },
    },
  },
});
