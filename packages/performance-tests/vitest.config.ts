/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["test/TransformerRegression.test.ts", "test/unit/**/*.test.ts"],
    setupFiles: ["./test/setup.ts"],
    // Transformations and worker-owned startup/teardown may legitimately run for hours.
    testTimeout: 0,
    hookTimeout: 0,
    pool: "forks",
    maxWorkers: 1,
    fileParallelism: false,
  },
});
