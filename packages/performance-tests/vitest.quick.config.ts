/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: [
      "test/quick/tests/**/*.test.ts",
      "test/quick/QuickPerformance.test.ts",
    ],
    testTimeout: 900_000,
    hookTimeout: 900_000,
    pool: "forks",
    maxWorkers: 1,
    fileParallelism: false,
  },
});
