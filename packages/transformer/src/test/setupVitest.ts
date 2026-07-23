/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import { afterAll, afterEach, beforeAll, vi } from "vitest";
import { IModelHost, IModelHostOptions } from "@itwin/core-backend";
import { Logger, LogLevel, ProcessDetector } from "@itwin/core-bentley";
import { KnownTestLocations } from "./TestUtils/KnownTestLocations";
// Register custom matchers before each test file loads.
import "./TestUtils/AdvancedEqual";

export async function startTransformerTestHost(): Promise<void> {
  Logger.initializeToConsole();
  Logger.setLevelDefault(LogLevel.Error);
  const cfg: IModelHostOptions = {};
  if (ProcessDetector.isIOSAppBackend) {
    cfg.cacheDir = undefined; // Let the native side handle the cache.
  } else {
    cfg.cacheDir = path.join(KnownTestLocations.outputDir, ".cache");
  }
  await IModelHost.startup(cfg);
}

beforeAll(async () => {
  await startTransformerTestHost();
});

afterAll(async () => {
  await IModelHost.shutdown();
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});
