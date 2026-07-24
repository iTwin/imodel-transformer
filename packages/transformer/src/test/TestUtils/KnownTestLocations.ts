/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import * as fs from "node:fs";
import { tmpdir } from "node:os";
import { ProcessDetector } from "@itwin/core-bentley";

function getTestWorkerId(): string {
  return (
    process.env.VITEST_POOL_ID ??
    process.env.VITEST_WORKER_ID ??
    String(process.pid)
  );
}

export class KnownTestLocations {
  /** The directory where test assets are stored. Keep in mind that the test is playing the role of the app. */
  public static get assetsDir(): string {
    return path.join(__dirname, "../assets");
  }

  /** Worker-local directory for generated files and native caches. */
  public static get outputDir(): string {
    if (ProcessDetector.isMobileAppBackend) {
      return path.join(tmpdir(), "../output");
    }

    // Assume that we are running in nodejs
    const runRoot =
      process.env.TRANSFORMER_TEST_OUTPUT_ROOT ??
      path.join(
        tmpdir(),
        "imodel-transformer-tests",
        `standalone-${process.pid}`
      );
    const dir = path.join(runRoot, `worker-${getTestWorkerId()}`, "output");
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }
}
