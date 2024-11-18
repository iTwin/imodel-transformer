/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "path";
import { tmpdir } from "os";
import { ProcessDetector } from "@itwin/core-bentley";
import { fileURLToPath } from "url";
// eslint-disable-next-line @typescript-eslint/naming-convention
const __dirname = path.dirname(fileURLToPath(import.meta.url));
export class KnownTestLocations {
  /** The directory where test assets are stored. Keep in mind that the test is playing the role of the app. */
  public static get assetsDir(): string {
    return path.join(__dirname, "../assets");
  }

  /** The directory where tests can write. */
  public static get outputDir(): string {
    if (ProcessDetector.isMobileAppBackend) {
      return path.join(tmpdir(), "../output");
    }

    // Assume that we are running in nodejs
    return path.join(__dirname, "../output");
  }
}
