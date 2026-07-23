/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";

export default function setupTestOutputCleanup(): () => void {
  const outputRoot = process.env.TRANSFORMER_TEST_OUTPUT_ROOT;
  if (outputRoot === undefined)
    throw new Error("TRANSFORMER_TEST_OUTPUT_ROOT was not configured");

  return () => fs.rmSync(outputRoot, { recursive: true, force: true });
}
