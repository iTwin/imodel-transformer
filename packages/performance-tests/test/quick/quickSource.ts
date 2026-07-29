/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";

/** Resolve a versioned quick-harness source asset from ts-node or compiled `lib` output. */
export function resolveQuickSourceFile(relativePath: string): string {
  const adjacent = path.join(__dirname, relativePath);
  if (fs.existsSync(adjacent)) return adjacent;
  const compiledSource = path.join(__dirname, "../../test/quick", relativePath);
  if (fs.existsSync(compiledSource)) return compiledSource;
  throw new Error(`Quick performance source input is missing: ${relativePath}`);
}
