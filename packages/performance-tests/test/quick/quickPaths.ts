/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "path";

const packageDirectory = path.resolve(__dirname, "..", "..");

export const quickSourceDirectory = path.join(
  packageDirectory,
  "test",
  "quick"
);

export function quickSourcePath(...segments: string[]): string {
  return path.join(quickSourceDirectory, ...segments);
}
