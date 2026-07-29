/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

function findPackageDirectory(from: string): string {
  let candidate = from;
  while (true) {
    const packageJson = path.join(candidate, "package.json");
    if (fs.existsSync(packageJson)) {
      const parsed = JSON.parse(fs.readFileSync(packageJson, "utf8")) as {
        name?: string;
      };
      if (parsed.name === "transformer-performance-tests") return candidate;
    }
    const parent = path.dirname(candidate);
    if (parent === candidate)
      throw new Error("Cannot locate transformer-performance-tests package");
    candidate = parent;
  }
}

export const quickSourceDirectory = path.join(
  findPackageDirectory(path.dirname(fileURLToPath(import.meta.url))),
  "test",
  "quick"
);

export function quickSourcePath(...segments: string[]): string {
  return path.join(quickSourceDirectory, ...segments);
}
