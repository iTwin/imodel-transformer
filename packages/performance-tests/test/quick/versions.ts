/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";

export interface ResolvedVersions {
  readonly coreBackend: string;
  readonly node: string;
  readonly transformer: string;
}

export function packageVersion(packageName: string): string {
  const packageJson = JSON.parse(
    fs.readFileSync(require.resolve(`${packageName}/package.json`), "utf8")
  ) as { version: string };
  return packageJson.version;
}

/**
 * Versions actually loaded by the measuring process. A fixture descriptor records the versions that
 * *generated* it, which are not necessarily the versions that measure it.
 */
export function resolvedVersions(): ResolvedVersions {
  return {
    coreBackend: packageVersion("@itwin/core-backend"),
    node: process.version,
    transformer: packageVersion("@itwin/imodel-transformer"),
  };
}
