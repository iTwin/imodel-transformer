/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";

interface TransformerPackageMetadata {
  name: string;
  version: string;
  peerDependencies: Record<string, string>;
}

const packageRoot =
  path.basename(__dirname) === "src"
    ? path.resolve(__dirname, "..")
    : path.resolve(__dirname, "../..");

// eslint-disable-next-line @typescript-eslint/no-var-requires, @typescript-eslint/no-require-imports
export const transformerPackageMetadata = require(
  path.join(packageRoot, "package.json")
) as TransformerPackageMetadata;
