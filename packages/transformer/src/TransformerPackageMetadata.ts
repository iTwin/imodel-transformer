/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { readFileSync } from "node:fs";

interface TransformerPackageMetadata {
  name: string;
  version: string;
  peerDependencies: Record<string, string>;
}

export const transformerPackageMetadata = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8")
) as TransformerPackageMetadata;
