/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { DatasetDescriptor } from "./DatasetDescriptor";
import { canonicalSha256 } from "./FixtureManifest";

const scale = 25;
const distribution = {
  base: {
    aspects: 480 * scale,
    elements: 240 * scale,
    geometricElements: 120 * scale,
    relationships: 120 * scale,
  },
  operations: {
    elements: {
      inserts: 24 * scale,
      updates: 24 * scale,
      deletes: 24 * scale,
    },
    aspects: {
      inserts: 24 * scale,
      updates: 24 * scale,
      deletes: 48 * scale,
    },
    relationships: {
      inserts: 12 * scale,
      updates: 12 * scale,
      deletes: 33 * scale,
    },
    geometryUpdates: 6 * scale,
    sourceChangesets: 8,
  },
} as const;

function packageVersion(packageName: string): string {
  const packageJson = JSON.parse(
    fs.readFileSync(require.resolve(`${packageName}/package.json`), "utf8")
  ) as { version: string };
  return packageJson.version;
}

const generator = {
  coreBackend: packageVersion("@itwin/core-backend"),
  node: process.version,
  transformer: packageVersion("@itwin/imodel-transformer"),
};

const recipeIdentity = {
  schema: "QuickPerf.01.00.00",
  seed: 328,
  distribution,
  inputs: {
    recipe: fs.readFileSync(
      path.join(__dirname, "recipes/balancedIncremental.ts"),
      "utf8"
    ),
    schema: fs.readFileSync(
      path.join(__dirname, "schemas/QuickPerf.ecschema.xml"),
      "utf8"
    ),
    lockfile: fs.readFileSync(
      path.join(__dirname, "../../../../pnpm-lock.yaml"),
      "utf8"
    ),
  },
  versions: generator,
};

export const balancedIncrementalDescriptor: DatasetDescriptor = {
  id: "balanced-incremental",
  version: 1,
  label: "balanced incremental",
  scenarioClaims: [
    "incremental synchronization",
    "aspect lifecycle",
    "relationship lifecycle",
    "mixed scalar and geometry element changes",
  ],
  layout: {
    kind: "reconstructed",
    recipe: "balanced-incremental",
    seed: 328,
  },
  distribution,
  generator,
  recipeHash: canonicalSha256(recipeIdentity),
};

export function getFixtureDescriptor(id: string): DatasetDescriptor {
  if (id !== balancedIncrementalDescriptor.id)
    throw new Error(`Unknown quick performance fixture: ${id}`);
  return balancedIncrementalDescriptor;
}
