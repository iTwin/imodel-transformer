/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import { IModelHost } from "@itwin/core-backend";
import * as path from "node:path";
import { getFixtureProvider } from "../fixtures/FixtureProvider.js";
import { configureFixture } from "../fixtures/FixtureRecipe.js";
import {
  updateHeavyScanFixture,
  updateHeavyScanRecipe,
} from "../fixtures/recipes/updateHeavyScan.js";
import { assertSafeBenchmarkOutputPath } from "../framework/BenchmarkRunner.js";
import {
  ComparisonFingerprint,
  fingerprintForArtifact,
  hashFixtureArtifact,
} from "./ComparisonRunner.js";

const comparisonFixtureMarker = ".imodel-transformer-comparison-fixture";

const updateHeavyScanSmokeFixture = configureFixture(updateHeavyScanRecipe, {
  id: "update-heavy-scan-smoke",
  version: 1,
  label: "update-heavy scan (smoke only)",
  scenarioClaims: ["changeset scanning"],
  topology: "source-only",
  seed: 328,
  parameters: { changesets: 4, scale: 1 },
});

export async function prepareComparisonFixture(
  outputDirectory: string,
  smoke = false
): Promise<{
  readonly directory: string;
  readonly artifactHash: string;
  readonly fingerprint: ComparisonFingerprint;
}> {
  const fixture = smoke ? updateHeavyScanSmokeFixture : updateHeavyScanFixture;
  const { descriptor } = fixture;
  if (descriptor.layout.topology !== "source-only")
    throw new Error("Comparison fixture must produce a detached artifact");
  const provider = getFixtureProvider(descriptor);
  assertSafeBenchmarkOutputPath(outputDirectory);
  fs.mkdirSync(outputDirectory, { recursive: true });
  const marker = path.join(outputDirectory, comparisonFixtureMarker);
  if (fs.readdirSync(outputDirectory).length > 0 && !fs.existsSync(marker))
    throw new Error(
      `Refusing to replace non-empty unowned fixture directory: ${outputDirectory}`
    );
  fs.writeFileSync(marker, "Owned by quick performance comparison.\n");
  const profileName = `quick-compare-fixture-${process.pid}`;
  await IModelHost.startup({ profileName });
  const profileDir = IModelHost.profileDir;
  try {
    try {
      await provider.build(fixture, outputDirectory);
    } finally {
      fs.mkdirSync(outputDirectory, { recursive: true });
      fs.writeFileSync(marker, "Owned by quick performance comparison.\n");
    }
  } finally {
    await IModelHost.shutdown();
    if (profileDir.includes(profileName))
      fs.rmSync(profileDir, { recursive: true, force: true });
  }
  return {
    directory: outputDirectory,
    artifactHash: hashFixtureArtifact(outputDirectory),
    fingerprint: fingerprintForArtifact(outputDirectory),
  };
}
