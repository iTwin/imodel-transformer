/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import { IModelHost } from "@itwin/core-backend";
import * as path from "node:path";
import { updateHeavyScanDescriptor } from "../catalogs/FixtureCatalog.js";
import {
  canonicalSha256,
  FixtureDescriptor,
} from "../fixtures/FixtureDescriptor.js";
import { getFixtureProvider } from "../fixtures/FixtureProvider.js";
import { assertSafeBenchmarkOutputPath } from "../framework/BenchmarkRunner.js";
import {
  ComparisonFingerprint,
  fingerprintForArtifact,
  hashFixtureArtifact,
} from "./ComparisonRunner.js";

const comparisonFixtureMarker = ".imodel-transformer-comparison-fixture";

function smokeDescriptor(): FixtureDescriptor {
  const divide = (value: number): number => Math.max(1, Math.round(value / 16));
  const distribution = updateHeavyScanDescriptor.distribution;
  const reduced = {
    base: {
      aspects: divide(distribution.base.aspects),
      elements: divide(distribution.base.elements),
      geometricElements: distribution.base.geometricElements,
      relationships: divide(distribution.base.relationships),
    },
    operations: {
      elements: {
        inserts: divide(distribution.operations.elements.inserts),
        updates: divide(distribution.operations.elements.updates),
        deletes: divide(distribution.operations.elements.deletes),
      },
      aspects: {
        inserts: divide(distribution.operations.aspects.inserts),
        updates: divide(distribution.operations.aspects.updates),
        deletes: divide(distribution.operations.aspects.deletes),
      },
      relationships: {
        inserts: divide(distribution.operations.relationships.inserts),
        updates: divide(distribution.operations.relationships.updates),
        deletes: divide(distribution.operations.relationships.deletes),
      },
      geometryUpdates: distribution.operations.geometryUpdates,
      sourceChangesets: distribution.operations.sourceChangesets,
    },
  };
  return {
    ...updateHeavyScanDescriptor,
    id: `${updateHeavyScanDescriptor.id}-smoke`,
    label: `${updateHeavyScanDescriptor.label} (smoke only)`,
    distribution: reduced,
    recipeHash: canonicalSha256({
      purpose: "local-smoke-not-calibration",
      sourceRecipeHash: updateHeavyScanDescriptor.recipeHash,
      distribution: reduced,
    }),
  };
}

export async function prepareComparisonFixture(
  outputDirectory: string,
  smoke = false
): Promise<{
  readonly directory: string;
  readonly artifactHash: string;
  readonly fingerprint: ComparisonFingerprint;
}> {
  const descriptor = smoke ? smokeDescriptor() : updateHeavyScanDescriptor;
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
      await provider.build(descriptor, outputDirectory);
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
