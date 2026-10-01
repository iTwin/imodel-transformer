/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { EditTxn } from "@itwin/core-backend";
import { configureFixture } from "../../src/fixtures/FixtureRecipe.js";
import { FixtureTopology } from "../../src/fixtures/FixtureDescriptor.js";
import {
  getFixtureProvider,
  PreparedDataset,
  requireLiveHubDataset,
  requireStandaloneDataset,
} from "../../src/fixtures/FixtureProvider.js";
import { aspectHeavyNoopRecipe } from "../../src/fixtures/recipes/aspectHeavyNoop.js";
import { BenchmarkScenario } from "../../src/framework/BenchmarkScenario.js";
import {
  aspectHeavyNoop,
  aspectHeavyProcessChanges,
} from "../../src/scenarios/aspectHeavyNoop.js";
import {
  shutdownIsolatedHost,
  startIsolatedHost,
} from "../support/isolatedHost.js";

const elementCount = 5;
const aspectsPerElement = 3;
const aspectCount = elementCount * aspectsPerElement;

function targetDbOf(dataset: PreparedDataset) {
  return dataset.topology === "source-and-empty-target"
    ? requireLiveHubDataset(dataset).hub.targetDb
    : requireStandaloneDataset(dataset).targetDb;
}

const modes: ReadonlyArray<{
  operation: "processAll" | "processChanges";
  topology: FixtureTopology;
  factory: (dataset: PreparedDataset) => BenchmarkScenario;
  updatedElementCount: number;
  updatedModelCount: number;
}> = [
  {
    operation: "processAll",
    topology: "standalone-source-and-empty-target",
    factory: aspectHeavyNoop,
    updatedElementCount: 0,
    updatedModelCount: 0,
  },
  {
    operation: "processChanges",
    topology: "source-and-empty-target",
    factory: aspectHeavyProcessChanges,
    updatedElementCount: elementCount,
    updatedModelCount: 1,
  },
];

describe("aspect-heavy scenarios", () => {
  let root = "";

  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-aspect-heavy-"));
    await startIsolatedHost();
  });

  afterAll(async () => {
    await shutdownIsolatedHost();
    if (root !== "") fs.rmSync(root, { recursive: true, force: true });
  });

  it.each(modes)(
    "runs the $operation lifecycle and reports its aspect writes",
    async ({
      operation,
      topology,
      factory,
      updatedElementCount,
      updatedModelCount,
    }) => {
      const fixture = configureFixture(aspectHeavyNoopRecipe, {
        id: `aspect-heavy-${operation}-test`,
        version: 1,
        label: `aspect-heavy ${operation} test`,
        scenarioClaims: ["aspect-heavy repeated transformation"],
        topology,
        seed: 49979687,
        parameters: { elementCount, aspectsPerElement },
      });
      const provider = getFixtureProvider(fixture.descriptor);
      const built = await provider.build(
        fixture,
        path.join(root, `${operation}-artifact`)
      );
      const digests: string[] = [];
      try {
        for (let sample = 0; sample < 2; sample++) {
          const sampleDir = path.join(root, `${operation}-sample-${sample}`);
          const dataset = await provider.materialize(
            built,
            sampleDir,
            `${operation}-sample-${sample}`
          );
          const scenario = factory(dataset);
          try {
            await scenario.prepare?.();
            expect(() => scenario.getMetrics?.()).to.throw(
              /not available before finish/
            );
            await scenario.measure();
            digests.push(await scenario.finish());

            expect(scenario.getMetrics?.()).to.deep.equal({
              operation,
              elementCount,
              aspectsPerElement,
              updatedElementCount,
              updatedModelCount,
              // The source is unchanged, so reconciliation writes no aspects
              // and every target aspect keeps its ID.
              numElementAspectsInserted: 0,
              numElementAspectsUpdated: 0,
              numElementAspectsDeleted: 0,
              targetAspectIdsBefore: aspectCount,
              targetAspectIdsAfter: aspectCount,
              targetAspectIdsReplaced: 0,
              targetAspectIdsAdded: 0,
              targetAspectIdsChanged: false,
            });
            // finish saved the measured changes and ended its edit transaction,
            // so a new one can start; abort after finish is a no-op.
            const probe = new EditTxn(targetDbOf(dataset), "cleanup probe");
            expect(() => probe.start()).not.to.throw();
            probe.end();
            expect(() => scenario.abort()).not.to.throw();
          } finally {
            scenario.abort();
            await provider.disposeSample(dataset);
            fs.rmSync(sampleDir, { recursive: true, force: true });
          }
        }
      } finally {
        await provider.disposeBuild(built);
      }
      expect(digests).to.have.length(2);
      expect(new Set(digests).size).to.equal(1);
    }
  );
});
