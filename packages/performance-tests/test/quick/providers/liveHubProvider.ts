/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelTransformer } from "@itwin/imodel-transformer";
import { DatasetDescriptor } from "../DatasetDescriptor";
import { BuiltFixture, FixtureProvider } from "../FixtureProvider";
import { getFixtureRecipe } from "../FixtureRecipe";
import { PreparedDataset, requireLiveHubDataset } from "../FixtureMaterializer";
import {
  createStartedEditTxn,
  disposeReconstructedHub,
  ReconstructedHub,
  reconstructHub,
} from "../LocalHubFixture";

/**
 * The `source-and-empty-target` topology.
 *
 * Its measured region re-enters the hub (changeset queries, provenance pushes), so it cannot be
 * captured as relocatable bytes. Stage 1 is therefore a no-op and stage 2 performs the whole
 * reconstruction — behaviour identical to the pre-two-stage runner.
 */
export const liveHubFixtureProvider: FixtureProvider = {
  async build(
    descriptor: DatasetDescriptor,
    artifactDir: string
  ): Promise<BuiltFixture> {
    getFixtureRecipe(descriptor.layout.recipe);
    return { descriptor, directory: artifactDir, buildMilliseconds: 0 };
  },

  async materialize(
    built: BuiltFixture,
    sampleDir: string,
    sampleName: string
  ): Promise<PreparedDataset> {
    const { descriptor } = built;
    const recipe = getFixtureRecipe(descriptor.layout.recipe);
    const start = process.hrtime.bigint();
    let recipeState: unknown;
    let hub: ReconstructedHub | undefined;
    try {
      hub = await reconstructHub(sampleDir, sampleName, async (sourceSeed) => {
        recipeState = await recipe.createSeed(sourceSeed, descriptor);
      });

      const editTxn = createStartedEditTxn(hub.targetDb);
      const transformer = new IModelTransformer({
        source: hub.sourceDb,
        target: editTxn,
      });
      try {
        await transformer.processSchemas();
        await transformer.process();
      } finally {
        transformer.dispose();
        if (editTxn.isActive) editTxn.end();
      }
      await hub.targetDb.pushChanges({
        accessToken: hub.accessToken,
        description: "establish quick fixture provenance",
      });
      await recipe.applySourceChangesets(
        hub.sourceDb,
        hub.accessToken,
        descriptor,
        recipeState
      );
      await recipe.validate(hub.sourceDb, descriptor);
      return {
        topology: "source-and-empty-target",
        descriptor,
        hub,
        reconstructionMilliseconds:
          Number(process.hrtime.bigint() - start) / 1_000_000,
      };
    } catch (error) {
      if (hub) {
        try {
          await disposeReconstructedHub(hub);
        } catch (cleanupError) {
          throw new AggregateError(
            [error, cleanupError],
            "Fixture materialization and cleanup both failed"
          );
        }
      }
      throw error;
    }
  },

  async disposeSample(dataset: PreparedDataset): Promise<void> {
    await disposeReconstructedHub(requireLiveHubDataset(dataset).hub);
  },

  async disposeBuild(): Promise<void> {},
};

/** Materialize directly from a descriptor, bypassing the (no-op) stage-1 build. */
export async function materializeLiveHubFixture(
  descriptor: DatasetDescriptor,
  sampleDir: string,
  sampleName: string
): Promise<PreparedDataset> {
  const built = await liveHubFixtureProvider.build(descriptor, sampleDir);
  return liveHubFixtureProvider.materialize(built, sampleDir, sampleName);
}
