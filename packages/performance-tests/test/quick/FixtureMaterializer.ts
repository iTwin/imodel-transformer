/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "path";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { DatasetDescriptor } from "./DatasetDescriptor";
import {
  createStartedEditTxn,
  disposeReconstructedHub,
  ReconstructedHub,
  reconstructHub,
} from "./LocalHubFixture";
import {
  applyBalancedChangesets,
  BalancedRecipeState,
  createBalancedSeed,
} from "./recipes/balancedIncremental";
import { assertFixtureDistribution } from "./validation/validateFixture";

export interface PreparedDataset {
  readonly descriptor: DatasetDescriptor;
  readonly hub: ReconstructedHub;
  readonly reconstructionMilliseconds: number;
}

export async function materializeFixture(
  descriptor: DatasetDescriptor,
  outputDir: string,
  sampleName: string
): Promise<PreparedDataset> {
  const start = process.hrtime.bigint();
  let recipeState: BalancedRecipeState | undefined;
  let hub: ReconstructedHub | undefined;
  try {
    hub = await reconstructHub(outputDir, sampleName, async (sourceSeed) => {
      recipeState = await createBalancedSeed(sourceSeed, descriptor);
    });
    if (!recipeState)
      throw new Error("Balanced fixture recipe did not create state");

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
    await applyBalancedChangesets(
      hub.sourceDb,
      hub.accessToken,
      descriptor,
      recipeState
    );
    await assertFixtureDistribution(hub.sourceDb, descriptor);
    const reconstructionMilliseconds =
      Number(process.hrtime.bigint() - start) / 1_000_000;
    return { descriptor, hub, reconstructionMilliseconds };
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
}

export function fixtureWorkingDirectory(
  root: string,
  sampleName: string
): string {
  return path.join(root, sampleName);
}
