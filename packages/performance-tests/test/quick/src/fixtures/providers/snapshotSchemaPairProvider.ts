/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { SnapshotDb } from "@itwin/core-backend";
import { FixtureDescriptor } from "../FixtureDescriptor.js";
import {
  BuiltFixture,
  FixtureProvider,
  PreparedDataset,
  requireSnapshotSchemaPairDataset,
} from "../FixtureProvider.js";
import { getFixtureRecipe, requireSchemaPairRecipe } from "../FixtureRecipe.js";

/**
 * The `snapshot-schema-pair` topology: local source and target `SnapshotDb`s, each pre-populated
 * with a deterministic, already-divergent dynamic schema. No hub, no briefcases, no changesets —
 * the whole point is a credential-free fixture for schema-processing scenarios.
 *
 * Stage 1 is a structural no-op (there is nothing reusable to build ahead of the sample loop: the
 * schemas are cheap to generate and the databases are cheap to create). Stage 2 does the whole
 * materialization, mirroring `liveHubFixtureProvider`.
 */
export const snapshotSchemaPairFixtureProvider: FixtureProvider = {
  async build(
    descriptor: FixtureDescriptor,
    artifactDir: string
  ): Promise<BuiltFixture> {
    requireSchemaPairRecipe(getFixtureRecipe(descriptor.layout.recipe));
    return { descriptor, directory: artifactDir, buildMilliseconds: 0 };
  },

  async materialize(
    built: BuiltFixture,
    sampleDir: string,
    sampleName: string
  ): Promise<PreparedDataset> {
    const { descriptor } = built;
    const recipe = requireSchemaPairRecipe(
      getFixtureRecipe(descriptor.layout.recipe)
    );
    const start = process.hrtime.bigint();
    fs.rmSync(sampleDir, { recursive: true, force: true });
    fs.mkdirSync(sampleDir, { recursive: true });

    let sourceDb: SnapshotDb | undefined;
    let targetDb: SnapshotDb | undefined;
    try {
      const { sourceSchemaXml, targetSchemaXml, expectation } =
        await recipe.createSchemaPair(descriptor);
      sourceDb = SnapshotDb.createEmpty(path.join(sampleDir, "source.bim"), {
        rootSubject: { name: `${sampleName}-source` },
      });
      await sourceDb.importSchemaStrings([sourceSchemaXml]);

      targetDb = SnapshotDb.createEmpty(path.join(sampleDir, "target.bim"), {
        rootSubject: { name: `${sampleName}-target` },
      });
      await targetDb.importSchemaStrings([targetSchemaXml]);

      return {
        topology: "snapshot-schema-pair",
        descriptor,
        directory: sampleDir,
        sourceDb,
        targetDb,
        expectation,
        reconstructionMilliseconds:
          Number(process.hrtime.bigint() - start) / 1_000_000,
      };
    } catch (error) {
      const errors: unknown[] = [error];
      try {
        sourceDb?.close();
      } catch (closeError) {
        errors.push(closeError);
      }
      try {
        targetDb?.close();
      } catch (closeError) {
        errors.push(closeError);
      }
      try {
        fs.rmSync(sampleDir, { recursive: true, force: true });
      } catch (removeError) {
        errors.push(removeError);
      }
      if (errors.length > 1)
        throw new AggregateError(
          errors,
          "Schema-pair sample materialization and cleanup both failed"
        );
      throw error;
    }
  },

  async disposeSample(dataset: PreparedDataset): Promise<void> {
    const prepared = requireSnapshotSchemaPairDataset(dataset);
    const errors: unknown[] = [];
    try {
      prepared.sourceDb.close();
    } catch (error) {
      errors.push(error);
    }
    try {
      prepared.targetDb.close();
    } catch (error) {
      errors.push(error);
    }
    try {
      fs.rmSync(prepared.directory, { recursive: true, force: true });
    } catch (error) {
      errors.push(error);
    }
    if (errors.length > 0)
      throw new AggregateError(errors, "Failed to dispose schema-pair sample");
  },

  async disposeBuild(): Promise<void> {},
};
