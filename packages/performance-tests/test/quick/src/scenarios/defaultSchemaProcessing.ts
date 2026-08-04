/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import { EditTxn, SnapshotDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireDetachedDataset,
} from "../fixtures/FixtureProvider.js";
import {
  schemaProcessingClassCount,
  schemaProcessingFixture,
  schemaProcessingSchemaName,
} from "../fixtures/recipes/schemaProcessing.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";

export function defaultSchemaProcessing(
  dataset: PreparedDataset
): BenchmarkScenario {
  const source = requireDetachedDataset(dataset);
  const targetDb = SnapshotDb.createEmpty(
    path.join(source.directory, "schema-processing-target.bim"),
    { rootSubject: { name: "quick schema processing target" } }
  );
  const editTxn = new EditTxn(targetDb, "quick schema processing");
  editTxn.start();
  const transformer = new IModelTransformer({
    source: source.sourceDb,
    target: editTxn,
  });
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    transformer.dispose();
    if (editTxn.isActive) editTxn.end();
    targetDb.close();
    disposed = true;
  };

  return {
    abort: dispose,
    async measure() {
      await transformer.processSchemas();
    },
    async finish() {
      try {
        const sourceVersion = source.sourceDb.querySchemaVersion(
          schemaProcessingSchemaName
        );
        const targetVersion = targetDb.querySchemaVersion(
          schemaProcessingSchemaName
        );
        if (targetVersion !== sourceVersion)
          throw new Error(
            `Processed schema version mismatch: source=${sourceVersion}, target=${targetVersion}`
          );
        const classNames = Array.from(
          { length: schemaProcessingClassCount },
          (_, index) => `Entity${index}`
        );
        for (const className of classNames) {
          if (
            !targetDb.containsClass(
              `${schemaProcessingSchemaName}:${className}`
            )
          )
            throw new Error(`Processed target is missing class ${className}`);
        }
        return canonicalSha256({
          schemaName: schemaProcessingSchemaName,
          version: targetVersion,
          classNames,
        });
      } finally {
        dispose();
      }
    },
  };
}

export const defaultSchemaProcessingScenario: BenchmarkScenarioDefinition = {
  id: "default-schema-processing",
  defaultFixtureId: schemaProcessingFixture.descriptor.id,
  capabilities: {
    topology: "source-only",
    requiredClaims: ["default schema processing"],
  },
  factory: defaultSchemaProcessing,
};

export const defaultSchemaProcessingBenchmark = defineBenchmark({
  scenario: defaultSchemaProcessingScenario,
  fixtures: [schemaProcessingFixture],
});
