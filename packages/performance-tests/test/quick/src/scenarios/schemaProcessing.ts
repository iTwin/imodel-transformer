/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import { EditTxn, SnapshotDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { runWithCleanup } from "../../../Cleanup.js";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireDetachedDataset,
} from "../fixtures/FixtureProvider.js";
import {
  assertSchemaProcessingSchema,
  schemaProcessingClassCount,
  schemaProcessingFixture,
  schemaProcessingPropertiesPerClass,
  schemaProcessingSchemaName,
  schemaProcessingSourceVersionSemver,
} from "../fixtures/recipes/schemaProcessing.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";

interface SchemaProcessor {
  dispose(): void;
  processSchemas(): Promise<void>;
}

export function schemaProcessing(dataset: PreparedDataset): BenchmarkScenario {
  const source = requireDetachedDataset(dataset);
  let targetDb: SnapshotDb | undefined;
  let editTxn: EditTxn | undefined;
  let transformer: SchemaProcessor | undefined;
  let processed = false;
  let disposed = false;

  const dispose = () => {
    if (disposed) return;
    disposed = true;
    const errors: unknown[] = [];
    try {
      transformer?.dispose();
    } catch (error) {
      errors.push(error);
    }
    try {
      if (editTxn?.isActive) editTxn.end();
    } catch (error) {
      errors.push(error);
    }
    try {
      targetDb?.close();
    } catch (error) {
      errors.push(error);
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1)
      throw new AggregateError(errors, "Failed to dispose schema processing");
  };

  return {
    abort: dispose,
    async prepare() {
      targetDb = SnapshotDb.createEmpty(
        path.join(source.directory, "schema-processing-target.bim"),
        { rootSubject: { name: "quick schema processing target" } }
      );
      editTxn = new EditTxn(targetDb, "quick schema processing");
      editTxn.start();
      transformer = new IModelTransformer({
        source: source.sourceDb,
        target: editTxn,
      });
    },
    async measure() {
      if (transformer === undefined)
        throw new Error("Schema processing scenario was not prepared");
      await transformer.processSchemas();
      processed = true;
    },
    async finish() {
      return runWithCleanup(async () => {
        if (!processed || targetDb === undefined)
          throw new Error(
            "Schema processing scenario finished before measuring"
          );
        const classNames = assertSchemaProcessingSchema(
          targetDb,
          schemaProcessingSourceVersionSemver,
          schemaProcessingClassCount
        );
        return canonicalSha256({
          schemaName: schemaProcessingSchemaName,
          version: schemaProcessingSourceVersionSemver,
          classNames,
          propertiesPerClass: schemaProcessingPropertiesPerClass,
        });
      }, [{ name: "dispose schema processing scenario", run: dispose }]);
    },
  };
}

export const schemaProcessingScenario: BenchmarkScenarioDefinition = {
  id: "schema-processing",
  defaultFixtureId: schemaProcessingFixture.descriptor.id,
  capabilities: {
    topology: "source-only",
    requiredClaims: ["schema processing"],
  },
  factory: schemaProcessing,
};

export const schemaProcessingBenchmark = defineBenchmark({
  scenario: schemaProcessingScenario,
  fixtures: [schemaProcessingFixture],
});
