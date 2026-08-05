/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createRequire } from "node:module";
import * as path from "node:path";
import { EditTxn, SnapshotDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import type { SchemaProcessingStrategy } from "@itwin/imodel-transformer/schema-processing";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireDetachedDataset,
} from "../fixtures/FixtureProvider.js";
import {
  buildSchemaProcessingXml,
  schemaProcessingClassCount,
  schemaProcessingFixture,
  schemaProcessingPropertiesPerClass,
  schemaProcessingSchemaName,
  schemaProcessingTargetClassCount,
  schemaProcessingTargetVersion,
} from "../fixtures/recipes/schemaProcessing.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";

export const schemaProcessingStrategyIds = [
  "default",
  "newer-version",
  "dynamic-union",
] as const;
export type SchemaProcessingStrategyId =
  (typeof schemaProcessingStrategyIds)[number];

export function resolveSchemaProcessingStrategyId(
  value = process.env.QUICK_PERF_SCHEMA_PROCESSING_STRATEGY
): SchemaProcessingStrategyId {
  const strategy = value?.trim() || "default";
  if (
    !schemaProcessingStrategyIds.includes(
      strategy as SchemaProcessingStrategyId
    )
  )
    throw new Error(
      `Unknown schema processing strategy "${strategy}". Available strategies: ${schemaProcessingStrategyIds.join(", ")}`
    );
  return strategy as SchemaProcessingStrategyId;
}

function createStrategy(
  strategy: Exclude<SchemaProcessingStrategyId, "default">
): SchemaProcessingStrategy {
  let strategyModule: typeof import("@itwin/imodel-transformer/schema-processing");
  try {
    strategyModule = createRequire(import.meta.url)(
      "@itwin/imodel-transformer/schema-processing"
    ) as typeof import("@itwin/imodel-transformer/schema-processing");
  } catch (error) {
    throw new Error(
      `Schema processing strategy "${strategy}" requires a comparison revision that exports @itwin/imodel-transformer/schema-processing`,
      { cause: error }
    );
  }
  switch (strategy) {
    case "newer-version":
      return new strategyModule.NewerVersionSchemaImportStrategy();
    case "dynamic-union":
      return new strategyModule.DynamicSchemaUnionStrategy();
  }
}

const configuredStrategy = resolveSchemaProcessingStrategyId();

export function schemaProcessing(
  dataset: PreparedDataset,
  strategyId = configuredStrategy
): BenchmarkScenario {
  const source = requireDetachedDataset(dataset);
  const strategy =
    strategyId === "default" ? undefined : createStrategy(strategyId);
  const targetDb = SnapshotDb.createEmpty(
    path.join(source.directory, "schema-processing-target.bim"),
    { rootSubject: { name: "quick schema processing target" } }
  );
  let editTxn: EditTxn | undefined;
  let transformer: IModelTransformer | undefined;
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    transformer?.dispose();
    if (editTxn?.isActive) editTxn.end();
    targetDb.close();
    disposed = true;
  };

  return {
    abort: dispose,
    async prepare() {
      if (strategyId === "dynamic-union") {
        await targetDb.importSchemaStrings([
          buildSchemaProcessingXml(
            {
              classCount: schemaProcessingTargetClassCount,
              propertiesPerClass: schemaProcessingPropertiesPerClass,
            },
            schemaProcessingTargetVersion
          ),
        ]);
      }
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
      await transformer.processSchemas(
        strategy === undefined ? undefined : { strategy }
      );
    },
    async finish() {
      try {
        const sourceVersion = source.sourceDb.querySchemaVersion(
          schemaProcessingSchemaName
        );
        const targetVersion = targetDb.querySchemaVersion(
          schemaProcessingSchemaName
        );
        const expectedTargetVersion =
          strategyId === "dynamic-union" ? "1.0.3" : sourceVersion;
        if (targetVersion !== expectedTargetVersion)
          throw new Error(
            `Processed schema version mismatch: expected=${expectedTargetVersion}, actual=${targetVersion}`
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

export const schemaProcessingScenario: BenchmarkScenarioDefinition = {
  id: "schema-processing",
  defaultFixtureId: schemaProcessingFixture.descriptor.id,
  capabilities: {
    topology: "source-only",
    requiredClaims: ["schema processing"],
  },
  configuration: { strategy: configuredStrategy },
  factory: schemaProcessing,
};

export const schemaProcessingBenchmark = defineBenchmark({
  scenario: schemaProcessingScenario,
  fixtures: [schemaProcessingFixture],
});
