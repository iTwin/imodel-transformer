/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import {
  PreparedDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import { realisticBuildingTransformLargeFixture } from "../fixtures/recipes/realisticBuildingTransformLarge.js";
import { realisticBuildingTransformFixture } from "../fixtures/recipes/realisticBuildingTransform.js";
import { standaloneFullTransformFixture } from "../fixtures/recipes/standaloneFullTransform.js";
import { relationshipHeavyTransformFixture } from "../fixtures/recipes/relationshipHeavyTransform.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import { outputShapeDigest } from "./outputShape.js";

const outputClassQueries = {
  aspects: "bis.ElementAspect",
  elements: "bis.Element",
  models: "bis.Model",
  relationships: "bis.ElementRefersToElements",
} as const;

export function standaloneFullTransformation(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { sourceDb, targetDb } = requireStandaloneDataset(dataset);
  const editTxn = new EditTxn(targetDb, "Quick standalone full transformation");
  editTxn.start();
  const transformer = new IModelTransformer(
    { source: sourceDb, target: editTxn },
    { loadSourceGeometry: true, noProvenance: true }
  );
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    const errors: unknown[] = [];
    try {
      transformer.dispose();
    } catch (error) {
      errors.push(error);
    }
    try {
      if (editTxn.isActive) editTxn.end();
    } catch (error) {
      errors.push(error);
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1)
      throw new AggregateError(
        errors,
        "Failed to dispose standalone full transformation"
      );
  };
  return {
    abort: dispose,
    async prepare() {
      await transformer.processSchemas();
    },
    async measure() {
      await transformer.process();
    },
    async finish() {
      editTxn.saveChanges("complete quick standalone full transformation");
      dispose();
      return outputShapeDigest(targetDb, outputClassQueries);
    },
  };
}

export const standaloneFullTransformationScenario: BenchmarkScenarioDefinition =
  {
    id: "standalone-full-transformation",
    defaultFixtureId: "standalone-full-transform",
    capabilities: {
      topology: "standalone-source-and-empty-target",
      requiredClaims: ["full transformation"],
    },
    factory: standaloneFullTransformation,
  };

export const standaloneFullTransformationBenchmark = defineBenchmark({
  scenario: standaloneFullTransformationScenario,
  fixtures: [
    standaloneFullTransformFixture,
    relationshipHeavyTransformFixture,
    realisticBuildingTransformFixture,
    realisticBuildingTransformLargeFixture,
  ],
});
