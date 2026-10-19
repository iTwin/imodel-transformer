/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  ElementRefersToElements,
  IModelDb,
} from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import {
  PreparedDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
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

async function countRelationships(db: IModelDb): Promise<number> {
  const reader = db.createQueryReader(
    `SELECT count(*) cnt FROM ${ElementRefersToElements.classFullName}`,
    undefined,
    { usePrimaryConn: true }
  );
  if (!(await reader.step()))
    throw new Error("Relationship count query returned no row");
  return reader.current.cnt as number;
}

/**
 * Re-runs a full transformation into a target that an untimed full transformation already
 * populated, so every exported relationship already exists in the target. Both runs record
 * provenance so the re-run maps source elements without FederationGuids onto the existing
 * target elements instead of inserting duplicates.
 */
export function standaloneFullRetransformation(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { sourceDb, targetDb } = requireStandaloneDataset(dataset);
  const editTxn = new EditTxn(
    targetDb,
    "Quick standalone full retransformation"
  );
  editTxn.start();
  const createTransformer = () =>
    new IModelTransformer(
      { source: sourceDb, target: editTxn },
      { loadSourceGeometry: true }
    );
  const transformers = new Set<IModelTransformer>();
  let rerun: IModelTransformer | undefined;
  let populatedDigest: string | undefined;
  let disposed = false;
  const disposeTransformer = (transformer: IModelTransformer) => {
    transformers.delete(transformer);
    transformer.dispose();
  };
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    const errors: unknown[] = [];
    for (const transformer of [...transformers]) {
      try {
        disposeTransformer(transformer);
      } catch (error) {
        errors.push(error);
      }
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
        "Failed to dispose standalone full retransformation"
      );
  };
  return {
    abort: dispose,
    async prepare() {
      const initial = createTransformer();
      transformers.add(initial);
      await initial.processSchemas();
      await initial.process();
      editTxn.saveChanges("populate quick standalone retransformation target");
      disposeTransformer(initial);

      const sourceRelationships = await countRelationships(sourceDb);
      const targetRelationships = await countRelationships(targetDb);
      if (targetRelationships !== sourceRelationships)
        throw new Error(
          `Initial full transformation copied ${targetRelationships} of ${sourceRelationships} source relationships`
        );
      populatedDigest = await outputShapeDigest(targetDb, outputClassQueries);

      rerun = createTransformer();
      transformers.add(rerun);
    },
    async measure() {
      if (rerun === undefined)
        throw new Error("Standalone full retransformation was not prepared");
      await rerun.process();
    },
    async finish() {
      editTxn.saveChanges("complete quick standalone full retransformation");
      dispose();
      const digest = await outputShapeDigest(targetDb, outputClassQueries);
      // Any duplicate element, relationship, or provenance aspect changes the class counts.
      if (digest !== populatedDigest)
        throw new Error(
          `Full retransformation changed the populated target output shape: before=${populatedDigest}, after=${digest}`
        );
      return digest;
    },
  };
}

export const standaloneFullRetransformationScenario: BenchmarkScenarioDefinition =
  {
    id: "standalone-full-retransformation",
    defaultFixtureId: "relationship-heavy-transform",
    capabilities: {
      topology: "standalone-source-and-empty-target",
      requiredClaims: ["full transformation"],
    },
    factory: standaloneFullRetransformation,
  };

export const standaloneFullRetransformationBenchmark = defineBenchmark({
  scenario: standaloneFullRetransformationScenario,
  fixtures: [relationshipHeavyTransformFixture],
});
