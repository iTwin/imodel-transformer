/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import { EditTxn, ElementAspect, IModelDb } from "@itwin/core-backend";
import { ElementAspectProps } from "@itwin/core-common";
import {
  ChangedInstanceIds,
  IModelImporter,
  IModelTransformer,
} from "@itwin/imodel-transformer";
import {
  PreparedDataset,
  requireLiveHubDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import {
  aspectHeavyNoopFixture,
  aspectHeavyNoopLargeFixture,
  aspectHeavyProcessChangesFixture,
} from "../fixtures/recipes/aspectHeavyNoop.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import { outputShapeDigest } from "./outputShape.js";

const uniqueAspectClass = "QuickPerf:BalancedUniqueAspect";
const multiAspectClass = "QuickPerf:BalancedMultiAspect";
const outputClassQueries = {
  aspects: "bis.ElementAspect",
  elements: "bis.Element",
  models: "bis.Model",
  relationships: "bis.ElementRefersToElements",
} as const;

type Operation = "processAll" | "processChanges";

interface AspectCounters {
  inserted: number;
  updated: number;
  deleted: number;
}

class AspectCountingImporter extends IModelImporter {
  public readonly aspectCounters: AspectCounters = {
    inserted: 0,
    updated: 0,
    deleted: 0,
  };

  protected override async onInsertElementAspect(
    aspectProps: ElementAspectProps
  ): Promise<Id64String> {
    this.aspectCounters.inserted++;
    return super.onInsertElementAspect(aspectProps);
  }

  protected override async onUpdateElementAspect(
    aspectProps: ElementAspectProps
  ): Promise<void> {
    this.aspectCounters.updated++;
    await super.onUpdateElementAspect(aspectProps);
  }

  protected override async onDeleteElementAspect(
    aspect: ElementAspect
  ): Promise<void> {
    this.aspectCounters.deleted++;
    await super.onDeleteElementAspect(aspect);
  }
}

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(
      `Aspect-heavy target count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

async function queryAspectIds(db: IModelDb): Promise<Set<Id64String>> {
  const ids = new Set<Id64String>();
  const reader = db.createQueryReader(
    `SELECT ECInstanceId as id FROM ${uniqueAspectClass}
     UNION ALL
     SELECT ECInstanceId as id FROM ${multiAspectClass}`,
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step()) ids.add(reader.current.id as Id64String);
  return ids;
}

async function queryPhysicalObjectIds(db: IModelDb): Promise<Set<Id64String>> {
  const ids = new Set<Id64String>();
  const reader = db.createQueryReader(
    "SELECT ECInstanceId as id FROM Generic.PhysicalObject",
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step()) ids.add(reader.current.id as Id64String);
  return ids;
}

async function queryWorkloadModelIds(db: IModelDb): Promise<Set<Id64String>> {
  const ids = new Set<Id64String>();
  const reader = db.createQueryReader(
    "SELECT DISTINCT Model.Id as id FROM Generic.PhysicalObject",
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step()) ids.add(reader.current.id as Id64String);
  return ids;
}

function disposeTransform(
  transformer: IModelTransformer | undefined,
  editTxn: EditTxn | undefined
): void {
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
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1)
    throw new AggregateError(
      errors,
      "Failed to dispose aspect-heavy transform"
    );
}

async function runInitialTransform(
  sourceDb: IModelDb,
  targetDb: IModelDb
): Promise<void> {
  const editTxn = new EditTxn(targetDb, "Initial aspect-heavy transformation");
  editTxn.start();
  const transformer = new IModelTransformer(
    { source: sourceDb, target: editTxn },
    { noProvenance: true }
  );
  try {
    await transformer.processSchemas();
    await transformer.process();
    editTxn.saveChanges("save initial aspect-heavy transformation");
  } finally {
    disposeTransform(transformer, editTxn);
  }
}

function createAspectHeavyScenario(
  dataset: PreparedDataset,
  sourceDb: IModelDb,
  targetDb: IModelDb,
  operation: Operation
): BenchmarkScenario {
  const { elements: elementCount, aspects: aspectCount } =
    dataset.descriptor.distribution.base;
  const aspectsPerElement = aspectCount / elementCount;
  if (!Number.isSafeInteger(aspectsPerElement))
    throw new Error(
      "Aspect-heavy fixture must have an integer aspect count per element"
    );

  let editTxn: EditTxn | undefined;
  let importer: AspectCountingImporter | undefined;
  let transformer: IModelTransformer | undefined;
  let initialAspectIds: Set<Id64String> | undefined;
  let updatedElementCount = 0;
  let updatedModelCount = 0;
  let metrics: Readonly<Record<string, number | string | boolean>> | undefined;
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    disposeTransform(transformer, editTxn);
  };

  return {
    abort: dispose,
    async prepare() {
      if (operation === "processAll")
        await runInitialTransform(sourceDb, targetDb);
      initialAspectIds = await queryAspectIds(targetDb);
      if (initialAspectIds.size !== aspectCount)
        throw new Error(
          `Initial transform expected ${aspectCount} aspects, found ${initialAspectIds.size}`
        );

      let argsForProcessChanges;
      if (operation === "processChanges") {
        const changedInstanceIds = new ChangedInstanceIds(sourceDb);
        const updatedElementIds = await queryPhysicalObjectIds(sourceDb);
        if (updatedElementIds.size !== elementCount)
          throw new Error(
            `Expected ${elementCount} updated workload elements, found ${updatedElementIds.size}`
          );
        const updatedModelIds = await queryWorkloadModelIds(sourceDb);
        if (updatedModelIds.size === 0)
          throw new Error(
            "Aspect-heavy source has no model for its workload elements"
          );
        for (const id of updatedElementIds)
          changedInstanceIds.element.updateIds.add(id);
        for (const id of updatedModelIds)
          changedInstanceIds.model.updateIds.add(id);
        updatedElementCount = updatedElementIds.size;
        updatedModelCount = updatedModelIds.size;
        argsForProcessChanges = {
          changedInstanceIds,
          // Keep the target commit outside the measured processChanges call.
          saveTargetChanges: async () => {},
        };
      }

      editTxn = new EditTxn(targetDb, `Repeated aspect-heavy ${operation}`);
      editTxn.start();
      importer = new AspectCountingImporter(editTxn);
      transformer = new IModelTransformer(
        { source: sourceDb, target: importer },
        {
          noProvenance: true,
          ...(argsForProcessChanges === undefined
            ? {}
            : { argsForProcessChanges }),
        }
      );
    },
    async measure() {
      if (transformer === undefined)
        throw new Error("Aspect-heavy transformer was not prepared");
      await transformer.process();
    },
    async finish() {
      if (
        initialAspectIds === undefined ||
        importer === undefined ||
        editTxn === undefined
      )
        throw new Error("Aspect-heavy benchmark was not prepared");
      editTxn.saveChanges(`save repeated aspect-heavy ${operation}`);
      const finalAspectIds = await queryAspectIds(targetDb);
      const expectedUnique = elementCount;
      const expectedMulti = aspectCount - expectedUnique;
      const actualUnique = await queryCount(
        targetDb,
        `SELECT count(*) cnt FROM ${uniqueAspectClass}`
      );
      const actualMulti = await queryCount(
        targetDb,
        `SELECT count(*) cnt FROM ${multiAspectClass}`
      );
      if (actualUnique !== expectedUnique || actualMulti !== expectedMulti)
        throw new Error(
          `Repeated transform aspect count mismatch: expected unique=${expectedUnique}, multi=${expectedMulti}; found unique=${actualUnique}, multi=${actualMulti}`
        );

      let replacedAspectIds = 0;
      for (const id of initialAspectIds)
        if (!finalAspectIds.has(id)) replacedAspectIds++;
      let addedAspectIds = 0;
      for (const id of finalAspectIds)
        if (!initialAspectIds.has(id)) addedAspectIds++;
      metrics = {
        operation,
        elementCount,
        aspectsPerElement,
        updatedElementCount,
        updatedModelCount,
        numElementAspectsInserted: importer.aspectCounters.inserted,
        numElementAspectsUpdated: importer.aspectCounters.updated,
        numElementAspectsDeleted: importer.aspectCounters.deleted,
        targetAspectIdsBefore: initialAspectIds.size,
        targetAspectIdsAfter: finalAspectIds.size,
        targetAspectIdsReplaced: replacedAspectIds,
        targetAspectIdsAdded: addedAspectIds,
        targetAspectIdsChanged: replacedAspectIds > 0 || addedAspectIds > 0,
      };
      const digest = await outputShapeDigest(targetDb, outputClassQueries);
      dispose();
      return digest;
    },
    getMetrics() {
      if (metrics === undefined)
        throw new Error(
          "Aspect-heavy benchmark metrics are not available before finish"
        );
      return metrics;
    },
  };
}

export function aspectHeavyNoop(dataset: PreparedDataset): BenchmarkScenario {
  const { sourceDb, targetDb } = requireStandaloneDataset(dataset);
  return createAspectHeavyScenario(dataset, sourceDb, targetDb, "processAll");
}

export function aspectHeavyProcessChanges(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { hub } = requireLiveHubDataset(dataset);
  return createAspectHeavyScenario(
    dataset,
    hub.sourceDb,
    hub.targetDb,
    "processChanges"
  );
}

export const aspectHeavyNoopScenario: BenchmarkScenarioDefinition = {
  id: "aspect-heavy-noop",
  defaultFixtureId: "aspect-heavy-noop-1k-3",
  capabilities: {
    topology: "standalone-source-and-empty-target",
    requiredClaims: ["aspect-heavy repeated transformation"],
  },
  configuration: {
    initialTransform: "untimed processAll",
    measuredTransform: "second processAll with unchanged source",
  },
  factory: aspectHeavyNoop,
};

export const aspectHeavyNoopBenchmark = defineBenchmark({
  scenario: aspectHeavyNoopScenario,
  fixtures: [aspectHeavyNoopFixture, aspectHeavyNoopLargeFixture],
});

export const aspectHeavyProcessChangesScenario: BenchmarkScenarioDefinition = {
  id: "aspect-heavy-process-changes",
  defaultFixtureId: "aspect-heavy-process-changes-1k-3",
  capabilities: {
    topology: "source-and-empty-target",
    requiredClaims: ["aspect-heavy repeated transformation"],
  },
  configuration: {
    initialTransform: "fixture-provider full transform",
    measuredTransform:
      "processChanges with all workload elements and their model marked updated",
    sourceEdits: "none",
  },
  factory: aspectHeavyProcessChanges,
};

export const aspectHeavyProcessChangesBenchmark = defineBenchmark({
  scenario: aspectHeavyProcessChangesScenario,
  fixtures: [aspectHeavyProcessChangesFixture],
});
