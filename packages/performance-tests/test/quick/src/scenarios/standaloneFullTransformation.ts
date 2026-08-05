/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, PhysicalObject, SnapshotDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import { standaloneFullTransformFixture } from "../fixtures/recipes/standaloneFullTransform.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";

async function queryCount(db: SnapshotDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(
      `Standalone transform count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

async function physicalElementIdentity(db: SnapshotDb): Promise<unknown[]> {
  const rows: unknown[] = [];
  const reader = db.createQueryReader(
    `SELECT ECInstanceId id, ec_classname(ECClassId, 's.c') className, UserLabel userLabel
     FROM bis.PhysicalElement
     ORDER BY className, userLabel, ECInstanceId`,
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step()) {
    const element = db.elements.getElement<PhysicalObject>({
      id: reader.current.id as string,
      wantGeometry: true,
    });
    const category = db.elements.getElement(element.category);
    const model = db.models.getModel(element.model);
    const modeledElement = db.elements.getElement(model.modeledElement.id);
    const range = element.calculateRange3d();
    rows.push({
      category: {
        className: category.classFullName,
        code: category.code.value,
        label: category.userLabel,
      },
      className: reader.current.className,
      code: element.code.value,
      geometry: element.geom,
      model: {
        className: model.classFullName,
        code: modeledElement.code.value,
        label: modeledElement.userLabel,
      },
      placement: {
        angles: element.placement.angles.toJSON(),
        origin: element.placement.origin.toJSON(),
      },
      range: range.isNull
        ? undefined
        : { low: range.low.toJSON(), high: range.high.toJSON() },
      userLabel: reader.current.userLabel,
    });
  }
  return rows;
}

async function classDistribution(
  db: SnapshotDb,
  className: string
): Promise<unknown[]> {
  const rows: unknown[] = [];
  const reader = db.createQueryReader(
    `SELECT ec_classname(ECClassId, 's.c') className, count(*) cnt
     FROM ${className}
     GROUP BY ECClassId
     ORDER BY className`,
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step())
    rows.push({
      className: reader.current.className,
      count: reader.current.cnt,
    });
  return rows;
}

async function structuralIdentity(db: SnapshotDb): Promise<unknown> {
  const [aspects, elements, models, relationships] = await Promise.all([
    classDistribution(db, "bis.ElementAspect"),
    classDistribution(db, "bis.Element"),
    classDistribution(db, "bis.Model"),
    classDistribution(db, "bis.ElementRefersToElements"),
  ]);
  return { aspects, elements, models, relationships };
}

async function assertFullTransformation(
  sourceDb: SnapshotDb,
  targetDb: SnapshotDb
): Promise<string> {
  const [
    sourceElements,
    targetElements,
    sourcePhysical,
    targetPhysical,
    sourceStructure,
    targetStructure,
  ] = await Promise.all([
    queryCount(sourceDb, "SELECT count(*) cnt FROM bis.Element"),
    queryCount(targetDb, "SELECT count(*) cnt FROM bis.Element"),
    physicalElementIdentity(sourceDb),
    physicalElementIdentity(targetDb),
    structuralIdentity(sourceDb),
    structuralIdentity(targetDb),
  ]);
  if (targetElements !== sourceElements)
    throw new Error(
      `Standalone full transform element-count mismatch: source=${sourceElements}, target=${targetElements}`
    );
  if (canonicalSha256(sourcePhysical) !== canonicalSha256(targetPhysical))
    throw new Error(
      `Standalone full transform physical-element mismatch: source=${sourcePhysical.length}, target=${targetPhysical.length}`
    );
  if (canonicalSha256(sourceStructure) !== canonicalSha256(targetStructure))
    throw new Error(
      "Standalone full transform structural distribution mismatch"
    );
  return canonicalSha256({
    elementCount: targetElements,
    physicalElements: targetPhysical,
    structure: targetStructure,
  });
}

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
    transformer.dispose();
    if (editTxn.isActive) editTxn.end();
    disposed = true;
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
      return assertFullTransformation(sourceDb, targetDb);
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
  fixtures: [standaloneFullTransformFixture],
});
