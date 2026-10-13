/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import { Code, IModel, PhysicalElementProps } from "@itwin/core-common";
import {
  BriefcaseDb,
  ElementOwnsChildElements,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import {
  configureFixture,
  defineFixtureRecipe,
  FixtureRecipeContext,
} from "../FixtureRecipe.js";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { quickPath } from "../../support/paths.js";
import { queryCount } from "../validation/validateFixture.js";

/**
 * Sparse changes beneath unchanged parents.
 *
 * The base is one `PhysicalModel` of label-free, non-geometric `PhysicalObject` assemblies. Each assembly has
 * `subAssembliesPerAssembly` sub-assemblies, and each sub-assembly has `partsPerSubAssembly` parts. The changesets
 * update one existing part in some assemblies and insert one new part in others, so every changed element sits
 * beneath two unchanged ancestors and no two changed elements share an ancestor. Half of the assemblies stay
 * unchanged. Change processing must reach each changed element through its unchanged ancestors, so per-ancestor
 * work, such as applying the export filter to each ancestor, stands out.
 *
 * As in the large-base recipe, only changed elements are labeled (`inserted-hierarchy-<n>` and
 * `updated-hierarchy-<n>`), which keeps per-sample semantic validation proportional to the delta.
 */
export interface HierarchicalIncrementalParameters {
  readonly scale: number;
}

interface HierarchicalWorkload {
  readonly assemblies: number;
  readonly baseElements: number;
  readonly elementInserts: number;
  readonly elementUpdates: number;
}

const assembliesPerScale = 40;
const subAssembliesPerAssembly = 2;
const partsPerSubAssembly = 4;
const elementsPerAssembly =
  1 + subAssembliesPerAssembly * (1 + partsPerSubAssembly);
const elementInsertsPerScale = 10;
const elementUpdatesPerScale = 10;
/** Inserts land in one changeset and updates in a second one. */
const hierarchicalSourceChangesets = 2;

function hierarchicalWorkload(
  parameters: Readonly<HierarchicalIncrementalParameters>
): HierarchicalWorkload {
  const { scale } = parameters;
  if (!Number.isInteger(scale) || scale < 1)
    throw new Error("Hierarchical fixture scale must be a positive integer");
  const assemblies = assembliesPerScale * scale;
  return {
    assemblies,
    baseElements: assemblies * elementsPerAssembly,
    elementInserts: elementInsertsPerScale * scale,
    elementUpdates: elementUpdatesPerScale * scale,
  };
}

function hierarchicalDistribution(
  parameters: Readonly<HierarchicalIncrementalParameters>
): FixtureDistribution {
  const { baseElements, elementInserts, elementUpdates } =
    hierarchicalWorkload(parameters);
  return {
    base: {
      aspects: 0,
      elements: baseElements,
      geometricElements: 0,
      relationships: 0,
    },
    operations: {
      elements: {
        inserts: elementInserts,
        updates: elementUpdates,
        deletes: 0,
      },
      aspects: { inserts: 0, updates: 0, deletes: 0 },
      relationships: { inserts: 0, updates: 0, deletes: 0 },
      geometryUpdates: 0,
      sourceChangesets: hierarchicalSourceChangesets,
    },
  };
}

export interface HierarchicalRecipeState {
  readonly categoryId: Id64String;
  readonly modelId: Id64String;
  /** The first part of each sub-assembly, indexed by `assembly * subAssembliesPerAssembly + subAssembly`. */
  readonly firstPartIds: readonly Id64String[];
  /** Every sub-assembly, indexed by `assembly * subAssembliesPerAssembly + subAssembly`. */
  readonly subAssemblyIds: readonly Id64String[];
}

function elementProps(
  state: Pick<HierarchicalRecipeState, "categoryId" | "modelId">,
  codeValue: string,
  parentId?: Id64String,
  userLabel?: string
): PhysicalElementProps {
  return {
    category: state.categoryId,
    classFullName: PhysicalObject.classFullName,
    code: new Code({
      scope: IModel.rootSubjectId,
      spec: IModel.rootSubjectId,
      value: codeValue,
    }),
    model: state.modelId,
    parent:
      parentId === undefined
        ? undefined
        : new ElementOwnsChildElements(parentId),
    userLabel,
  };
}

export async function createHierarchicalSeed(
  fileName: string,
  context: FixtureRecipeContext<HierarchicalIncrementalParameters>
): Promise<HierarchicalRecipeState> {
  const { descriptor, parameters, schemaFiles } = context;
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: descriptor.id },
  });
  try {
    // The base carries no aspects, but the scenario's semantic comparison prepares
    // queries against the QuickPerf aspect classes, so the schema must exist.
    await db.importSchemas([...schemaFiles]);
    const ids = withEditTxn(
      db,
      "create hierarchical model and category",
      (txn) => ({
        modelId: PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "HierarchyModel"
        ),
        categoryId: SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "HierarchyCategory",
          {}
        ),
      })
    );
    const { assemblies } = hierarchicalWorkload(parameters);
    const subAssemblyIds: Id64String[] = [];
    const firstPartIds: Id64String[] = [];
    withEditTxn(db, "insert hierarchical base elements", (txn) => {
      for (let assembly = 0; assembly < assemblies; assembly++) {
        const assemblyId = txn.insertElement(
          elementProps(ids, `hierarchy-${assembly}`)
        );
        for (let sub = 0; sub < subAssembliesPerAssembly; sub++) {
          const subAssemblyId = txn.insertElement(
            elementProps(ids, `hierarchy-${assembly}-${sub}`, assemblyId)
          );
          subAssemblyIds.push(subAssemblyId);
          for (let part = 0; part < partsPerSubAssembly; part++) {
            const partId = txn.insertElement(
              elementProps(
                ids,
                `hierarchy-${assembly}-${sub}-${part}`,
                subAssemblyId
              )
            );
            if (part === 0) firstPartIds.push(partId);
          }
        }
      }
    });
    return { ...ids, firstPartIds, subAssemblyIds };
  } finally {
    db.close();
  }
}

export async function applyHierarchicalChangesets(
  db: BriefcaseDb,
  accessToken: string,
  context: FixtureRecipeContext<HierarchicalIncrementalParameters>,
  state: HierarchicalRecipeState
): Promise<void> {
  const { assemblies, elementInserts, elementUpdates } = hierarchicalWorkload(
    context.parameters
  );
  // Each change touches its own assembly, spread across the model so that untouched assemblies sit between them.
  const stride = Math.floor(assemblies / (elementUpdates + elementInserts));
  const lastSubAssembly = subAssembliesPerAssembly - 1;

  withEditTxn(db, "insert parts under unchanged sub-assemblies", (txn) => {
    for (let index = 0; index < elementInserts; index++) {
      const assembly = (elementUpdates + index) * stride;
      txn.insertElement(
        elementProps(
          state,
          `hierarchy-${assembly}-${lastSubAssembly}-inserted`,
          state.subAssemblyIds[
            assembly * subAssembliesPerAssembly + lastSubAssembly
          ],
          `inserted-hierarchy-${index}`
        )
      );
    }
  });
  await db.pushChanges({
    accessToken,
    description: "hierarchical delta 1: part inserts",
  });

  withEditTxn(db, "update parts under unchanged sub-assemblies", (txn) => {
    for (let index = 0; index < elementUpdates; index++) {
      const partId =
        state.firstPartIds[index * stride * subAssembliesPerAssembly];
      const props = db.elements.getElementProps<PhysicalElementProps>(partId);
      txn.updateElement({ ...props, userLabel: `updated-hierarchy-${index}` });
    }
  });
  await db.pushChanges({
    accessToken,
    description: "hierarchical delta 2: part updates",
  });
}

export async function assertHierarchicalDistribution(
  db: IModelDb,
  context: FixtureRecipeContext<HierarchicalIncrementalParameters>
): Promise<void> {
  const { distribution } = context.descriptor;
  const { inserts, updates } = distribution.operations.elements;
  const expected = {
    elements: distribution.base.elements + inserts,
    inserted: inserts,
    updated: updates,
    changedWithTwoAncestors: inserts + updates,
    touchedAssemblies: inserts + updates,
  };
  const actual = {
    elements: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject"
    ),
    inserted: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE UserLabel LIKE 'inserted-hierarchy-%'"
    ),
    updated: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE UserLabel LIKE 'updated-hierarchy-%'"
    ),
    changedWithTwoAncestors: await queryCount(
      db,
      `SELECT count(*) cnt FROM Generic.PhysicalObject part
       JOIN Generic.PhysicalObject sub ON sub.ECInstanceId=part.Parent.Id
       JOIN Generic.PhysicalObject assembly ON assembly.ECInstanceId=sub.Parent.Id
       WHERE part.UserLabel LIKE '%-hierarchy-%' AND assembly.Parent.Id IS NULL`
    ),
    touchedAssemblies: await queryCount(
      db,
      `SELECT count(DISTINCT sub.Parent.Id) cnt FROM Generic.PhysicalObject part
       JOIN Generic.PhysicalObject sub ON sub.ECInstanceId=part.Parent.Id
       WHERE part.UserLabel LIKE '%-hierarchy-%'`
    ),
  };
  if (JSON.stringify(actual) !== JSON.stringify(expected))
    throw new Error(
      `Hierarchical fixture distribution mismatch: expected=${JSON.stringify(
        expected
      )}, actual=${JSON.stringify(actual)}`
    );
  if (!db.isBriefcaseDb())
    throw new Error("Hierarchical fixture validation requires a briefcase");
  if (db.changeset.index !== distribution.operations.sourceChangesets)
    throw new Error(
      `Expected ${distribution.operations.sourceChangesets} source changesets, got ${db.changeset.index}`
    );
}

export const hierarchicalIncrementalRecipe = defineFixtureRecipe({
  id: "hierarchical-incremental",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "hierarchicalIncremental.ts"),
      quickPath("src", "fixtures", "validation", "validateFixture.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "QuickPerf.ecschema.xml")],
    values: { schema: "QuickPerf.01.00.00" },
  },
  distribution: hierarchicalDistribution,
  createSeed: createHierarchicalSeed,
  applySourceChangesets: applyHierarchicalChangesets,
  validate: assertHierarchicalDistribution,
});

/** Incremental synchronization fixture with 500 changed parts, each beneath its own two unchanged ancestors. */
export const hierarchicalIncrementalFixture = configureFixture(
  hierarchicalIncrementalRecipe,
  {
    id: "hierarchical-incremental",
    version: 1,
    label: "sparse changes beneath unchanged parents",
    scenarioClaims: ["incremental synchronization", "element hierarchy"],
    topology: "source-and-empty-target",
    seed: 438,
    parameters: { scale: 25 },
  }
);
