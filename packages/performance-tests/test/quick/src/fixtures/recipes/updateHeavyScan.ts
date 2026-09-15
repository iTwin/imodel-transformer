/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { AccessToken, Id64String } from "@itwin/core-bentley";
import {
  Code,
  CodeScopeSpec,
  CodeSpec,
  ElementAspectProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import {
  BriefcaseDb,
  EditTxn,
  ElementGroupsMembers,
  ElementOwnsMultiAspects,
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

const scanAspectClass = "QuickPerfScan:ScanAspect";

/**
 * Per-unit shape of the update-heavy scan recipe.
 *
 * Elements are partitioned into four regions to produce a stable update-heavy mix across the
 * scanned changesets:
 *
 * - `updated`: updated in every changeset
 * - `deletedLate`: updated, then deleted in the last changeset
 * - `insertedThenUpdated`: inserted first, updated afterwards
 * - `insertedThenDeleted`: inserted first, deleted in the last changeset
 *
 * Elements carry no geometry. An unchanged GeometryStream column never appears in a SQLite
 * changeset, so geometry would cost build time, disk and determinism while contributing nothing to
 * scan cost. This recipe is tuned as a sensitive regression detector, not as a representative
 * production workload; answering the representativeness question is a separate, geometry-bearing
 * recipe.
 */
export interface ScanRegionSizes {
  readonly updated: number;
  readonly deletedLate: number;
  readonly insertedThenUpdated: number;
  readonly insertedThenDeleted: number;
  readonly seedRelationships: number;
  readonly insertedRelationships: number;
  readonly updatedRelationships: number;
  readonly deletedRelationships: number;
}

export interface UpdateHeavyScanParameters {
  readonly changesets: number;
  readonly scale: number;
}

const perUnit: ScanRegionSizes = {
  deletedLate: 20,
  deletedRelationships: 10,
  insertedRelationships: 10,
  insertedThenDeleted: 10,
  insertedThenUpdated: 20,
  seedRelationships: 40,
  updated: 200,
  updatedRelationships: 10,
};

/** Fewer than four changesets collapses the schedule's distinct first/middle/penultimate/last phases. */
const minimumChangesets = 4;

export function scanRegionSizes(
  parameters: Readonly<UpdateHeavyScanParameters>
): ScanRegionSizes {
  const { changesets, scale } = parameters;
  if (!Number.isInteger(scale) || scale < 1)
    throw new Error("Scan fixture scale must be a positive integer");
  if (!Number.isInteger(changesets) || changesets < minimumChangesets)
    throw new Error(
      `Scan fixture needs at least ${minimumChangesets} source changesets`
    );
  return {
    deletedLate: perUnit.deletedLate * scale,
    deletedRelationships: perUnit.deletedRelationships * scale,
    insertedRelationships: perUnit.insertedRelationships * scale,
    insertedThenDeleted: perUnit.insertedThenDeleted * scale,
    insertedThenUpdated: perUnit.insertedThenUpdated * scale,
    seedRelationships: perUnit.seedRelationships * scale,
    updated: perUnit.updated * scale,
    updatedRelationships: perUnit.updatedRelationships * scale,
  };
}

function scanDistribution(
  parameters: Readonly<UpdateHeavyScanParameters>
): FixtureDistribution {
  const sizes = scanRegionSizes(parameters);
  const subsequentChangesets = parameters.changesets - 1;
  return {
    base: {
      aspects: sizes.updated + sizes.deletedLate,
      elements: sizes.updated + sizes.deletedLate,
      geometricElements: 0,
      relationships: sizes.seedRelationships,
    },
    operations: {
      elements: {
        inserts: sizes.insertedThenUpdated + sizes.insertedThenDeleted,
        updates:
          sizes.updated * parameters.changesets +
          (sizes.deletedLate + sizes.insertedThenUpdated) *
            subsequentChangesets,
        deletes: sizes.deletedLate + sizes.insertedThenDeleted,
      },
      aspects: {
        inserts: sizes.insertedThenUpdated,
        updates:
          sizes.updated * parameters.changesets +
          sizes.deletedLate * subsequentChangesets,
        deletes: sizes.deletedLate,
      },
      relationships: {
        inserts: sizes.insertedRelationships,
        updates: sizes.updatedRelationships,
        deletes: sizes.deletedRelationships,
      },
      geometryUpdates: 0,
      sourceChangesets: parameters.changesets,
    },
  };
}

export interface ScanRecipeState {
  readonly categoryId: Id64String;
  readonly modelId: Id64String;
  /** Region A: updated in every changeset. */
  readonly updatedIds: readonly Id64String[];
  /** Region B: updated throughout, then deleted in the final changeset. */
  readonly deletedLateIds: readonly Id64String[];
  /** One owned multi-aspect per region A element, in the same order. */
  readonly updatedAspectIds: readonly Id64String[];
  /** One owned multi-aspect per region B element, in the same order. */
  readonly deletedLateAspectIds: readonly Id64String[];
  readonly relationshipIds: readonly Id64String[];
}

function scanElementProps(
  modelId: Id64String,
  categoryId: Id64String,
  region: string,
  index: number,
  label: string
): PhysicalElementProps {
  return {
    category: categoryId,
    classFullName: PhysicalObject.classFullName,
    code: new Code({
      scope: IModel.rootSubjectId,
      spec: IModel.rootSubjectId,
      value: `scan-${region}-${index}`,
    }),
    model: modelId,
    userLabel: label,
  };
}

function scanAspectProps(
  ownerId: Id64String,
  sequence: number,
  payload: string,
  aspectId?: Id64String
): ElementAspectProps {
  return {
    classFullName: scanAspectClass,
    element: new ElementOwnsMultiAspects(ownerId),
    id: aspectId,
    payload,
    sequence,
  } as ElementAspectProps;
}

export async function createScanSeed(
  fileName: string,
  context: FixtureRecipeContext<UpdateHeavyScanParameters>
): Promise<ScanRecipeState> {
  const { descriptor, parameters, schemaFiles } = context;
  const sizes = scanRegionSizes(parameters);
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: descriptor.id },
  });
  try {
    await db.importSchemas([...schemaFiles]);

    const { categoryId, modelId } = withEditTxn(
      db,
      "create scan model and category",
      (txn) => ({
        categoryId: SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "ScanCategory",
          {}
        ),
        modelId: PhysicalModel.insert(txn, IModel.rootSubjectId, "ScanModel"),
      })
    );

    const insertRegion = (region: string, count: number) =>
      withEditTxn(db, `insert scan region ${region}`, (txn) => {
        const elementIds: Id64String[] = [];
        const aspectIds: Id64String[] = [];
        for (let index = 0; index < count; index++) {
          const elementId = txn.insertElement(
            scanElementProps(
              modelId,
              categoryId,
              region,
              index,
              `${region}-base-${index}`
            )
          );
          elementIds.push(elementId);
          aspectIds.push(
            txn.insertAspect(
              scanAspectProps(elementId, index, `${region}-aspect-${index}`)
            )
          );
        }
        return { aspectIds, elementIds };
      });

    const regionA = insertRegion("a", sizes.updated);
    const regionB = insertRegion("b", sizes.deletedLate);

    // Relationships live entirely within region A, so element deletion cannot alter this mix.
    const relationshipIds = withEditTxn(
      db,
      "insert scan relationships",
      (txn) =>
        Array.from({ length: sizes.seedRelationships }, (_, index) =>
          txn.insertRelationship(
            ElementGroupsMembers.create(
              db,
              regionA.elementIds[index],
              regionA.elementIds[index + 1],
              index
            ).toJSON()
          )
        )
    );

    return {
      categoryId,
      deletedLateAspectIds: regionB.aspectIds,
      deletedLateIds: regionB.elementIds,
      modelId,
      relationshipIds,
      updatedAspectIds: regionA.aspectIds,
      updatedIds: regionA.elementIds,
    };
  } finally {
    db.close();
  }
}

/**
 * Runs the deterministic changeset schedule.
 *
 * The schedule is:
 * - changeset 1:   structural inserts (regions C and D, a model, a CodeSpec, relationships)
 *                  plus the first round of region A/B updates
 * - changesets 2..n-2: region A/B/C updates
 * - changeset n-1: the same updates plus relationship updates and deletes
 * - changeset n:   region A/C updates, then region B and D element deletes
 */
export async function applyScanChangesets(
  db: BriefcaseDb,
  accessToken: AccessToken,
  context: FixtureRecipeContext<UpdateHeavyScanParameters>,
  state: ScanRecipeState
): Promise<void> {
  const { parameters } = context;
  const sizes = scanRegionSizes(parameters);
  const changesetCount = parameters.changesets;

  const insertedThenUpdatedIds: Id64String[] = [];
  const insertedThenDeletedIds: Id64String[] = [];

  const updateElements = (
    txn: EditTxn,
    ids: readonly Id64String[],
    region: string,
    changeset: number
  ) => {
    ids.forEach((id, index) =>
      txn.updateElement({
        ...scanElementProps(
          state.modelId,
          state.categoryId,
          region,
          index,
          `${region}-updated-${changeset}-${index}`
        ),
        id,
      })
    );
  };

  const updateAspects = (
    txn: EditTxn,
    ownerIds: readonly Id64String[],
    aspectIds: readonly Id64String[],
    region: string,
    changeset: number
  ) => {
    aspectIds.forEach((aspectId, index) =>
      txn.updateAspect(
        scanAspectProps(
          ownerIds[index],
          index,
          `${region}-aspect-${changeset}-${index}`,
          aspectId
        )
      )
    );
  };

  for (let changeset = 1; changeset <= changesetCount; changeset++) {
    const isFirst = changeset === 1;
    const isLast = changeset === changesetCount;
    const isPenultimate = changeset === changesetCount - 1;

    withEditTxn(db, `scan changeset ${changeset}`, (txn) => {
      if (isFirst) {
        for (let index = 0; index < sizes.insertedThenUpdated; index++) {
          const elementId = txn.insertElement(
            scanElementProps(
              state.modelId,
              state.categoryId,
              "c",
              index,
              `c-inserted-${index}`
            )
          );
          insertedThenUpdatedIds.push(elementId);
          txn.insertAspect(
            scanAspectProps(elementId, index, `c-aspect-${index}`)
          );
        }

        // Region D carries no aspects so its insert/delete traffic stays element-only.
        for (let index = 0; index < sizes.insertedThenDeleted; index++)
          insertedThenDeletedIds.push(
            txn.insertElement(
              scanElementProps(
                state.modelId,
                state.categoryId,
                "d",
                index,
                `d-inserted-${index}`
              )
            )
          );
        PhysicalModel.insert(txn, IModel.rootSubjectId, "ScanExtraModel");

        db.codeSpecs.insert(
          txn,
          CodeSpec.create(db, "ScanCodeSpec", CodeScopeSpec.Type.Repository)
        );

        for (let index = 0; index < sizes.insertedRelationships; index++)
          txn.insertRelationship(
            ElementGroupsMembers.create(
              db,
              state.updatedIds[index],
              state.updatedIds[index + sizes.seedRelationships + 1],
              index
            ).toJSON()
          );
      }

      updateElements(txn, state.updatedIds, "a", changeset);
      updateAspects(
        txn,
        state.updatedIds,
        state.updatedAspectIds,
        "a",
        changeset
      );

      if (!isLast) {
        updateElements(txn, state.deletedLateIds, "b", changeset);
        updateAspects(
          txn,
          state.deletedLateIds,
          state.deletedLateAspectIds,
          "b",
          changeset
        );
      }

      if (!isFirst) updateElements(txn, insertedThenUpdatedIds, "c", changeset);

      if (isPenultimate) {
        const updated = state.relationshipIds.slice(
          0,
          sizes.updatedRelationships
        );
        for (const relationshipId of updated) {
          const relationship =
            db.relationships.getInstance<ElementGroupsMembers>(
              ElementGroupsMembers.classFullName,
              relationshipId
            );
          relationship.memberPriority += sizes.seedRelationships;
          txn.updateRelationship(relationship.toJSON());
        }
        const deleted = state.relationshipIds.slice(
          sizes.updatedRelationships,
          sizes.updatedRelationships + sizes.deletedRelationships
        );
        for (const relationshipId of deleted)
          txn.deleteRelationship(
            db.relationships
              .getInstance<ElementGroupsMembers>(
                ElementGroupsMembers.classFullName,
                relationshipId
              )
              .toJSON()
          );
      }

      if (isLast) {
        txn.deleteElement([...state.deletedLateIds]);
        txn.deleteElement(insertedThenDeletedIds);
      }
    });

    await db.pushChanges({
      accessToken,
      description: `scan changeset ${changeset} of ${changesetCount}`,
    });
  }
}

/**
 * Assert the built source iModel matches the region structure the recipe promised.
 *
 * This checks the tip state, which is what a copied briefcase actually contains. It is deliberately
 * expressed in terms of regions rather than raw totals: a regression that deleted the wrong region
 * would still produce plausible totals, but cannot produce the right per-region counts.
 */
export async function validateScanFixture(
  db: BriefcaseDb,
  context: FixtureRecipeContext<UpdateHeavyScanParameters>
): Promise<void> {
  const { descriptor, parameters } = context;
  const sizes = scanRegionSizes(parameters);
  const queryCount = async (ecsql: string): Promise<number> => {
    const reader = db.createQueryReader(ecsql, undefined, {
      usePrimaryConn: true,
    });
    if (!(await reader.step()))
      throw new Error(`Count query returned no rows: ${ecsql}`);
    return reader.current.cnt as number;
  };
  const regionCount = async (region: string) =>
    queryCount(
      `SELECT count(*) cnt FROM ${PhysicalObject.classFullName.replace(
        ":",
        "."
      )} WHERE CodeValue LIKE 'scan-${region}-%'`
    );

  const expectations: Array<[string, number, number]> = [
    [
      "region A elements (updated throughout)",
      await regionCount("a"),
      sizes.updated,
    ],
    [
      "region B elements (deleted in the last changeset)",
      await regionCount("b"),
      0,
    ],
    [
      "region C elements (inserted then updated)",
      await regionCount("c"),
      sizes.insertedThenUpdated,
    ],
    ["region D elements (inserted then deleted)", await regionCount("d"), 0],
    [
      "scan aspects",
      await queryCount("SELECT count(*) cnt FROM QuickPerfScan.ScanAspect"),
      sizes.updated + sizes.insertedThenUpdated,
    ],
    [
      "relationships",
      await queryCount("SELECT count(*) cnt FROM BisCore.ElementGroupsMembers"),
      sizes.seedRelationships +
        sizes.insertedRelationships -
        sizes.deletedRelationships,
    ],
    [
      "updated relationships",
      await queryCount(
        `SELECT count(*) cnt FROM BisCore.ElementGroupsMembers WHERE MemberPriority >= ${sizes.seedRelationships}`
      ),
      sizes.updatedRelationships,
    ],
  ];

  const failures = expectations
    .filter(([, actual, expected]) => actual !== expected)
    .map(
      ([what, actual, expected]) =>
        `${what}: expected ${expected}, got ${actual}`
    );
  if (failures.length > 0)
    throw new Error(
      `Fixture "${descriptor.id}" does not match its recipe:\n  ${failures.join(
        "\n  "
      )}`
    );
}

export const updateHeavyScanRecipe = defineFixtureRecipe({
  id: "update-heavy-scan",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "updateHeavyScan.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "QuickPerfScan.ecschema.xml")],
    values: { schema: "QuickPerfScan.01.00.00" },
  },
  distribution: scanDistribution,
  createSeed: createScanSeed,
  applySourceChangesets: applyScanChangesets,
  validate: validateScanFixture,
});

export const updateHeavyScanFixture = configureFixture(updateHeavyScanRecipe, {
  id: "update-heavy-scan",
  version: 1,
  label: "update-heavy scan",
  scenarioClaims: ["changeset scanning"],
  topology: "source-only",
  seed: 328,
  parameters: { changesets: 20, scale: 16 },
});
