/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "path";
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
import { DatasetDescriptor } from "../DatasetDescriptor";
import { ScanLedger, ScanLedgerEntry } from "../validation/scanOracle";
import { queryCount } from "../validation/validateFixture";

const scanAspectClass = "QuickPerfScan:ScanAspect";

/**
 * Per-unit shape of the update-heavy scan recipe.
 *
 * Elements are partitioned into four regions so that every id has one unambiguous expected outcome
 * after `ChangedInstanceIds` squashes the whole scanned range:
 *
 * - `updated`             updated in every changeset            -> `element.updateIds`
 * - `deletedLate`         updated, then deleted in the last     -> `element.deleteIds`
 * - `insertedThenUpdated` inserted first, updated afterwards    -> `element.insertIds`
 * - `insertedThenDeleted` inserted first, deleted in the last   -> absent from all three sets
 *
 * The last region is the reason this recipe can catch squash regressions that a "the sizes match"
 * assertion cannot: those ids must cancel out entirely.
 *
 * Note there is no region for the fourth squash rule, delete-then-insert reinstating an insert.
 * That rule is encoded in the oracle and unit-tested directly, but no region exercises it against
 * a real changeset because it is not producible here: ids are assigned by the briefcase and a
 * deleted ElementId, aspect id or relationship id is never handed out again, so a delete can never
 * be followed by an insert of the same id within one scanned range.
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

/** Seed elements are regions A and B; the other two regions are created by the changesets. */
const seedElementsPerUnit = perUnit.updated + perUnit.deletedLate;

/** Fewer than four changesets collapses the schedule's distinct first/middle/penultimate/last phases. */
const minimumChangesets = 4;

export function scanRegionSizes(
  descriptor: DatasetDescriptor
): ScanRegionSizes {
  const scale = descriptor.distribution.base.elements / seedElementsPerUnit;
  if (!Number.isInteger(scale) || scale < 1)
    throw new Error(
      `Scan fixture elements must be a positive multiple of ${seedElementsPerUnit}`
    );
  if (descriptor.distribution.operations.sourceChangesets < minimumChangesets)
    throw new Error(
      `Scan fixture needs at least ${minimumChangesets} source changesets`
    );
  return Object.fromEntries(
    Object.entries(perUnit).map(([key, value]) => [key, value * scale])
  ) as unknown as ScanRegionSizes;
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
  descriptor: DatasetDescriptor
): Promise<ScanRecipeState> {
  const sizes = scanRegionSizes(descriptor);
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: descriptor.id },
  });
  try {
    await db.importSchemas([
      path.join(__dirname, "../schemas/QuickPerfScan.ecschema.xml"),
    ]);

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

    // Relationships live entirely within region A, which is never deleted, so no relationship is
    // ever removed by an element cascade. Every relationship delete in the ledger is explicit.
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
 * Runs the changeset schedule and returns the ledger of every operation performed.
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
  descriptor: DatasetDescriptor,
  state: ScanRecipeState
): Promise<readonly ScanLedgerEntry[]> {
  const sizes = scanRegionSizes(descriptor);
  const changesetCount = descriptor.distribution.operations.sourceChangesets;
  const ledger = new ScanLedger();

  const insertedThenUpdatedIds: Id64String[] = [];
  const insertedThenUpdatedAspectIds: Id64String[] = [];
  const insertedThenDeletedIds: Id64String[] = [];
  const insertedRelationshipIds: Id64String[] = [];

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
      } as PhysicalElementProps)
    );
    ledger.record("element", "Updated", ids);
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
    ledger.record("aspect", "Updated", aspectIds);
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
          insertedThenUpdatedAspectIds.push(
            txn.insertAspect(
              scanAspectProps(elementId, index, `c-aspect-${index}`)
            )
          );
        }
        ledger.record("element", "Inserted", insertedThenUpdatedIds);
        ledger.record("aspect", "Inserted", insertedThenUpdatedAspectIds);

        // Region D carries no aspects. Whether cascade-deleting an aspect that was inserted within
        // the same scanned range emits a row at all is a second unknown, and this region already
        // exists to test one thing: that insert-then-delete cancels.
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
        ledger.record("element", "Inserted", insertedThenDeletedIds);

        // Inserting elements into a model bumps that model's row, so the model the recipe fills is
        // expected to appear as an update even though the recipe never edits the model itself.
        ledger.record("model", "Updated", state.modelId);

        // A model insert also inserts its modeled partition element under the same id, so this one
        // call is expected to show up in two collections.
        const extraModelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "ScanExtraModel"
        );
        ledger.record("model", "Inserted", extraModelId);
        ledger.record("element", "Inserted", extraModelId);
        // ...and that partition element lands in the repository model, bumping it in turn.
        ledger.record("model", "Updated", IModel.repositoryModelId);

        ledger.record(
          "codeSpec",
          "Inserted",
          db.codeSpecs.insert(
            txn,
            CodeSpec.create(db, "ScanCodeSpec", CodeScopeSpec.Type.Repository)
          )
        );

        for (let index = 0; index < sizes.insertedRelationships; index++)
          insertedRelationshipIds.push(
            txn.insertRelationship(
              ElementGroupsMembers.create(
                db,
                state.updatedIds[index],
                state.updatedIds[index + sizes.seedRelationships + 1],
                index
              ).toJSON()
            )
          );
        ledger.record("relationship", "Inserted", insertedRelationshipIds);
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
          relationship.memberPriority += 1000;
          txn.updateRelationship(relationship.toJSON());
        }
        ledger.record("relationship", "Updated", updated);

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
        ledger.record("relationship", "Deleted", deleted);
      }

      if (isLast) {
        txn.deleteElement([...state.deletedLateIds]);
        ledger.record("element", "Deleted", state.deletedLateIds);
        // Deleting an element cascades to the aspects it owns.
        ledger.record("aspect", "Deleted", state.deletedLateAspectIds);

        txn.deleteElement(insertedThenDeletedIds);
        ledger.record("element", "Deleted", insertedThenDeletedIds);
      }
    });

    await db.pushChanges({
      accessToken,
      description: `scan changeset ${changeset} of ${changesetCount}`,
    });
  }

  return ledger.entries;
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
  descriptor: DatasetDescriptor
): Promise<void> {
  const sizes = scanRegionSizes(descriptor);
  const regionCount = async (region: string) =>
    queryCount(
      db,
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
      await queryCount(db, "SELECT count(*) cnt FROM QuickPerfScan.ScanAspect"),
      sizes.updated + sizes.insertedThenUpdated,
    ],
    [
      "relationships",
      await queryCount(
        db,
        "SELECT count(*) cnt FROM BisCore.ElementGroupsMembers"
      ),
      sizes.seedRelationships +
        sizes.insertedRelationships -
        sizes.deletedRelationships,
    ],
    [
      "updated relationships",
      await queryCount(
        db,
        "SELECT count(*) cnt FROM BisCore.ElementGroupsMembers WHERE MemberPriority >= 1000"
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
