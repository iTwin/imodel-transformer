/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "path";
import { Id64String } from "@itwin/core-bentley";
import {
  Code,
  ElementAspectProps,
  GeometryStreamBuilder,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import {
  BriefcaseDb,
  ElementGroupsMembers,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import {
  Box,
  Point3d,
  Range3d,
  YawPitchRollAngles,
} from "@itwin/core-geometry";
import { DatasetDescriptor } from "../DatasetDescriptor";

const uniqueAspectClass = "QuickPerf:BalancedUniqueAspect";
const multiAspectClass = "QuickPerf:BalancedMultiAspect";
const elementsPerUnit = 240;
const relationshipsPerUnit = 120;
const insertedElementsPerUnit = 24;

function fixtureScale(descriptor: DatasetDescriptor): number {
  const scale = descriptor.distribution.base.elements / elementsPerUnit;
  if (!Number.isInteger(scale) || scale < 1)
    throw new Error(
      `Balanced fixture elements must be a positive multiple of ${elementsPerUnit}`
    );
  return scale;
}

export function createBoxGeometry(length = 1) {
  const builder = new GeometryStreamBuilder();
  const box = Box.createRange(
    Range3d.create(
      Point3d.createZero(),
      Point3d.create(length, length, length)
    ),
    true
  );
  if (!box) throw new Error("Failed to create deterministic box geometry");
  builder.appendGeometry(box);
  return builder.geometryStream;
}

export interface BalancedRecipeState {
  readonly categoryIds: readonly Id64String[];
  readonly elementIds: readonly Id64String[];
  readonly modelIds: readonly Id64String[];
  readonly relationshipIds: readonly Id64String[];
}

function elementProps(
  modelId: Id64String,
  categoryId: Id64String,
  index: number,
  geometric: boolean
): PhysicalElementProps {
  return {
    category: categoryId,
    classFullName: PhysicalObject.classFullName,
    code: new Code({
      scope: IModel.rootSubjectId,
      spec: IModel.rootSubjectId,
      value: `quick-element-${index}`,
    }),
    federationGuid:
      index % 20 === 0
        ? `00000000-0000-4000-8000-${index.toString().padStart(12, "0")}`
        : undefined,
    geom: geometric ? createBoxGeometry() : undefined,
    model: modelId,
    placement: geometric
      ? {
          angles: YawPitchRollAngles.createDegrees(0, 0, index % 360),
          origin: Point3d.create(index, index % 17, 0),
        }
      : undefined,
    userLabel: `base-${index}`,
  };
}

function insertAspects(
  db: SnapshotDb | BriefcaseDb,
  ownerId: Id64String,
  sequence: number
): Id64String[] {
  return withEditTxn(db, "insert quick aspects", (txn) => [
    txn.insertAspect({
      classFullName: uniqueAspectClass,
      element: new ElementOwnsUniqueAspect(ownerId),
      payload: `unique-${sequence}`,
      sequence,
    } as ElementAspectProps),
    txn.insertAspect({
      classFullName: multiAspectClass,
      element: new ElementOwnsMultiAspects(ownerId),
      payload: `multi-${sequence}`,
      sequence,
    } as ElementAspectProps),
  ]);
}

export async function createBalancedSeed(
  fileName: string,
  descriptor: DatasetDescriptor
): Promise<BalancedRecipeState> {
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: descriptor.id },
  });
  try {
    await db.importSchemas([
      path.join(__dirname, "../schemas/QuickPerf.ecschema.xml"),
    ]);

    const { categoryIds, modelIds } = withEditTxn(
      db,
      "create quick models and categories",
      (txn) => ({
        modelIds: Array.from({ length: 4 }, (_, index) =>
          PhysicalModel.insert(txn, IModel.rootSubjectId, `QuickModel-${index}`)
        ),
        categoryIds: Array.from({ length: 4 }, (_, index) =>
          SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            `QuickCategory-${index}`,
            {}
          )
        ),
      })
    );

    const elementIds: Id64String[] = [];
    for (
      let index = 0;
      index < descriptor.distribution.base.elements;
      index++
    ) {
      const id = withEditTxn(db, `insert base element ${index}`, (txn) =>
        txn.insertElement(
          elementProps(
            modelIds[index % modelIds.length],
            categoryIds[index % categoryIds.length],
            index,
            index % elementsPerUnit <
              descriptor.distribution.base.geometricElements /
                fixtureScale(descriptor)
          )
        )
      );
      elementIds.push(id);
      insertAspects(db, id, index);
    }

    const relationshipIds = withEditTxn(
      db,
      "insert base relationships",
      (txn) =>
        Array.from(
          { length: descriptor.distribution.base.relationships },
          (_, index) => {
            const unit = Math.floor(index / relationshipsPerUnit);
            const unitOffset = index % relationshipsPerUnit;
            const elementIndex = unit * elementsPerUnit + unitOffset;
            const relationship = ElementGroupsMembers.create(
              db,
              elementIds[elementIndex],
              elementIds[elementIndex + 1],
              unitOffset
            );
            return txn.insertRelationship(relationship.toJSON());
          }
        )
    );
    return { categoryIds, elementIds, modelIds, relationshipIds };
  } finally {
    db.close();
  }
}

async function push(db: BriefcaseDb, accessToken: string, description: string) {
  await db.pushChanges({ accessToken, description });
}

export async function applyBalancedChangesets(
  db: BriefcaseDb,
  accessToken: string,
  descriptor: DatasetDescriptor,
  state: BalancedRecipeState
): Promise<void> {
  const scale = fixtureScale(descriptor);
  const insertedIds: Id64String[] = Array.from({
    length: descriptor.distribution.operations.elements.inserts,
  });
  for (let batch = 0; batch < 2; batch++) {
    withEditTxn(db, `insert delta element batch ${batch + 1}`, (txn) => {
      for (let unit = 0; unit < scale; unit++) {
        for (let offset = 0; offset < 12; offset++) {
          const insertedIndex =
            unit * insertedElementsPerUnit + batch * 12 + offset;
          const index = descriptor.distribution.base.elements + insertedIndex;
          const id = txn.insertElement(
            elementProps(
              state.modelIds[index % state.modelIds.length],
              state.categoryIds[index % state.categoryIds.length],
              index,
              offset % 2 === 0
            )
          );
          insertedIds[insertedIndex] = id;
          txn.insertAspect({
            classFullName: multiAspectClass,
            element: new ElementOwnsMultiAspects(id),
            payload: `inserted-aspect-${insertedIndex}`,
            sequence: insertedIndex,
          } as ElementAspectProps);
        }
      }
    });
    await push(
      db,
      accessToken,
      `quick delta ${batch + 1}: element/aspect inserts`
    );
  }

  for (let batch = 0; batch < 2; batch++) {
    withEditTxn(db, `update delta element batch ${batch + 1}`, (txn) => {
      for (let unit = 0; unit < scale; unit++) {
        for (let offset = 0; offset < 12; offset++) {
          const unitOffset = batch * 12 + offset;
          const index = unit * elementsPerUnit + unitOffset;
          const ownerId = state.elementIds[index];
          const props =
            db.elements.getElementProps<PhysicalElementProps>(ownerId);
          const geometryUpdate = unitOffset < 6;
          txn.updateElement({
            ...props,
            geom: geometryUpdate
              ? createBoxGeometry(unitOffset + 2)
              : props.geom,
            placement: geometryUpdate
              ? {
                  angles: YawPitchRollAngles.createDegrees(0, 0, unitOffset),
                  origin: Point3d.create(index + 1000, index, 0),
                }
              : props.placement,
            userLabel: `updated-${index}`,
          } as PhysicalElementProps);
          const aspect = db.elements.getAspects(ownerId, multiAspectClass)[0];
          txn.updateAspect({
            ...aspect.toJSON(),
            payload: `updated-aspect-${index}`,
          } as ElementAspectProps);
        }
      }
    });
    await push(
      db,
      accessToken,
      `quick delta ${batch + 3}: element/aspect updates`
    );
  }

  withEditTxn(db, "insert delta relationships", (txn) => {
    for (let unit = 0; unit < scale; unit++) {
      for (let index = 0; index < 12; index++)
        txn.insertRelationship(
          ElementGroupsMembers.create(
            db,
            insertedIds[unit * insertedElementsPerUnit + index],
            state.elementIds[unit * elementsPerUnit + index + 48],
            index
          ).toJSON()
        );
    }
  });
  await push(db, accessToken, "quick delta 5: relationship inserts");

  withEditTxn(db, "update delta relationships", (txn) => {
    for (let unit = 0; unit < scale; unit++) {
      for (let index = 0; index < 12; index++) {
        const relationship = db.relationships.getInstance<ElementGroupsMembers>(
          ElementGroupsMembers.classFullName,
          state.relationshipIds[unit * relationshipsPerUnit + index]
        );
        relationship.memberPriority += 1000;
        txn.updateRelationship(relationship.toJSON());
      }
    }
  });
  await push(db, accessToken, "quick delta 6: relationship updates");

  withEditTxn(db, "delete delta relationships", (txn) => {
    for (let unit = 0; unit < scale; unit++) {
      for (let index = 24; index < 36; index++) {
        const relationship = db.relationships.getInstance<ElementGroupsMembers>(
          ElementGroupsMembers.classFullName,
          state.relationshipIds[unit * relationshipsPerUnit + index]
        );
        txn.deleteRelationship(relationship.toJSON());
      }
    }
  });
  await push(db, accessToken, "quick delta 7: relationship deletes");

  withEditTxn(db, "delete delta elements and owned aspects", (txn) => {
    for (let unit = 0; unit < scale; unit++) {
      for (let index = 100; index < 124; index++)
        txn.deleteElement(state.elementIds[unit * elementsPerUnit + index]);
    }
  });
  await push(db, accessToken, "quick delta 8: element/aspect cascade deletes");
}
