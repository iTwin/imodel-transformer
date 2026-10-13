/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  Code,
  ElementProps,
  IModel,
  RelationshipProps,
} from "@itwin/core-common";
import {
  DefinitionModel,
  EditTxn,
  IModelDb,
  InformationRecordModel,
  PhysicalModel,
  withEditTxn,
} from "@itwin/core-backend";

export async function queryRealisticBuildingCount(
  db: IModelDb,
  ecsql: string,
  fixtureLabel: string
): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(`${fixtureLabel} count query returned no row: ${ecsql}`);
  return reader.current.cnt as number;
}

interface ModelStructureOptions {
  readonly definitionModelName: string;
  readonly fixtureLabel: string;
  readonly informationModelName: string;
  readonly modelCount: number;
  readonly physicalModelName: (index: number) => string;
  readonly transactionDescription: string;
}

export async function createRealisticBuildingModelStructure(
  db: IModelDb,
  options: ModelStructureOptions
): Promise<{
  readonly definitionModelId: string;
  readonly informationModelId: string;
  readonly physicalModelIds: readonly string[];
}> {
  const initialModelCount = await queryRealisticBuildingCount(
    db,
    "SELECT count(*) cnt FROM bis.Model",
    options.fixtureLabel
  );
  const additionalModelCount = options.modelCount - initialModelCount;
  if (additionalModelCount < 3)
    throw new Error(
      `${options.fixtureLabel} needs at least three additional models, found ${additionalModelCount}`
    );

  let definitionModelId = IModel.dictionaryId;
  let informationModelId = IModel.repositoryModelId;
  const physicalModelIds: string[] = [];
  withEditTxn(db, options.transactionDescription, (txn) => {
    definitionModelId = DefinitionModel.insert(
      txn,
      IModel.rootSubjectId,
      options.definitionModelName
    );
    informationModelId = InformationRecordModel.insert(
      txn,
      IModel.rootSubjectId,
      options.informationModelName
    );
    for (let index = 0; index < additionalModelCount - 2; index++)
      physicalModelIds.push(
        PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          options.physicalModelName(index)
        )
      );
  });

  return { definitionModelId, informationModelId, physicalModelIds };
}

export async function remainingRealisticBuildingEntityCount(
  db: IModelDb,
  ecClassName: string,
  targetCount: number,
  fixtureLabel: string,
  entityLabel: string
): Promise<number> {
  const currentCount = await queryRealisticBuildingCount(
    db,
    `SELECT count(*) cnt FROM ${ecClassName}`,
    fixtureLabel
  );
  const remainingCount = targetCount - currentCount;
  if (remainingCount < 0)
    throw new Error(
      `${fixtureLabel} ${entityLabel} exceed target by ${-remainingCount}`
    );
  return remainingCount;
}

export function deterministicRealisticBuildingFederationGuid(
  prefix: string,
  seed: number,
  index: number
): string {
  const suffix = (BigInt(seed) * 1_000_003n + BigInt(index + 1))
    .toString(16)
    .padStart(12, "0")
    .slice(-12);
  return `${prefix}-0000-4000-8000-${suffix}`;
}

interface SyntheticDefinitionOptions {
  readonly classFullNames: readonly string[];
  readonly count: number;
  readonly definitionIds: string[];
  readonly federationGuid: (index: number) => string;
  readonly modelId: string;
  readonly token: (index: number) => string;
  readonly transactionDescription: string;
  readonly userLabel: (index: number) => string;
}

export function insertRealisticBuildingSyntheticDefinitions(
  db: IModelDb,
  options: SyntheticDefinitionOptions
): void {
  withEditTxn(db, options.transactionDescription, (txn) => {
    for (let index = 0; index < options.count; index++) {
      const props: ElementProps & { ordinal: number; token: string } = {
        classFullName:
          options.classFullNames[index % options.classFullNames.length],
        model: options.modelId,
        code: Code.createEmpty(),
        federationGuid: options.federationGuid(index),
        userLabel: options.userLabel(index),
        ordinal: index,
        token: options.token(index),
      };
      options.definitionIds.push(txn.insertElement(props));
    }
  });
}

interface InformationRecordOptions {
  readonly classFullName: string;
  readonly count: number;
  readonly federationGuid: (index: number) => string;
  readonly modelId: string;
  readonly token: (index: number) => string;
  readonly transactionDescription: string;
  readonly userLabel: (index: number) => string;
}

export function insertRealisticBuildingInformationRecords(
  db: IModelDb,
  options: InformationRecordOptions
): string[] {
  const informationIds: string[] = [];
  withEditTxn(db, options.transactionDescription, (txn) => {
    for (let index = 0; index < options.count; index++) {
      const props: ElementProps & { ordinal: number; token: string } = {
        classFullName: options.classFullName,
        model: options.modelId,
        code: Code.createEmpty(),
        federationGuid: options.federationGuid(index),
        userLabel: options.userLabel(index),
        ordinal: index,
        token: options.token(index),
      };
      informationIds.push(txn.insertElement(props));
    }
  });
  return informationIds;
}

interface RelationshipOptions {
  readonly definitionIds: readonly string[];
  readonly driveClassFullName: string;
  readonly driveCount: number;
  readonly driveTargetOffset: number;
  readonly driveTargetStride: number;
  readonly elementIds: readonly string[];
  readonly physicalIds: readonly string[];
  readonly referenceClassFullName: string;
  readonly referenceCount: number;
  readonly referenceOffsetStride: number;
  readonly seed: number;
}

export function insertRealisticBuildingRelationships(
  txn: EditTxn,
  options: RelationshipOptions
): void {
  for (let index = 0; index < options.referenceCount; index++) {
    const sourceIndex = index % options.elementIds.length;
    const offset =
      ((index * options.referenceOffsetStride + options.seed) %
        (options.elementIds.length - 1)) +
      1;
    const props: RelationshipProps & { ordinal: number } = {
      classFullName: options.referenceClassFullName,
      sourceId: options.elementIds[sourceIndex],
      targetId:
        options.elementIds[(sourceIndex + offset) % options.elementIds.length],
      ordinal: index,
    };
    txn.insertRelationship(props);
  }

  for (let index = 0; index < options.driveCount; index++) {
    const props: RelationshipProps & {
      priority: number;
      status: number;
    } = {
      classFullName: options.driveClassFullName,
      sourceId: options.definitionIds[index % options.definitionIds.length],
      targetId:
        options.physicalIds[
          (index * options.driveTargetStride + options.driveTargetOffset) %
            options.physicalIds.length
        ],
      priority: index % 8,
      status: 0,
    };
    txn.insertRelationship(props);
  }
}
