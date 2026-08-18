/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "vitest";
import {
  Drawing,
  ElementGroupsMembers,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  IModelDb,
  PhysicalObject,
  withEditTxn,
} from "@itwin/core-backend";
import { GuidString, Id64, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ElementProps,
  ExternalSourceAspectProps,
  PhysicalElementProps,
} from "@itwin/core-common";
import { ChangedInstanceIds, IModelExporter } from "../../IModelExporter";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer";
import { createStartedEditTxn } from "../IModelTransformerUtils";
import { IModelTestUtils } from "./IModelTestUtils";

export class CustomChangesTransformer extends IModelTransformer {
  public readonly editTxn: ReturnType<typeof createStartedEditTxn>;

  public constructor(
    source: IModelDb,
    target: IModelDb,
    isChangeProcessing: boolean
  ) {
    const editTxn = createStartedEditTxn(target);
    const options: IModelTransformOptions = {
      includeSourceProvenance: true,
    };
    if (isChangeProcessing) options.argsForProcessChanges = {};
    const exporter = new IModelExporter(source);
    super({ source: exporter, target: editTxn }, options);
    this.editTxn = editTxn;
  }

  public override async addCustomChanges(
    _sourceDbChanges: ChangedInstanceIds
  ) {}
}

export function insertDrawingElement(
  iModel: IModelDb,
  documentListModelId: Id64String,
  drawingName: string
): ElementProps {
  const id = withEditTxn(iModel, `insert drawing ${drawingName}`, (txn) =>
    Drawing.insert(txn, documentListModelId, drawingName)
  );
  return iModel.elements.getElementProps(id);
}

export function insertPhysicalElement(
  iModel: IModelDb,
  modelId: Id64String,
  categoryId: Id64String,
  uniqueName: string
): ElementProps {
  const code = new Code({ scope: "0x1", spec: "0x1", value: uniqueName });
  const element: PhysicalElementProps = {
    classFullName: PhysicalObject.classFullName,
    model: modelId,
    category: categoryId,
    code,
    userLabel: uniqueName,
  };

  const id = withEditTxn(
    iModel,
    `insert physical element ${uniqueName}`,
    (txn) => txn.insertElement(element)
  );
  // Re-read the element to populate its federationGuid value.
  return iModel.elements.getElementProps(id);
}

export function insertElementAspect(
  iModel: IModelDb,
  scopeId: Id64String,
  elementId: Id64String,
  identifier: string
): Id64String {
  const aspectProps: ExternalSourceAspectProps = {
    classFullName: ExternalSourceAspect.classFullName,
    kind: "something",
    scope: { id: scopeId },
    element: {
      id: elementId,
      relClassName: ElementOwnsExternalSourceAspects.classFullName,
    },
    identifier,
  };

  return withEditTxn(iModel, `insert aspect ${identifier}`, (txn) =>
    txn.insertAspect(aspectProps)
  );
}

export function insertElementGroupsElementsRelationship(
  iModel: IModelDb,
  sourceId: Id64String,
  targetId: Id64String
) {
  const rel = ElementGroupsMembers.create(iModel, sourceId, targetId, 0);
  const id = withEditTxn(iModel, "insert element groups relationship", (txn) =>
    txn.insertRelationship(rel.toJSON())
  );
  return iModel.relationships.getInstance(
    ElementGroupsMembers.classFullName,
    id
  );
}

export function assertElementsExistByCode(
  iModel: IModelDb,
  properties: ElementProps[]
): void {
  properties.forEach((elemProp) => {
    expect(elemProp.code.value).to.not.be.undefined;
    expect(
      IModelTestUtils.queryByCodeValue(iModel, elemProp.code.value!),
      `Element '${elemProp.code.value}' should exist in iModel.`
    ).to.not.be.equal(Id64.invalid);
  });
}

export function assertModelExistsByName(
  iModel: IModelDb,
  names: string[]
): void {
  names.forEach((name) => {
    expect(
      IModelTestUtils.queryModelIddByModeledElementCodeValue(iModel, name),
      `Model '${name}' should exist in iModel.`
    ).to.not.be.equal(Id64.invalid);
  });
}

export function assertModelDoesNotExistByName(
  iModel: IModelDb,
  names: string[]
): void {
  names.forEach((name) => {
    expect(
      IModelTestUtils.queryModelIddByModeledElementCodeValue(iModel, name),
      `Model '${name}' should not exist in iModel.`
    ).to.be.equal(Id64.invalid);
  });
}

export function assertElementsDoNotExistByCode(
  iModel: IModelDb,
  properties: ElementProps[]
): void {
  properties.forEach((elemProp) => {
    expect(elemProp.code.value).to.not.be.undefined;
    expect(
      IModelTestUtils.queryByCodeValue(iModel, elemProp.code.value!),
      `Element '${elemProp.code.value}' should not exist in iModel.`
    ).to.be.equal(Id64.invalid);
  });
}

export function assertElementHasExpectedAspectCount(
  iModel: IModelDb,
  federationGuid: GuidString,
  expectedAspectCount: number
): void {
  const element = iModel.elements.tryGetElement(federationGuid);
  expect(
    element,
    `Could not locate element with federationGuid: ${federationGuid}`
  ).to.not.be.undefined;
  expect(iModel.elements.getAspects(element!.id).length).to.be.equal(
    expectedAspectCount,
    "Aspect count is different than expected."
  );
}
