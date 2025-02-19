/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import path = require("path");
import { KnownTestLocations } from "../TestUtils";
import {
  DocumentListModel,
  Drawing,
  ElementGroupsMembers,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  IModelDb,
  IModelJsFs,
  SnapshotDb,
  Subject,
} from "@itwin/core-backend";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { Id64String } from "@itwin/core-bentley";
import {
  ElementProps,
  ExternalSourceAspectProps,
  IModel,
} from "@itwin/core-common";
import { ChangedInstanceIds, ChangedInstanceOps } from "../../IModelExporter";
import { expect } from "chai";

describe("ChangedInstanceIds", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "IModelTransformer"
  );

  let sourceDb: SnapshotDb;

  let documentListModel: Id64String;
  let parentDrawing: ElementProps;
  let childDrawing1: ElementProps;
  let childDrawing2: ElementProps;
  let aspect1Id: Id64String;
  let aspect2Id: Id64String;
  let relationshipId: Id64String;

  before(async () => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir)) {
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    }
    if (!IModelJsFs.existsSync(outputDir)) {
      IModelJsFs.mkdirSync(outputDir);
    }

    sourceDb = prepareSnapshotDb("ChangedInstanceIds");
    // add data to source iModel
    const sourceSubjectId = Subject.insert(
      sourceDb,
      IModel.rootSubjectId,
      "S1"
    );
    documentListModel = DocumentListModel.insert(
      sourceDb,
      sourceSubjectId,
      "DL"
    );
    parentDrawing = insertDrawingElement(
      sourceDb,
      documentListModel,
      "ParentDrawing"
    );
    childDrawing1 = insertDrawingElement(
      sourceDb,
      parentDrawing.id!,
      "ChildDrawing1"
    );
    childDrawing2 = insertDrawingElement(
      sourceDb,
      parentDrawing.id!,
      "ChildDrawing2"
    );
    const parentDrawing2 = insertDrawingElement(
      sourceDb,
      documentListModel,
      "ParentDrawing2"
    );
    insertDrawingElement(sourceDb, parentDrawing2.id!, "ChildDrawing3");
    aspect1Id = insertElementAspect(
      sourceDb,
      sourceSubjectId,
      childDrawing1.id!,
      "TestAspect1"
    );
    aspect2Id = insertElementAspect(
      sourceDb,
      sourceSubjectId,
      childDrawing2.id!,
      "TestAspect2"
    );
    relationshipId = insertElementGroupsElementsRelationship(
      sourceDb,
      childDrawing1.id!,
      childDrawing2.id!
    );
    sourceDb.saveChanges();
  });

  after(() => {
    sourceDb.close();
  });

  function prepareSnapshotDb(name: string) {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "ChangedInstanceIds",
      `${name}.bim`
    );
    return SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name },
    });
  }

  function insertDrawingElement(
    iModel: IModelDb,
    documentListModelId: Id64String,
    drawingName: string
  ): ElementProps {
    const id = Drawing.insert(iModel, documentListModelId, drawingName);
    return iModel.elements.getElementProps(id);
  }

  function insertElementAspect(
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

    return iModel.elements.insertAspect(aspectProps);
  }

  function insertElementGroupsElementsRelationship(
    iModel: IModelDb,
    sourceId: Id64String,
    targetId: Id64String
  ): Id64String {
    return ElementGroupsMembers.create(iModel, sourceId, targetId, 0).insert();
  }

  function assertHasValues(
    instanceOps: ChangedInstanceOps,
    propertyName: string,
    expectedInserted: Id64String[],
    expectedUpdated: Id64String[],
    expectedDeleted: Id64String[]
  ) {
    expect([...instanceOps.insertIds]).to.have.all.members(
      expectedInserted,
      `'${propertyName}.insertIds' contains different values than expected`
    );
    expect([...instanceOps.updateIds]).to.have.all.members(
      expectedUpdated,
      `'${propertyName}.updateIds' contains different values than expected`
    );
    expect([...instanceOps.deleteIds]).to.have.all.members(
      expectedDeleted,
      `'${propertyName}.deleteIds' contains different values than expected`
    );
  }
  describe("addCustomElementChange", async function () {
    it("should add changes for related entities when element is inserted", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomElementChange(
        "Inserted",
        childDrawing1.id!
      );

      assertHasValues(
        sourceDbChanges.element,
        "element",
        [childDrawing1.id!],
        ["0x1", documentListModel, parentDrawing.id!],
        []
      );
      assertHasValues(
        sourceDbChanges.model,
        "model",
        [],
        ["0x1", documentListModel, parentDrawing.id!],
        []
      );
      assertHasValues(sourceDbChanges.aspect, "aspect", [aspect1Id], [], []);
      assertHasValues(
        sourceDbChanges.relationship,
        "relationship",
        [relationshipId],
        [],
        []
      );
    });

    it("should add changes for related entities when element is updated", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomElementChange(
        "Updated",
        childDrawing1.id!
      );

      assertHasValues(
        sourceDbChanges.element,
        "element",
        [],
        ["0x1", documentListModel, parentDrawing.id!, childDrawing1.id!],
        []
      );
      assertHasValues(
        sourceDbChanges.model,
        "model",
        [],
        ["0x1", documentListModel, parentDrawing.id!],
        []
      );
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });

    it("should add changes for related entities when element is deleted", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomElementChange(
        "Deleted",
        childDrawing1.id!
      );

      assertHasValues(
        sourceDbChanges.element,
        "element",
        [],
        [],
        [childDrawing1.id!]
      );
      assertHasValues(sourceDbChanges.model, "model", [], [], []);
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });
  });

  describe("addCustomModelChange", async function () {
    it("should add custom changes when one model is inserted", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomModelChange("Inserted", parentDrawing.id!);
      // Act
      assertHasValues(
        sourceDbChanges.element,
        "element",
        [parentDrawing.id!],
        [documentListModel, "0x1"],
        []
      );
      assertHasValues(
        sourceDbChanges.model,
        "model",
        [parentDrawing.id!],
        [documentListModel, "0x1"],
        []
      );
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });

    it("should add custom changes when multiple models are inserted", async function () {
      // Arrange
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomModelChange("Inserted", [
        childDrawing1.id!,
        childDrawing2.id!,
      ]);

      // Act
      assertHasValues(
        sourceDbChanges.element,
        "element",
        [childDrawing1.id!, childDrawing2.id!],
        ["0x1", documentListModel, parentDrawing.id!],
        []
      );
      assertHasValues(
        sourceDbChanges.model,
        "model",
        [childDrawing1.id!, childDrawing2.id!],
        ["0x1", documentListModel, parentDrawing.id!],
        []
      );
      assertHasValues(
        sourceDbChanges.aspect,
        "aspect",
        [aspect1Id, aspect2Id],
        [],
        []
      );
      assertHasValues(
        sourceDbChanges.relationship,
        "relationship",
        [relationshipId],
        [],
        []
      );
    });
  });
});
