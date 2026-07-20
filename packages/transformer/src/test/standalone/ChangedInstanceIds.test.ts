/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as path from "node:path";
import { KnownTestLocations } from "../TestUtils";
import {
  ChangedECInstance,
  DocumentListModel,
  Drawing,
  ElementGroupsMembers,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  IModelDb,
  IModelJsFs,
  SnapshotDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { Id64String, ITwinError } from "@itwin/core-bentley";
import {
  ElementProps,
  ExternalSourceAspectProps,
  IModel,
} from "@itwin/core-common";
import { ChangedInstanceIds, ChangedInstanceOps } from "../../IModelExporter";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";
import { assert, expect } from "chai";

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
  let parentAspect1Id: Id64String;
  let parentRelationshipId: Id64String;
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
    withEditTxn(sourceDb, "add data to source iModel", (txn) => {
      const sourceSubjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
      documentListModel = DocumentListModel.insert(txn, sourceSubjectId, "DL");
      parentDrawing = insertDrawingElement(
        txn,
        sourceDb,
        documentListModel,
        "ParentDrawing"
      );
      childDrawing1 = insertDrawingElement(
        txn,
        sourceDb,
        parentDrawing.id!,
        "ChildDrawing1"
      );
      childDrawing2 = insertDrawingElement(
        txn,
        sourceDb,
        parentDrawing.id!,
        "ChildDrawing2"
      );
      const parentDrawing2 = insertDrawingElement(
        txn,
        sourceDb,
        documentListModel,
        "ParentDrawing2"
      );
      insertDrawingElement(txn, sourceDb, parentDrawing2.id!, "ChildDrawing3");
      parentAspect1Id = insertElementAspect(
        txn,
        sourceSubjectId,
        parentDrawing.id!,
        "TestParentAspect1"
      );
      aspect1Id = insertElementAspect(
        txn,
        sourceSubjectId,
        childDrawing1.id!,
        "TestChildAspect1"
      );
      aspect2Id = insertElementAspect(
        txn,
        sourceSubjectId,
        childDrawing2.id!,
        "TestChildAspect2"
      );
      relationshipId = ElementGroupsMembers.create(
        sourceDb,
        childDrawing1.id!,
        childDrawing2.id!,
        0
      ).insert(txn);

      parentRelationshipId = ElementGroupsMembers.create(
        sourceDb,
        parentDrawing.id!,
        parentDrawing2.id!,
        0
      ).insert(txn);
    });
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
    txn: Parameters<Parameters<typeof withEditTxn>[2]>[0],
    iModel: IModelDb,
    documentListModelId: Id64String,
    drawingName: string
  ): ElementProps {
    const id = Drawing.insert(txn, documentListModelId, drawingName);
    return iModel.elements.getElementProps(id);
  }

  function insertElementAspect(
    txn: Parameters<Parameters<typeof withEditTxn>[2]>[0],
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

    return txn.insertAspect(aspectProps);
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
  describe("addChange", function () {
    it("identifies missing changed-instance metadata", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      const change: ChangedECInstance = {
        ECInstanceId: childDrawing1.id!,
        $meta: {
          tables: ["BisCore:Element"],
          op: "Inserted",
          stage: "New",
          changeIndexes: [],
        },
      };

      try {
        await sourceDbChanges.addChange(change);
        assert.fail("Expected addChange() to throw");
      } catch (error) {
        expect(
          ITwinError.isError(
            error,
            IModelTransformerErrorScope,
            IModelTransformerError.ChangedInstanceMetadataMissing
          )
        ).to.be.true;
        expect(error).to.have.property(
          "message",
          `ECClassId was not found for id: ${childDrawing1.id}! Table is : BisCore:Element`
        );
      }
    });
  });

  describe("addCustomElementChange", async function () {
    it("should add changes for related entities when element is Inserted", async function () {
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
      assertHasValues(
        sourceDbChanges.aspect,
        "aspect",
        [aspect1Id, parentAspect1Id],
        [],
        []
      );
      assertHasValues(
        sourceDbChanges.relationship,
        "relationship",
        [relationshipId, parentRelationshipId],
        [],
        []
      );
    });

    it("should add changes for related entities when element is Updated", async function () {
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

    it("should add changes for related entities when multiple elements are updated", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomElementChange("Updated", [
        childDrawing1.id!,
        childDrawing2.id!,
      ]);

      assertHasValues(
        sourceDbChanges.element,
        "element",
        [],
        [
          "0x1",
          documentListModel,
          parentDrawing.id!,
          childDrawing1.id!,
          childDrawing2.id!,
        ],
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

    it("should add changes for related entities when element is Deleted", async function () {
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

    it("should not add changes when empty array is passed for custom element change ", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomElementChange("Inserted", []);

      assertHasValues(sourceDbChanges.element, "element", [], [], []);
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
      assertHasValues(
        sourceDbChanges.aspect,
        "aspect",
        [parentAspect1Id],
        [],
        []
      );
      assertHasValues(
        sourceDbChanges.relationship,
        "relationship",
        [parentRelationshipId],
        [],
        []
      );
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
        [aspect1Id, aspect2Id, parentAspect1Id],
        [],
        []
      );
      assertHasValues(
        sourceDbChanges.relationship,
        "relationship",
        [relationshipId, parentRelationshipId],
        [],
        []
      );
    });

    it("should add custom changes when set with multiple models is passed to insert", async function () {
      // Arrange
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomModelChange(
        "Inserted",
        new Set([childDrawing1.id!, childDrawing2.id!])
      );

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
        [aspect1Id, aspect2Id, parentAspect1Id],
        [],
        []
      );
      assertHasValues(
        sourceDbChanges.relationship,
        "relationship",
        [relationshipId, parentRelationshipId],
        [],
        []
      );
    });

    it("should add custom changes when model is Updated", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomModelChange("Updated", parentDrawing.id!);
      // Act
      assertHasValues(
        sourceDbChanges.element,
        "element",
        [],
        [documentListModel, "0x1", parentDrawing.id!],
        []
      );
      assertHasValues(
        sourceDbChanges.model,
        "model",
        [],
        [documentListModel, "0x1", parentDrawing.id!],
        []
      );
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });

    it("should add custom changes when model is Deleted", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomModelChange("Deleted", parentDrawing.id!);
      // Act
      assertHasValues(
        sourceDbChanges.element,
        "element",
        [],
        [],
        [parentDrawing.id!]
      );
      assertHasValues(
        sourceDbChanges.model,
        "model",
        [],
        [],
        [parentDrawing.id!]
      );
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });

    it("should not add changes when empty array is passed for custom model change ", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      await sourceDbChanges.addCustomModelChange("Inserted", []);

      assertHasValues(sourceDbChanges.element, "element", [], [], []);
      assertHasValues(sourceDbChanges.model, "model", [], [], []);
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });
  });

  describe("addCustomAspectChange", async function () {
    it("should add custom changes when aspect is Inserted", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      sourceDbChanges.addCustomAspectChange("Inserted", aspect1Id);
      // Act
      assertHasValues(sourceDbChanges.element, "element", [], [], []);
      assertHasValues(sourceDbChanges.model, "model", [], [], []);
      assertHasValues(sourceDbChanges.aspect, "aspect", [aspect1Id], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });

    it("should add custom changes when aspect is Updated", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      sourceDbChanges.addCustomAspectChange("Updated", aspect1Id);
      // Act
      assertHasValues(sourceDbChanges.element, "element", [], [], []);
      assertHasValues(sourceDbChanges.model, "model", [], [], []);
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [aspect1Id], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });

    it("should add custom changes when aspect is Deleted", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      sourceDbChanges.addCustomAspectChange("Deleted", aspect1Id);
      // Act
      assertHasValues(sourceDbChanges.element, "element", [], [], []);
      assertHasValues(sourceDbChanges.model, "model", [], [], []);
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], [aspect1Id]);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });

    it("should not add changes when empty array is passed for custom aspect change ", async function () {
      const sourceDbChanges = new ChangedInstanceIds(sourceDb);
      sourceDbChanges.addCustomAspectChange("Inserted", []);
      assertHasValues(sourceDbChanges.element, "element", [], [], []);
      assertHasValues(sourceDbChanges.model, "model", [], [], []);
      assertHasValues(sourceDbChanges.aspect, "aspect", [], [], []);
      assertHasValues(sourceDbChanges.relationship, "relationship", [], [], []);
    });
  });
});
