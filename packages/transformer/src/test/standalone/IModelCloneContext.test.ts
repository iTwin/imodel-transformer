/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ElementRefersToElements,
  EntityReferences,
  GraphicalElement3dRepresentsElement,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ConcreteEntityTypes,
  IModel,
  PhysicalElementProps,
  RelationshipProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { expect } from "chai";
import * as path from "path";
import {
  IModelTransformerTestUtils,
  createStartedEditTxn,
} from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";

import { IModelTransformer } from "../../IModelTransformer";
import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests

describe("IModelCloneContext", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "IModelTransformer"
  );

  before(async () => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir)) {
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    }
    if (!IModelJsFs.existsSync(outputDir)) {
      IModelJsFs.mkdirSync(outputDir);
    }
  });

  describe("findTargetEntityId", () => {
    it("should return target relationship id", async () => {
      // Setup
      // Source IModelDb
      const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
        "IModelCloneContext",
        "ShouldReturnRelationShipId.bim"
      );
      const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
        rootSubject: { name: "invalid-relationships" },
      });

      withEditTxn(
        sourceDb,
        "setup source elements and relationships",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "SpatialCategory",
            new SubCategoryAppearance()
          );
          const sourceModelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysicalModel"
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: sourceModelId,
            category: categoryId,
            code: Code.createEmpty(),
          };
          const obj1 = txn.insertElement(physicalObjectProps);
          const obj2 = txn.insertElement(physicalObjectProps);
          const obj3 = txn.insertElement(physicalObjectProps);

          const relationshipsProps: RelationshipProps[] = [
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: obj1,
              sourceId: obj2,
            },
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: obj2,
              sourceId: obj1,
            },
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: obj2,
              sourceId: obj3,
            },
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: obj3,
              sourceId: obj2,
            },
          ];

          relationshipsProps.forEach((props) => txn.insertRelationship(props));
        }
      );

      // Target IModelDb
      const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
        "IModelTransformer",
        "relationships-Target.bim"
      );
      const targetDb = SnapshotDb.createEmpty(targetDbFile, {
        rootSubject: { name: "relationships-Target" },
      });

      const targetEditTxn = createStartedEditTxn(targetDb);
      // Import from beneath source Subject into target Subject
      const transformer = new IModelTransformer(
        sourceDb,
        targetDb,
        targetEditTxn
      );
      await transformer.process();
      targetEditTxn.end();

      // Assertion
      const sql = `SELECT r.ECInstanceId FROM ${ElementRefersToElements.classFullName} r
                    JOIN bis.Element s ON s.ECInstanceId = r.SourceECInstanceId
                    JOIN bis.Element t ON t.ECInstanceId = r.TargetECInstanceId
                    WHERE s.ECInstanceId IS NOT NULL AND t.ECInstanceId IS NOT NULL`;
      const sourceRelationshipIds: Id64String[] = [];
      for await (const row of sourceDb.createQueryReader(sql)) {
        sourceRelationshipIds.push(row.id);
      }
      let atLeastOneRelIdMissMatches = false;
      for (const sourceRelId of sourceRelationshipIds) {
        const targetRelId = EntityReferences.toId64(
          await transformer.context.findTargetEntityId(
            EntityReferences.fromEntityType(
              sourceRelId,
              ConcreteEntityTypes.Relationship
            )
          )
        );
        expect(targetRelId).to.not.be.equal(
          EntityReferences.fromEntityType(
            Id64.invalid,
            ConcreteEntityTypes.Relationship
          )
        ).and.to.be.not.undefined;

        if (!atLeastOneRelIdMissMatches)
          atLeastOneRelIdMissMatches = targetRelId !== sourceRelId;
      }
      /**
       * If this fails, then relationship ids match, and we don't really know if sourceDb and targetDb relationship ids differ.
       * It doesn't mean that functionality fails by itself.
       */
      expect(atLeastOneRelIdMissMatches).to.be.true;

      // Cleanup
      transformer.dispose();
      sourceDb.close();
      targetDb.close();
    });
  });
});
