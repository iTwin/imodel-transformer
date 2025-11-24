/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ECSqlStatement,
  ElementRefersToElements,
  EntityReferences,
  GraphicalElement3dRepresentsElement,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
} from "@itwin/core-backend";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
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
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
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

      const categoryId = SpatialCategory.insert(
        sourceDb,
        IModel.dictionaryId,
        "SpatialCategory",
        new SubCategoryAppearance()
      );
      const sourceModelId = PhysicalModel.insert(
        sourceDb,
        IModel.rootSubjectId,
        "PhysicalModel"
      );
      const physicalObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: sourceModelId,
        category: categoryId,
        code: Code.createEmpty(),
      };
      const physicalObject1 =
        sourceDb.elements.insertElement(physicalObjectProps);
      const physicalObject2 =
        sourceDb.elements.insertElement(physicalObjectProps);
      const physicalObject3 =
        sourceDb.elements.insertElement(physicalObjectProps);

      const relationshipsProps: RelationshipProps[] = [
        {
          classFullName: GraphicalElement3dRepresentsElement.classFullName,
          targetId: physicalObject1,
          sourceId: physicalObject2,
        },
        {
          classFullName: GraphicalElement3dRepresentsElement.classFullName,
          targetId: physicalObject2,
          sourceId: physicalObject1,
        },
        {
          classFullName: GraphicalElement3dRepresentsElement.classFullName,
          targetId: physicalObject2,
          sourceId: physicalObject3,
        },
        {
          classFullName: GraphicalElement3dRepresentsElement.classFullName,
          targetId: physicalObject3,
          sourceId: physicalObject2,
        },
      ];

      relationshipsProps.forEach((props) =>
        sourceDb.relationships.insertInstance(props)
      );
      // Target IModelDb
      const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
        "IModelTransformer",
        "relationships-Target.bim"
      );
      const targetDb = SnapshotDb.createEmpty(targetDbFile, {
        rootSubject: { name: "relationships-Target" },
      });
      // Import from beneath source Subject into target Subject
      const transformer = new IModelTransformer(sourceDb, targetDb);
      await transformer.process();
      targetDb.saveChanges();

      // Assertion
      const sql = `SELECT r.ECInstanceId FROM ${ElementRefersToElements.classFullName} r
                    JOIN bis.Element s ON s.ECInstanceId = r.SourceECInstanceId
                    JOIN bis.Element t ON t.ECInstanceId = r.TargetECInstanceId
                    WHERE s.ECInstanceId IS NOT NULL AND t.ECInstanceId IS NOT NULL`;
      const sourceRelationshipIds: Id64String[] = [];
      // eslint-disable-next-line @itwin/no-internal, @typescript-eslint/no-deprecated
      sourceDb.withPreparedStatement(sql, (statement: ECSqlStatement) => {
        while (DbResult.BE_SQLITE_ROW === statement.step()) {
          sourceRelationshipIds.push(statement.getValue(0).getId());
        }
      });
      let atLeastOneRelIdMissMatches = false;
      sourceRelationshipIds.forEach((sourceRelId) => {
        const targetRelId = EntityReferences.toId64(
          transformer.context.findTargetEntityId(
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
      });
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
