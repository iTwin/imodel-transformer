/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "vitest";

import * as semver from "semver";

import {
  BisCoreSchema,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSourceAspect,
  GenericSchema,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";

import {
  AccessToken,
  Guid,
  GuidString,
  Id64,
  Id64Array,
  Id64String,
} from "@itwin/core-bentley";
import {
  Code,
  ColorDef,
  IModel,
  PhysicalElementProps,
  Placement3d,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import { IModelExporter, IModelTransformer } from "../../imodel-transformer";

import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
  PhysicalModelConsolidator,
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { registerHubTestContext } from "../TestUtils/HubTestContext";

const { count } = IModelTestUtils;

describe("IModelTransformerHub - consolidation", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext("IModelTransformerHubConsolidation", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
  });

  it("should consolidate PhysicalModels", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("ConsolidateModelsSource");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("ConsolidateModelsTarget");
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      // open/upgrade sourceDb
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });

      const sourceModelIds: Id64Array = [];
      // Helper functions that take EditTxn
      const insertPhysicalObject = (
        txn: Parameters<Parameters<typeof withEditTxn>[2]>[0],
        catId: Id64String,
        physicalModelId: Id64String,
        modelIndex: number,
        originX: number,
        originY: number,
        undefinedFederationGuid: boolean = false
      ) => {
        const physicalObjectProps1: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: physicalModelId,
          category: catId,
          code: Code.createEmpty(),
          userLabel: `M${modelIndex}-PhysicalObject(${originX},${originY})`,
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: Placement3d.fromJSON({
            origin: { x: originX, y: originY },
            angles: {},
          }),
        };
        if (undefinedFederationGuid)
          physicalObjectProps1.federationGuid = Guid.empty;
        txn.insertElement(physicalObjectProps1);
      };

      const insertModelWithElements = (
        txn: Parameters<Parameters<typeof withEditTxn>[2]>[0],
        catId: Id64String,
        modelIndex: number
      ): Id64String => {
        const sourceModelId: Id64String = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          `PhysicalModel${modelIndex}`
        );
        const xArray: number[] = [
          20 * modelIndex + 1,
          20 * modelIndex + 3,
          20 * modelIndex + 5,
          20 * modelIndex + 7,
          20 * modelIndex + 9,
        ];
        const yArray: number[] = [0, 2, 4, 6, 8];
        let undefinedFederationGuid = false;
        for (const x of xArray) {
          for (const y of yArray) {
            insertPhysicalObject(
              txn,
              catId,
              sourceModelId,
              modelIndex,
              x,
              y,
              undefinedFederationGuid
            );
            undefinedFederationGuid = !undefinedFederationGuid;
          }
        }
        return sourceModelId;
      };

      // Wrap all source inserts in a single EditTxn
      const categoryId = withEditTxn(
        sourceDb,
        "insert category and models",
        (txn) => {
          const catId: Id64String = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "SpatialCategory",
            { color: ColorDef.green.toJSON() }
          );

          // insert models 0-4 with 25 elements each (5*25).
          for (let i = 0; i < 5; i++) {
            sourceModelIds.push(insertModelWithElements(txn, catId, i));
          }

          return catId;
        }
      );
      assert.equal(5, count(sourceDb, PhysicalModel.classFullName));
      assert.equal(125, count(sourceDb, PhysicalObject.classFullName));
      await sourceDb.pushChanges({
        accessToken,
        description: "5 physical models",
      });

      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
      const targetModelId = withEditTxn(
        targetDb,
        "insert target model",
        (txn) =>
          PhysicalModel.insert(txn, IModel.rootSubjectId, "PhysicalModel")
      );
      assert.isTrue(Id64.isValidId64(targetModelId));

      const consolidateEditTxn1 = createStartedEditTxn(targetDb);
      let transformer = new PhysicalModelConsolidator(
        sourceDb,
        targetDb,
        consolidateEditTxn1,
        targetModelId
      );
      await transformer.process();
      consolidateEditTxn1.end();

      assert.equal(1, count(targetDb, PhysicalModel.classFullName));
      const targetPartition =
        targetDb.elements.getElement<PhysicalPartition>(targetModelId);
      assert.equal(
        targetPartition.code.value,
        "PhysicalModel",
        "Target PhysicalModel name should not be overwritten during consolidation"
      );
      assert.equal(125, count(targetDb, PhysicalObject.classFullName));
      const aspects = targetDb.elements.getAspects(
        targetPartition.id,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      expect(aspects.map((aspect) => aspect.identifier)).to.have.members(
        sourceModelIds
      );
      expect(aspects.length).to.equal(
        5,
        "Provenance should be recorded for each source PhysicalModel"
      );

      // Insert 10 objects under model-1, update model2/partition2, insert models 5 & 6
      withEditTxn(sourceDb, "additional inserts and updates", (txn) => {
        const xArr: number[] = [101, 105];
        const yArr: number[] = [0, 2, 4, 6, 8];
        let undefinedFedGuid = false;
        for (const x of xArr) {
          for (const y of yArr) {
            insertPhysicalObject(
              txn,
              categoryId,
              sourceModelIds[1],
              1,
              x,
              y,
              undefinedFedGuid
            );
            undefinedFedGuid = !undefinedFedGuid;
          }
        }

        // Update model2 and partition2
        const model2 = sourceDb.models.getModel(sourceModelIds[2]);
        model2.isPrivate = true;
        model2.update(txn);

        const partition2 = sourceDb.elements.getElement(sourceModelIds[2]);
        partition2.userLabel = "Element-Updated";
        partition2.update(txn);

        // insert model 5 & 6 and 50 physical objects
        for (let i = 5; i < 7; i++) {
          sourceModelIds.push(insertModelWithElements(txn, categoryId, i));
        }
      });
      await sourceDb.pushChanges({ description: "additional PhysicalModels" });
      // 2 models added
      assert.equal(7, count(sourceDb, PhysicalModel.classFullName));
      // 60 elements added
      assert.equal(185, count(sourceDb, PhysicalObject.classFullName));
      const consolidateEditTxn2 = createStartedEditTxn(targetDb);
      transformer = new PhysicalModelConsolidator(
        sourceDb,
        targetDb,
        consolidateEditTxn2,
        targetModelId,
        {
          startChangeset: sourceDb.changeset,
        }
      );
      await withTransformerLifecycle(transformer, [consolidateEditTxn2]);

      const sql = `SELECT ECInstanceId, Model.Id AS modelId FROM ${PhysicalObject.classFullName}`;
      let objectCounter = 0;
      for await (const row of targetDb.createQueryReader(sql)) {
        const targetElementId = row.id;
        const targetElement = targetDb.elements.getElement<PhysicalObject>({
          id: targetElementId,
          wantGeometry: true,
        });
        assert.exists(targetElement.geom);
        assert.isFalse(targetElement.calculateRange3d().isNull);
        const targetElementModelId = row.modelId;
        assert.equal(targetModelId, targetElementModelId);
        ++objectCounter;
      }
      assert.equal(185, objectCounter);

      assert.equal(1, count(targetDb, PhysicalModel.classFullName));
      let modelId = Id64.invalid;
      const modelReader = targetDb.createQueryReader(
        `SELECT ECInstanceId, isPrivate FROM ${PhysicalModel.classFullName}`
      );
      if (await modelReader.step()) {
        const isPrivate = modelReader.current.isPrivate;
        assert.isFalse(isPrivate);
        modelId = modelReader.current.id;
      }
      assert.isTrue(Id64.isValidId64(modelId));

      const physicalPartition =
        targetDb.elements.getElement<PhysicalPartition>(modelId);
      assert.equal("PhysicalModel", physicalPartition.code.value);

      const sourceAspects = targetDb.elements.getAspects(
        modelId,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      expect(sourceAspects.map((aspect) => aspect.identifier)).to.have.members(
        sourceModelIds
      );

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("Clone/upgrade test", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("CloneSource");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("CloneTarget");
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      // open/upgrade sourceDb
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const seedBisCoreVersion = sourceDb.querySchemaVersion(
        BisCoreSchema.schemaName
      )!;
      assert.isTrue(semver.satisfies(seedBisCoreVersion, ">= 1.0.1"));
      await sourceDb.importSchemas([
        BisCoreSchema.schemaFilePath,
        GenericSchema.schemaFilePath,
      ]);
      const updatedBisCoreVersion = sourceDb.querySchemaVersion(
        BisCoreSchema.schemaName
      )!;
      assert.isTrue(semver.satisfies(updatedBisCoreVersion, ">= 1.0.10"));
      assert.isTrue(
        sourceDb.containsClass(ExternalSourceAspect.classFullName),
        "Expect BisCore to be updated and contain ExternalSourceAspect"
      );
      const expectedHasPendingTxns: boolean =
        seedBisCoreVersion !== updatedBisCoreVersion;

      // push sourceDb schema changes
      assert.equal(
        sourceDb.txns.hasPendingTxns,
        expectedHasPendingTxns,
        "Expect importSchemas to have saved changes"
      );
      assert.isFalse(
        sourceDb.txns.hasUnsavedChanges,
        "Expect no unsaved changes after importSchemas"
      );
      await sourceDb.pushChanges({
        accessToken,
        description: "Import schemas to upgrade BisCore",
      }); // may push schema changes

      // import schemas again to test common scenario of not knowing whether schemas are up-to-date or not..
      await sourceDb.importSchemas([
        BisCoreSchema.schemaFilePath,
        GenericSchema.schemaFilePath,
      ]);
      assert.isFalse(
        sourceDb.txns.hasPendingTxns,
        "Expect importSchemas to be a no-op"
      );
      assert.isFalse(
        sourceDb.txns.hasUnsavedChanges,
        "Expect importSchemas to be a no-op"
      );
      await sourceDb.pushChanges({
        accessToken,
        description: "Import schemas again",
      }); // will be no changes to push in this case

      // populate sourceDb
      IModelTransformerTestUtils.populateTeamIModel(
        sourceDb,
        "Test",
        Point3d.createZero(),
        ColorDef.green
      );
      IModelTransformerTestUtils.assertTeamIModelContents(sourceDb, "Test");
      await sourceDb.pushChanges({
        accessToken,
        description: "Populate Source",
      });

      // open/upgrade targetDb
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
      await targetDb.importSchemas([
        BisCoreSchema.schemaFilePath,
        GenericSchema.schemaFilePath,
      ]);
      assert.isTrue(
        targetDb.containsClass(ExternalSourceAspect.classFullName),
        "Expect BisCore to be updated and contain ExternalSourceAspect"
      );

      // push targetDb schema changes
      withEditTxn(targetDb, "save schema changes", () => {});
      await targetDb.pushChanges({
        accessToken,
        description: "Upgrade BisCore",
      });

      // import sourceDb changes into targetDb
      const importEditTxn = createStartedEditTxn(targetDb);
      const transformer = new IModelTransformer({
        source: new IModelExporter(sourceDb),
        target: importEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      IModelTransformerTestUtils.assertTeamIModelContents(targetDb, "Test");
      importEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Import changes from sourceDb",
      });

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });
});
