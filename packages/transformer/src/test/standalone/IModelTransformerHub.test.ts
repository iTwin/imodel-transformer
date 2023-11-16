/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { assert, expect } from "chai";
import * as path from "path";
import * as semver from "semver";
import {
  BisCoreSchema, BriefcaseDb, BriefcaseManager, CategorySelector, DefinitionContainer, DefinitionModel, DefinitionPartition, deleteElementTree, DisplayStyle3d, ECSqlStatement, Element, ElementGroupsMembers, ElementOwnsChildElements, ElementOwnsExternalSourceAspects, ElementRefersToElements,
  ExternalSourceAspect, GenericSchema, HubMock, IModelDb, IModelHost, IModelJsFs, IModelJsNative, ModelSelector, NativeLoggerCategory, PhysicalModel,
  PhysicalObject, PhysicalPartition, SnapshotDb, SpatialCategory, SpatialViewDefinition, Subject, SubjectOwnsPartitionElements, SubjectOwnsSubjects,
} from "@itwin/core-backend";

import * as TestUtils from "../TestUtils";
import { AccessToken, DbResult, Guid, GuidString, Id64, Id64Array, Id64String, Logger, LogLevel } from "@itwin/core-bentley";
import { Code, ColorDef, DefinitionElementProps, ElementProps, ExternalSourceAspectProps, IModel, IModelVersion, InformationPartitionElementProps, ModelProps, PhysicalElementProps, Placement3d, SpatialViewDefinitionProps, SubCategoryAppearance } from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelExporter, IModelImporter, IModelTransformer, TransformerLoggerCategory } from "../../transformer";
import {
  CountingIModelImporter, HubWrappers, IModelToTextFileExporter, IModelTransformerTestUtils, PhysicalModelConsolidator, TestIModelTransformer,
  TransformerExtensiveTestScenario as TransformerExtensiveTestScenario,
} from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";
import { IModelTestUtils } from "../TestUtils";

import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests
import * as sinon from "sinon";
import { assertElemState, deleted, populateTimelineSeed, runTimeline, Timeline, TimelineIModelElemState, TimelineIModelState } from "../TestUtils/TimelineTestUtil";
import { DetachedExportElementAspectsStrategy } from "../../DetachedExportElementAspectsStrategy";

const { count } = IModelTestUtils;

describe("IModelTransformerHub", () => {
  const outputDir = path.join(KnownTestLocations.outputDir, "IModelTransformerHub");
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  let saveAndPushChanges: (db: BriefcaseDb, desc: string) => Promise<void>;

  before(async () => {
    HubMock.startup("IModelTransformerHub", KnownTestLocations.outputDir);
    iTwinId = HubMock.iTwinId;
    IModelJsFs.recursiveMkDirSync(outputDir);

    accessToken = await HubWrappers.getAccessToken(TestUtils.TestUserType.Regular);
    saveAndPushChanges = IModelTestUtils.saveAndPushChanges.bind(IModelTestUtils, accessToken);

    // initialize logging
    if (process.env.TRANSFORMER_TESTS_USE_LOG) {
      Logger.initializeToConsole();
      Logger.setLevelDefault(LogLevel.Error);
      Logger.setLevel(TransformerLoggerCategory.IModelExporter, LogLevel.Trace);
      Logger.setLevel(TransformerLoggerCategory.IModelImporter, LogLevel.Trace);
      Logger.setLevel(TransformerLoggerCategory.IModelTransformer, LogLevel.Trace);
      Logger.setLevel(NativeLoggerCategory.Changeset, LogLevel.Trace);
    }
  });
  after(() => HubMock.shutdown());

  const createPopulatedIModelHubIModel = async (iModelName: string, prepareIModel?: (iModel: SnapshotDb) => void | Promise<void>): Promise<string> => {
    // Create and push seed of IModel
    const seedFileName = path.join(outputDir, `${iModelName}.bim`);
    if (IModelJsFs.existsSync(seedFileName))
      IModelJsFs.removeSync(seedFileName);

    const seedDb = SnapshotDb.createEmpty(seedFileName, { rootSubject: { name: iModelName } });
    assert.isTrue(IModelJsFs.existsSync(seedFileName));
    await prepareIModel?.(seedDb);
    seedDb.saveChanges();
    seedDb.close();

    const iModelId = await IModelHost.hubAccess.createNewIModel({ iTwinId, iModelName, description: "source", version0: seedFileName, noLocks: true });
    return iModelId;
  };

  it("Transform source iModel to target iModel", async () => {
    const sourceIModelId = await createPopulatedIModelHubIModel("TransformerSource", async (sourceSeedDb) => {
      await TestUtils.ExtensiveTestScenario.prepareDb(sourceSeedDb);
    });

    const targetIModelId = await createPopulatedIModelHubIModel("TransformerTarget", async (targetSeedDb) => {
      await TransformerExtensiveTestScenario.prepareTargetDb(targetSeedDb);
      assert.isTrue(targetSeedDb.codeSpecs.hasName("TargetCodeSpec")); // inserted by prepareTargetDb
    });

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });
      assert.isTrue(sourceDb.isBriefcaseDb());
      assert.isTrue(targetDb.isBriefcaseDb());
      assert.isFalse(sourceDb.isSnapshot);
      assert.isFalse(targetDb.isSnapshot);
      assert.isTrue(targetDb.codeSpecs.hasName("TargetCodeSpec")); // make sure prepareTargetDb changes were saved and pushed to iModelHub

      if (true) { // initial import
        TestUtils.ExtensiveTestScenario.populateDb(sourceDb);
        sourceDb.saveChanges();
        await sourceDb.pushChanges({ accessToken, description: "Populate source" });

        // Use IModelExporter.exportChanges to verify the changes to the sourceDb
        const sourceExportFileName: string = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "TransformerSource-ExportChanges-1.txt");
        assert.isFalse(IModelJsFs.existsSync(sourceExportFileName));
        const sourceExporter = new IModelToTextFileExporter(sourceDb, sourceExportFileName);
        sourceExporter.exporter["_resetChangeDataOnExport"] = false;
        await sourceExporter.exportChanges(accessToken);
        assert.isTrue(IModelJsFs.existsSync(sourceExportFileName));
        const sourceDbChanges = (sourceExporter.exporter as any)._sourceDbChanges; // access private member for testing purposes
        assert.exists(sourceDbChanges);
        // expect inserts and 1 update from populateSourceDb
        assert.isAtLeast(sourceDbChanges.codeSpec.insertIds.size, 1);
        assert.isAtLeast(sourceDbChanges.element.insertIds.size, 1);
        assert.isAtLeast(sourceDbChanges.aspect.insertIds.size, 1);
        assert.isAtLeast(sourceDbChanges.model.insertIds.size, 1);
        assert.equal(sourceDbChanges.model.updateIds.size, 1, "Expect the RepositoryModel to be updated");
        assert.isTrue(sourceDbChanges.model.updateIds.has(IModel.repositoryModelId));
        assert.isAtLeast(sourceDbChanges.relationship.insertIds.size, 1);
        // expect no other updates nor deletes from populateSourceDb
        assert.equal(sourceDbChanges.codeSpec.updateIds.size, 0);
        assert.equal(sourceDbChanges.codeSpec.deleteIds.size, 0);
        assert.equal(sourceDbChanges.element.updateIds.size, 0);
        assert.equal(sourceDbChanges.element.deleteIds.size, 0);
        assert.equal(sourceDbChanges.aspect.updateIds.size, 0);
        assert.equal(sourceDbChanges.aspect.deleteIds.size, 0);
        assert.equal(sourceDbChanges.model.deleteIds.size, 0);
        assert.equal(sourceDbChanges.relationship.updateIds.size, 0);
        assert.equal(sourceDbChanges.relationship.deleteIds.size, 0);

        const transformer = new TestIModelTransformer(sourceDb, targetDb);
        await transformer.processChanges({ accessToken });
        transformer.dispose();
        targetDb.saveChanges();
        await targetDb.pushChanges({ accessToken, description: "Import #1" });
        TransformerExtensiveTestScenario.assertTargetDbContents(sourceDb, targetDb);

        // Use IModelExporter.exportChanges to verify the changes to the targetDb
        const targetExportFileName: string = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "TransformerTarget-ExportChanges-1.txt");
        assert.isFalse(IModelJsFs.existsSync(targetExportFileName));
        const targetExporter = new IModelToTextFileExporter(targetDb, targetExportFileName);
        targetExporter.exporter["_resetChangeDataOnExport"] = false;
        await targetExporter.exportChanges(accessToken);
        assert.isTrue(IModelJsFs.existsSync(targetExportFileName));
        const targetDbChanges: any = (targetExporter.exporter as any)._sourceDbChanges; // access private member for testing purposes
        assert.exists(targetDbChanges);
        // expect inserts and a few updates from transforming the result of populateSourceDb
        assert.isAtLeast(targetDbChanges.codeSpec.insertIds.size, 1);
        assert.isAtLeast(targetDbChanges.element.insertIds.size, 1);
        assert.isAtMost(targetDbChanges.element.updateIds.size, 1, "Expect the root Subject to be updated");
        assert.isAtLeast(targetDbChanges.aspect.insertIds.size, 1);
        assert.isAtLeast(targetDbChanges.model.insertIds.size, 1);
        assert.isAtMost(targetDbChanges.model.updateIds.size, 1, "Expect the RepositoryModel to be updated");
        assert.isTrue(targetDbChanges.model.updateIds.has(IModel.repositoryModelId));
        assert.isAtLeast(targetDbChanges.relationship.insertIds.size, 1);
        // expect no other changes from transforming the result of populateSourceDb
        assert.equal(targetDbChanges.codeSpec.updateIds.size, 0);
        assert.equal(targetDbChanges.codeSpec.deleteIds.size, 0);
        assert.equal(targetDbChanges.element.deleteIds.size, 0);
        assert.equal(targetDbChanges.aspect.updateIds.size, 0);
        assert.equal(targetDbChanges.aspect.deleteIds.size, 0);
        assert.equal(targetDbChanges.model.deleteIds.size, 0);
        assert.equal(targetDbChanges.relationship.updateIds.size, 0);
        assert.equal(targetDbChanges.relationship.deleteIds.size, 0);
      }

      if (true) { // second import with no changes to source, should be a no-op
        const numTargetElements: number = count(targetDb, Element.classFullName);
        const numTargetExternalSourceAspects: number = count(targetDb, ExternalSourceAspect.classFullName);
        const numTargetRelationships: number = count(targetDb, ElementRefersToElements.classFullName);
        const targetImporter = new CountingIModelImporter(targetDb);
        const transformer = new TestIModelTransformer(sourceDb, targetImporter);
        await transformer.processChanges({ accessToken });
        assert.equal(targetImporter.numModelsInserted, 0);
        assert.equal(targetImporter.numModelsUpdated, 0);
        assert.equal(targetImporter.numElementsInserted, 0);
        expect(targetImporter.numElementsUpdated).to.equal(0);
        assert.equal(targetImporter.numElementsDeleted, 0);
        assert.equal(targetImporter.numElementAspectsInserted, 0);
        assert.equal(targetImporter.numElementAspectsUpdated, 0);
        assert.equal(targetImporter.numRelationshipsInserted, 0);
        assert.equal(targetImporter.numRelationshipsUpdated, 0);
        assert.equal(numTargetElements, count(targetDb, Element.classFullName), "Second import should not add elements");
        assert.equal(numTargetExternalSourceAspects, count(targetDb, ExternalSourceAspect.classFullName), "Second import should not add aspects");
        assert.equal(numTargetRelationships, count(targetDb, ElementRefersToElements.classFullName), "Second import should not add relationships");
        targetDb.saveChanges();
        assert.isFalse(targetDb.nativeDb.hasPendingTxns());
        await targetDb.pushChanges({ accessToken, description: "Should not actually push because there are no changes" });
        transformer.dispose();
      }

      if (true) { // update source db, then import again
        TestUtils.ExtensiveTestScenario.updateDb(sourceDb);
        sourceDb.saveChanges();
        await sourceDb.pushChanges({ accessToken, description: "Update source" });

        // Use IModelExporter.exportChanges to verify the changes to the sourceDb
        const sourceExportFileName: string = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "TransformerSource-ExportChanges-2.txt");
        assert.isFalse(IModelJsFs.existsSync(sourceExportFileName));
        const sourceExporter = new IModelToTextFileExporter(sourceDb, sourceExportFileName);
        sourceExporter.exporter["_resetChangeDataOnExport"] = false;
        await sourceExporter.exportChanges(accessToken);
        assert.isTrue(IModelJsFs.existsSync(sourceExportFileName));
        const sourceDbChanges: any = (sourceExporter.exporter as any)._sourceDbChanges; // access private member for testing purposes
        assert.exists(sourceDbChanges);
        // expect some inserts from updateDb
        assert.equal(sourceDbChanges.codeSpec.insertIds.size, 0);
        assert.equal(sourceDbChanges.element.insertIds.size, 1);
        assert.equal(sourceDbChanges.aspect.insertIds.size, 0);
        assert.equal(sourceDbChanges.model.insertIds.size, 0);
        assert.equal(sourceDbChanges.relationship.insertIds.size, 2);
        // expect some updates from updateDb
        assert.isAtLeast(sourceDbChanges.element.updateIds.size, 1);
        assert.isAtLeast(sourceDbChanges.aspect.updateIds.size, 1);
        assert.isAtLeast(sourceDbChanges.model.updateIds.size, 1);
        assert.isAtLeast(sourceDbChanges.relationship.updateIds.size, 1);
        // expect some deletes from updateDb
        assert.isAtLeast(sourceDbChanges.element.deleteIds.size, 1);
        assert.equal(sourceDbChanges.relationship.deleteIds.size, 1);
        assert.equal(sourceDbChanges.model.deleteIds.size, 1);
        // don't expect other changes from updateDb
        assert.equal(sourceDbChanges.codeSpec.updateIds.size, 0);
        assert.equal(sourceDbChanges.codeSpec.deleteIds.size, 0);
        assert.equal(sourceDbChanges.aspect.deleteIds.size, 0);

        const transformer = new TestIModelTransformer(sourceDb, targetDb);
        await transformer.processChanges({ accessToken });
        transformer.dispose();
        targetDb.saveChanges();
        await targetDb.pushChanges({ accessToken, description: "Import #2" });
        TestUtils.ExtensiveTestScenario.assertUpdatesInDb(targetDb);

        // Use IModelExporter.exportChanges to verify the changes to the targetDb
        const targetExportFileName: string = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "TransformerTarget-ExportChanges-2.txt");
        assert.isFalse(IModelJsFs.existsSync(targetExportFileName));
        const targetExporter = new IModelToTextFileExporter(targetDb, targetExportFileName);
        targetExporter.exporter["_resetChangeDataOnExport"] = false;
        await targetExporter.exportChanges(accessToken);
        assert.isTrue(IModelJsFs.existsSync(targetExportFileName));
        const targetDbChanges: any = (targetExporter.exporter as any)._sourceDbChanges; // access private member for testing purposes
        assert.exists(targetDbChanges);
        // expect some inserts from transforming the result of updateDb
        assert.equal(targetDbChanges.codeSpec.insertIds.size, 0);
        assert.equal(targetDbChanges.element.insertIds.size, 1);
        assert.equal(targetDbChanges.aspect.insertIds.size, 0);
        assert.equal(targetDbChanges.model.insertIds.size, 0);
        assert.equal(targetDbChanges.relationship.insertIds.size, 2);
        // expect some updates from transforming the result of updateDb
        assert.isAtLeast(targetDbChanges.element.updateIds.size, 1);
        assert.isAtLeast(targetDbChanges.aspect.updateIds.size, 1);
        assert.isAtLeast(targetDbChanges.model.updateIds.size, 1);
        assert.isAtLeast(targetDbChanges.relationship.updateIds.size, 1);
        // expect some deletes from transforming the result of updateDb
        assert.isAtLeast(targetDbChanges.element.deleteIds.size, 1);
        assert.isAtLeast(targetDbChanges.aspect.deleteIds.size, 0);
        assert.equal(targetDbChanges.relationship.deleteIds.size, 1);
        assert.equal(targetDbChanges.model.deleteIds.size, 1);
        // don't expect other changes from transforming the result of updateDb
        assert.equal(targetDbChanges.codeSpec.updateIds.size, 0);
        assert.equal(targetDbChanges.codeSpec.deleteIds.size, 0);
      }

      const sourceIModelChangeSets = await IModelHost.hubAccess.queryChangesets({ accessToken, iModelId: sourceIModelId });
      const targetIModelChangeSets = await IModelHost.hubAccess.queryChangesets({ accessToken, iModelId: targetIModelId });
      assert.equal(sourceIModelChangeSets.length, 2);
      assert.equal(targetIModelChangeSets.length, 2);

      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should consolidate PhysicalModels", async () => {
    const sourceIModelName: string = IModelTransformerTestUtils.generateUniqueName("ConsolidateModelsSource");
    const sourceIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: sourceIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string = IModelTransformerTestUtils.generateUniqueName("ConsolidateModelsTarget");
    const targetIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: targetIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      // open/upgrade sourceDb
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
      const categoryId: Id64String = SpatialCategory.insert(sourceDb, IModel.dictionaryId, "SpatialCategory", { color: ColorDef.green.toJSON() });
      const sourceModelIds: Id64Array = [];

      const insertPhysicalObject = (physicalModelId: Id64String, modelIndex: number, originX: number, originY: number, undefinedFederationGuid: boolean = false) => {
        const physicalObjectProps1: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: physicalModelId,
          category: categoryId,
          code: Code.createEmpty(),
          userLabel: `M${modelIndex}-PhysicalObject(${originX},${originY})`,
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: Placement3d.fromJSON({ origin: { x: originX, y: originY }, angles: {} }),
        };
        if (undefinedFederationGuid)
          physicalObjectProps1.federationGuid = Guid.empty;
        sourceDb.elements.insertElement(physicalObjectProps1);
      };

      const insertModelWithElements = (modelIndex: number): Id64String => {
        const sourceModelId: Id64String = PhysicalModel.insert(sourceDb, IModel.rootSubjectId, `PhysicalModel${modelIndex}`);
        const xArray: number[] = [20 * modelIndex + 1, 20 * modelIndex + 3, 20 * modelIndex + 5, 20 * modelIndex + 7, 20 * modelIndex + 9];
        const yArray: number[] = [0, 2, 4, 6, 8];
        let undefinedFederationGuid = false;
        for (const x of xArray) {
          for (const y of yArray) {
              insertPhysicalObject(sourceModelId, modelIndex, x, y, undefinedFederationGuid);
              undefinedFederationGuid = !undefinedFederationGuid;
          }
        }
        return sourceModelId;
      };

      // insert models 0-4 with 25 elements each (5*25).
      for (let i = 0; i < 5; i++) {
        sourceModelIds.push(insertModelWithElements(i));
      }

      sourceDb.saveChanges();
      assert.equal(5, count(sourceDb, PhysicalModel.classFullName));
      assert.equal(125, count(sourceDb, PhysicalObject.classFullName));
      await sourceDb.pushChanges({ accessToken, description: "5 physical models" });

      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });
      const targetModelId: Id64String = PhysicalModel.insert(targetDb, IModel.rootSubjectId, "PhysicalModel");
      assert.isTrue(Id64.isValidId64(targetModelId));
      targetDb.saveChanges();

      const transformer = new PhysicalModelConsolidator(sourceDb, targetDb, targetModelId);
      await transformer.processAll();

      assert.equal(1, count(targetDb, PhysicalModel.classFullName));
      const targetPartition = targetDb.elements.getElement<PhysicalPartition>(targetModelId);
      assert.equal(targetPartition.code.value, "PhysicalModel", "Target PhysicalModel name should not be overwritten during consolidation");
      assert.equal(125, count(targetDb, PhysicalObject.classFullName));
      const aspects = targetDb.elements.getAspects(targetPartition.id, ExternalSourceAspect.classFullName) as ExternalSourceAspect[];
      expect(aspects.map((aspect) => aspect.identifier)).to.have.members(sourceModelIds);
      expect(aspects.length).to.equal(5, "Provenance should be recorded for each source PhysicalModel");

      // Insert 10 objects under model-1
      const xArr: number[] = [101, 105];
      const yArr: number[] = [0, 2, 4, 6, 8];
      let undefinedFedGuid = false;
      for (const x of xArr) {
        for (const y of yArr) {
          insertPhysicalObject(sourceModelIds[1], 1, x, y, undefinedFedGuid);
          undefinedFedGuid = !undefinedFedGuid;
        }
      }

      // Update model2 and partition2
      const model2 = sourceDb.models.getModel(sourceModelIds[2]);
      model2.isPrivate = true;
      model2.update();

      const partition2 = sourceDb.elements.getElement(sourceModelIds[2]);
      partition2.userLabel = "Element-Updated";
      partition2.update();

      // insert model 5 & 6 and 50 physical objects
      for (let i = 5; i < 7; i++) {
        sourceModelIds.push(insertModelWithElements(i));
      }

      sourceDb.saveChanges();
      await sourceDb.pushChanges({description: "additional PhysicalModels"});
      // 2 models added
      assert.equal(7, count(sourceDb, PhysicalModel.classFullName));
      // 60 elements added
      assert.equal(185, count(sourceDb, PhysicalObject.classFullName));

      await transformer.processChanges({ accessToken, startChangeset: sourceDb.changeset });
      transformer.dispose();

      const sql = `SELECT ECInstanceId, Model.Id FROM ${PhysicalObject.classFullName}`;
      targetDb.withPreparedStatement(sql, (statement: ECSqlStatement) => {
        let objectCounter = 0;
        while (DbResult.BE_SQLITE_ROW === statement.step()) {
          const targetElementId = statement.getValue(0).getId();
          const targetElement = targetDb.elements.getElement<PhysicalObject>({ id: targetElementId, wantGeometry: true });
          assert.exists(targetElement.geom);
          assert.isFalse(targetElement.calculateRange3d().isNull);
          const targetElementModelId = statement.getValue(1).getId();
          assert.equal(targetModelId, targetElementModelId);
          ++objectCounter;
        }
        assert.equal(185, objectCounter);
      });

      assert.equal(1, count(targetDb, PhysicalModel.classFullName));
      const modelId = targetDb.withPreparedStatement(`SELECT ECInstanceId, isPrivate FROM ${PhysicalModel.classFullName}`, (statement: ECSqlStatement) => {
        if (DbResult.BE_SQLITE_ROW === statement.step()) {
          const isPrivate = statement.getValue(1).getBoolean();
          assert.isFalse(isPrivate);
          return statement.getValue(0).getId();
        }
        return Id64.invalid;
      });
      assert.isTrue(Id64.isValidId64(modelId));

      const physicalPartition = targetDb.elements.getElement<PhysicalPartition>(modelId);
      assert.equal("PhysicalModel", physicalPartition.code.value);

      const sourceAspects = targetDb.elements.getAspects(modelId, ExternalSourceAspect.classFullName) as ExternalSourceAspect[];
      expect(sourceAspects.map((aspect) => aspect.identifier)).to.have.members(sourceModelIds);

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("Clone/upgrade test", async () => {
    const sourceIModelName: string = IModelTransformerTestUtils.generateUniqueName("CloneSource");
    const sourceIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: sourceIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string = IModelTransformerTestUtils.generateUniqueName("CloneTarget");
    const targetIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: targetIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      // open/upgrade sourceDb
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
      const seedBisCoreVersion = sourceDb.querySchemaVersion(BisCoreSchema.schemaName)!;
      assert.isTrue(semver.satisfies(seedBisCoreVersion, ">= 1.0.1"));
      await sourceDb.importSchemas([BisCoreSchema.schemaFilePath, GenericSchema.schemaFilePath]);
      const updatedBisCoreVersion = sourceDb.querySchemaVersion(BisCoreSchema.schemaName)!;
      assert.isTrue(semver.satisfies(updatedBisCoreVersion, ">= 1.0.10"));
      assert.isTrue(sourceDb.containsClass(ExternalSourceAspect.classFullName), "Expect BisCore to be updated and contain ExternalSourceAspect");
      const expectedHasPendingTxns: boolean = seedBisCoreVersion !== updatedBisCoreVersion;

      // push sourceDb schema changes
      assert.equal(sourceDb.nativeDb.hasPendingTxns(), expectedHasPendingTxns, "Expect importSchemas to have saved changes");
      assert.isFalse(sourceDb.nativeDb.hasUnsavedChanges(), "Expect no unsaved changes after importSchemas");
      await sourceDb.pushChanges({ accessToken, description: "Import schemas to upgrade BisCore" }); // may push schema changes

      // import schemas again to test common scenario of not knowing whether schemas are up-to-date or not..
      await sourceDb.importSchemas([BisCoreSchema.schemaFilePath, GenericSchema.schemaFilePath]);
      assert.isFalse(sourceDb.nativeDb.hasPendingTxns(), "Expect importSchemas to be a no-op");
      assert.isFalse(sourceDb.nativeDb.hasUnsavedChanges(), "Expect importSchemas to be a no-op");
      sourceDb.saveChanges(); // will be no changes to save in this case
      await sourceDb.pushChanges({ accessToken, description: "Import schemas again" }); // will be no changes to push in this case

      // populate sourceDb
      IModelTransformerTestUtils.populateTeamIModel(sourceDb, "Test", Point3d.createZero(), ColorDef.green);
      IModelTransformerTestUtils.assertTeamIModelContents(sourceDb, "Test");
      sourceDb.saveChanges();
      await sourceDb.pushChanges({ accessToken, description: "Populate Source" });

      // open/upgrade targetDb
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });
      await targetDb.importSchemas([BisCoreSchema.schemaFilePath, GenericSchema.schemaFilePath]);
      assert.isTrue(targetDb.containsClass(ExternalSourceAspect.classFullName), "Expect BisCore to be updated and contain ExternalSourceAspect");

      // push targetDb schema changes
      targetDb.saveChanges();
      await targetDb.pushChanges({ accessToken, description: "Upgrade BisCore" });

      // import sourceDb changes into targetDb
      const transformer = new IModelTransformer(new IModelExporter(sourceDb), targetDb);
      await transformer.processAll();
      transformer.dispose();
      IModelTransformerTestUtils.assertTeamIModelContents(targetDb, "Test");
      targetDb.saveChanges();
      await targetDb.pushChanges({ accessToken, description: "Import changes from sourceDb" });

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should merge changes made on a branch back to master", async () => {
    const masterIModelName = "Master";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = {1:1, 2:1, 20:1, 21:1, 40:1, 41:2, 42:3};
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, { rootSubject: { name: masterIModelName } });
    masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);

    // 20 will be deleted, so it's important to know remapping deleted elements still works if there is no fedguid
    const noFedGuidElemIds = masterSeedDb.queryEntityIds({ from: "Bis.Element", where: "UserLabel IN ('1','20','41','42')" });
    for (const elemId of noFedGuidElemIds)
      masterSeedDb.withSqliteStatement(
        `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
        (s) => { expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE); }
      );
    masterSeedDb.performCheckpoint();

    // hard to check this without closing the db...
    const seedSecondConn = SnapshotDb.openFile(masterSeedDb.pathName);
    for (const elemId of noFedGuidElemIds)
      expect(seedSecondConn.elements.getElement(elemId).federationGuid).to.be.undefined;
    seedSecondConn.close();

    const expectedRelationships = [
      { sourceLabel: "40", targetLabel: "2", idInBranch1: "not inserted yet", sourceFedGuid: true, targetFedGuid: true },
      { sourceLabel: "41", targetLabel: "42", idInBranch1: "not inserted yet", sourceFedGuid: false, targetFedGuid: false },
    ];

    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };

    const timeline: Timeline = [
      { master: { seed: masterSeed } }, // masterSeedState is above
      { branch1: { branch: "master" } },
      { master: { 40:5 } },
      { branch2: { branch: "master" } },
      { branch1: { 2:2, 3:1, 4:1 } },
      {
        branch1: {
          manualUpdate(db) {
            expectedRelationships.map(
              ({ sourceLabel, targetLabel }, i) => {
                const sourceId = IModelTestUtils.queryByUserLabel(db, sourceLabel);
                const targetId = IModelTestUtils.queryByUserLabel(db, targetLabel);
                assert(sourceId && targetId);
                const rel = ElementGroupsMembers.create(db, sourceId, targetId, 0);
                expectedRelationships[i].idInBranch1 = rel.insert();
              }
            );
          },
        },
      },
      {
        branch1: {
          manualUpdate(db) {
            const rel = db.relationships.getInstance<ElementGroupsMembers>(
              ElementGroupsMembers.classFullName,
              expectedRelationships[0].idInBranch1,
            );
            rel.memberPriority = 1;
            rel.update();
          },
        },
      },
      { branch1: { 1:2, 3:deleted, 5:1, 6:1, 20:deleted, 21:2 } },
      { branch1: { 21:deleted, 30:1 } },
      { master: { sync: ["branch1"] } }, // first master<-branch1 reverse sync
      {
        assert({ master, branch1 }) {
          assertElemState(master.db, {
            // relationship props are a lot to type out so let's grab those from the branch
            ...branch1.state,
            // double check deletions propagated by sync
            20: undefined as any,
            21: undefined as any,
            40:5, // this element was not changed in the branch, so the sync won't update it
          });
        },
      },
      { branch2: { sync: ["master"] } }, // first master->branch2 forward sync
      { assert({ master, branch2 }) { assertElemState(branch2.db, master.state); } },
      { branch2: { 7:1, 8:1 } },
      // insert 9 and a conflicting state for 7 on master
      { master: { 7:2, 9:1 } },
      { master: { sync: ["branch2"] } }, // first master<-branch2 reverse sync
      {
        assert({ master, branch1, branch2 }) {
          for (const { db } of [master, branch1, branch2]) {
            const elem1Id = IModelTestUtils.queryByUserLabel(db, "1");
            expect(db.elements.getElement(elem1Id).federationGuid).to.be.undefined;

            for (const rel of expectedRelationships) {
              const sourceId = IModelTestUtils.queryByUserLabel(db, rel.sourceLabel);
              const targetId = IModelTestUtils.queryByUserLabel(db, rel.targetLabel);
              expect(db.elements.getElement(sourceId).federationGuid !== undefined).to.be.equal(rel.sourceFedGuid);
              expect(db.elements.getElement(targetId).federationGuid !== undefined).to.be.equal(rel.targetFedGuid);
            }
          }

          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(0);

          for (const branch of [branch1, branch2]) {
            const elem1Id = IModelTestUtils.queryByUserLabel(branch.db, "1");
            expect(branch.db.elements.getElement(elem1Id).federationGuid).to.be.undefined;
            const aspects =
              [...branch.db.queryEntityIds({ from: "BisCore.ExternalSourceAspect" })]
              .map((aspectId) => branch.db.elements.getAspect(aspectId).toJSON()) as ExternalSourceAspectProps[];
            expect(aspects).to.deep.subsetEqual([
              {
                element: { id: IModelDb.rootSubjectId },
                identifier: master.db.iModelId,
              },
              {
                element: { id: "0xe" }, // link partition
                identifier: "0xe",
              },
              {
                element: { id: IModelDb.dictionaryId },
                identifier: IModelDb.dictionaryId,
              },
              {
                element: { id: elem1Id },
                identifier: elem1Id,
              },
            ]);
            expect(Date.parse(aspects[3].version!)).not.to.be.NaN;
          }

          // branch2 won the conflict since it is the synchronization source
          assertElemState(master.db, {7:1}, { subset: true });
        },
      },
      { master: { 6:2 } },
      {
        master: {
          manualUpdate(db) {
            expectedRelationships.forEach(
              ({ sourceLabel, targetLabel }) => {
                const sourceId = IModelTestUtils.queryByUserLabel(db, sourceLabel);
                const targetId = IModelTestUtils.queryByUserLabel(db, targetLabel);
                assert(sourceId && targetId);
                const rel = db.relationships.getInstance(
                  ElementGroupsMembers.classFullName,
                  { sourceId, targetId }
                );
                return rel.delete();
              }
            );
          },
        },
      },
      // FIXME: do a later sync and resync. Branch1 gets master's changes. master merges into branch1.
      { branch1: { sync: ["master"] } }, // first master->branch1 forward sync
      {
        assert({branch1}) {
          for (const rel of expectedRelationships) {
            expect(branch1.db.relationships.tryGetInstance(
              ElementGroupsMembers.classFullName,
              rel.idInBranch1,
            ), `had ${rel.sourceLabel}->${rel.targetLabel}`).to.be.undefined;
            const sourceId = IModelTestUtils.queryByUserLabel(branch1.db, rel.sourceLabel);
            const targetId = IModelTestUtils.queryByUserLabel(branch1.db, rel.targetLabel);
            assert(sourceId && targetId);
            expect(branch1.db.relationships.tryGetInstance(
              ElementGroupsMembers.classFullName,
              { sourceId, targetId },
            ), `had ${rel.sourceLabel}->${rel.targetLabel}`).to.be.undefined;

            // check rel aspect was deleted
            const srcElemAspects = branch1.db.elements.getAspects(sourceId, ExternalSourceAspect.classFullName) as ExternalSourceAspect[];
            expect(!srcElemAspects.some((a) => a.identifier === rel.idInBranch1)).to.be.true;
            expect(srcElemAspects.length).to.lessThanOrEqual(1);
          }
        },
      },
    ];

    const { trackedIModels, tearDown } = await runTimeline(timeline, { iTwinId, accessToken });
    masterSeedDb.close();

    // create empty iModel meant to contain replayed master history
    const replayedIModelName = "Replayed";
    const replayedIModelId = await IModelHost.hubAccess.createNewIModel({ iTwinId, iModelName: replayedIModelName, description: "blank", noLocks: true });

    const replayedDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: replayedIModelId });
    assert.isTrue(replayedDb.isBriefcaseDb());
    assert.equal(replayedDb.iTwinId, iTwinId);

    try {
      const master = trackedIModels.get("master");
      assert(master);

      const masterDbChangesets = await IModelHost.hubAccess.downloadChangesets({ accessToken, iModelId: master.id, targetDir: BriefcaseManager.getChangeSetsPath(master.id) });
      assert.equal(masterDbChangesets.length, 6);
      const masterDeletedElementIds = new Set<Id64String>();
      const masterDeletedRelationshipIds = new Set<Id64String>();
      for (const masterDbChangeset of masterDbChangesets) {
        assert.isDefined(masterDbChangeset.id);
        assert.isDefined(masterDbChangeset.description); // test code above always included a change description when pushChanges was called
        const changesetPath = masterDbChangeset.pathname;
        assert.isTrue(IModelJsFs.existsSync(changesetPath));
        // below is one way of determining the set of elements that were deleted in a specific changeset
        const statusOrResult = master.db.nativeDb.extractChangedInstanceIdsFromChangeSets([changesetPath]);
        assert.isUndefined(statusOrResult.error);
        const result = statusOrResult.result;
        if (result === undefined)
          throw Error("expected to be defined");

        if (result.element?.delete) {
          result.element.delete.forEach((id: Id64String) => masterDeletedElementIds.add(id));
        }
        if (result.relationship?.delete) {
          result.relationship.delete.forEach((id: Id64String) => masterDeletedRelationshipIds.add(id));
        }
      }
      expect(masterDeletedElementIds.size).to.equal(2); // elem '3' is never seen by master
      expect(masterDeletedRelationshipIds.size).to.equal(2);

      // replay master history to create replayed iModel
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: master.id, asOf: IModelVersion.first().toJSON() });
      const makeReplayTransformer = () => {
        const result = new IModelTransformer(sourceDb, replayedDb);
        // this replay strategy pretends that deleted elements never existed
        for (const elementId of masterDeletedElementIds) {
          result.exporter.excludeElement(elementId);
        }
        return result;
      };

      // NOTE: this test knows that there were no schema changes, so does not call `processSchemas`
      const replayInitTransformer = makeReplayTransformer();
      await replayInitTransformer.processAll(); // process any elements that were part of the "seed"
      replayInitTransformer.dispose();

      await saveAndPushChanges(replayedDb, "changes from source seed");
      for (const masterDbChangeset of masterDbChangesets) {
        const replayTransformer = makeReplayTransformer();
        await sourceDb.pullChanges({ accessToken, toIndex: masterDbChangeset.index });
        await replayTransformer.processChanges({ accessToken, startChangeset: sourceDb.changeset });
        await saveAndPushChanges(replayedDb, masterDbChangeset.description ?? "");
        replayTransformer.dispose();
      }
      sourceDb.close();
      assertElemState(replayedDb, master.state); // should have same ending state as masterDb

      // make sure there are no deletes in the replay history (all elements that were eventually deleted from masterDb were excluded)
      const replayedDbChangesets = await IModelHost.hubAccess.downloadChangesets({ accessToken, iModelId: replayedIModelId, targetDir: BriefcaseManager.getChangeSetsPath(replayedIModelId) });
      assert.isAtLeast(replayedDbChangesets.length, masterDbChangesets.length); // replayedDb will have more changesets when seed contains elements
      const replayedDeletedElementIds = new Set<Id64String>();
      for (const replayedDbChangeset of replayedDbChangesets) {
        assert.isDefined(replayedDbChangeset.id);
        const changesetPath = replayedDbChangeset.pathname;
        assert.isTrue(IModelJsFs.existsSync(changesetPath));
        // below is one way of determining the set of elements that were deleted in a specific changeset
        const statusOrResult = replayedDb.nativeDb.extractChangedInstanceIdsFromChangeSets([changesetPath]);
        const result = statusOrResult.result;
        if (result === undefined)
          throw Error("expected to be defined");

        assert.isDefined(result.element);
        if (result.element?.delete) {
          result.element.delete.forEach((id: Id64String) => replayedDeletedElementIds.add(id));
        }
      }
      assert.equal(replayedDeletedElementIds.size, 0);
    } finally {
      await tearDown();
      replayedDb.close();
      await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: replayedIModelId });
    }
  });

  it("ModelSelector processChanges", async () => {
    const sourceIModelName = "ModelSelectorSource";
    const sourceIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: sourceIModelName, noLocks: true });
    let targetIModelId!: GuidString;
    assert.isTrue(Guid.isGuid(sourceIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });

      // setup source
      const physModel1Id = PhysicalModel.insert(sourceDb, IModel.rootSubjectId, "phys-model-1");
      const physModel2Id = PhysicalModel.insert(sourceDb, IModel.rootSubjectId, "phys-model-2");
      const modelSelectorInSource = ModelSelector.create(sourceDb, IModelDb.dictionaryId, "model-selector", [physModel1Id]);
      const modelSelectorCode = modelSelectorInSource.code;
      const modelSelectorId = modelSelectorInSource.insert();
      sourceDb.saveChanges();
      await sourceDb.pushChanges({ accessToken, description: "setup source models and selector" });

      // create target branch
      const targetIModelName = "ModelSelectorTarget";
      sourceDb.performCheckpoint();

      targetIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: targetIModelName, noLocks: true, version0: sourceDb.pathName });
      assert.isTrue(Guid.isGuid(targetIModelId));
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });
      await targetDb.importSchemas([BisCoreSchema.schemaFilePath, GenericSchema.schemaFilePath]);
      assert.isTrue(targetDb.containsClass(ExternalSourceAspect.classFullName), "Expect BisCore to be updated and contain ExternalSourceAspect");
      const provenanceInitializer = new IModelTransformer(sourceDb, targetDb, { wasSourceIModelCopiedToTarget: true });
      await provenanceInitializer.processSchemas();
      await provenanceInitializer.processAll();
      provenanceInitializer.dispose();

      // update source (add model2 to model selector)
      // (it's important that we only change the model selector here to keep the changes isolated)
      const modelSelectorUpdate = sourceDb.elements.getElement<ModelSelector>(modelSelectorId, ModelSelector);
      modelSelectorUpdate.models = [...modelSelectorUpdate.models, physModel2Id];
      modelSelectorUpdate.update();
      sourceDb.saveChanges();
      await sourceDb.pushChanges({ accessToken, description: "add model2 to model selector" });

      // check that the model selector has the expected change in the source
      const modelSelectorUpdate2 = sourceDb.elements.getElement<ModelSelector>(modelSelectorId, ModelSelector);
      expect(modelSelectorUpdate2.models).to.have.length(2);

      // test extracted changed ids
      const sourceDbChangesets = await IModelHost.hubAccess.downloadChangesets({ accessToken, iModelId: sourceIModelId, targetDir: BriefcaseManager.getChangeSetsPath(sourceIModelId) });
      expect(sourceDbChangesets).to.have.length(2);
      const latestChangeset = sourceDbChangesets[1];
      const extractedChangedIds = sourceDb.nativeDb.extractChangedInstanceIdsFromChangeSets([latestChangeset.pathname]);
      const expectedChangedIds: IModelJsNative.ChangedInstanceIdsProps = {
        element: { update: [modelSelectorId] },
        model: { update: [IModel.dictionaryId] }, // containing model will also get last modification time updated
      };
      expect(extractedChangedIds.result).to.deep.equal(expectedChangedIds);

      // synchronize
      let didExportModelSelector = false, didImportModelSelector = false;
      class IModelImporterInjected extends IModelImporter {
        public override importElement(sourceElement: ElementProps): Id64String {
          if (sourceElement.id === modelSelectorId)
            didImportModelSelector = true;
          return super.importElement(sourceElement);
        }
      }
      class IModelTransformerInjected extends IModelTransformer {
        public override async onExportElement(sourceElement: Element) {
          if (sourceElement.id === modelSelectorId)
            didExportModelSelector = true;
          return super.onExportElement(sourceElement);
        }
      }

      const synchronizer = new IModelTransformerInjected(sourceDb, new IModelImporterInjected(targetDb));
      await synchronizer.processChanges({ accessToken });
      expect(didExportModelSelector).to.be.true;
      expect(didImportModelSelector).to.be.true;
      synchronizer.dispose();
      targetDb.saveChanges();
      await targetDb.pushChanges({ accessToken, description: "synchronize" });

      // check that the model selector has the expected change in the target
      const modelSelectorInTargetId = targetDb.elements.queryElementIdByCode(modelSelectorCode);
      assert(modelSelectorInTargetId !== undefined, `expected obj ${modelSelectorInTargetId} to be defined`);

      const modelSelectorInTarget = targetDb.elements.getElement<ModelSelector>(modelSelectorInTargetId, ModelSelector);
      expect(modelSelectorInTarget.models).to.have.length(2);

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        assert.fail(err, undefined, "failed to clean up");
      }
    }
  });

  it("should correctly initialize provenance map for change processing", async () => {
    const sourceIModelName: string = IModelTransformerTestUtils.generateUniqueName("Source");
    const sourceIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: sourceIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string = IModelTransformerTestUtils.generateUniqueName("Target");
    const targetIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: targetIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      // open/upgrade sourceDb
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });

      const subject1 = Subject.create(sourceDb, IModel.rootSubjectId, "S1");
      const subject2 = Subject.create(sourceDb, IModel.rootSubjectId, "S2");
      subject2.federationGuid = Guid.empty; // Empty guid will force the element to have an undefined federation guid.
      subject1.insert();
      const subject2Id = subject2.insert();
      PhysicalModel.insert(sourceDb, subject2Id, `PM1`);

      sourceDb.saveChanges();
      await sourceDb.pushChanges({ accessToken, description: "subject with no fed guid" });

      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });
      let transformer = new IModelTransformer(sourceDb, targetDb);
      await transformer.processAll();
      targetDb.saveChanges();
      transformer.dispose();

      PhysicalModel.insert(sourceDb, subject2Id, `PM2`);
      sourceDb.saveChanges();
      await sourceDb.pushChanges({ accessToken, description: "PhysicalPartition" });

      transformer = new IModelTransformer(sourceDb, targetDb);
      await transformer.processChanges({accessToken, startChangeset: {id: sourceDb.changeset.id}});

      const elementCodeValueMap = new Map<Id64String, string>();
      targetDb.withStatement(`SELECT ECInstanceId, CodeValue FROM ${Element.classFullName} WHERE ECInstanceId NOT IN (0x1, 0x10, 0xe)`, (statement: ECSqlStatement) => {
        while (statement.step() === DbResult.BE_SQLITE_ROW) {
            elementCodeValueMap.set(statement.getValue(0).getId(), statement.getValue(1).getString());
        }
      });

      // make sure provenance was tracked for all elements
      expect(count(sourceDb, Element.classFullName)).to.equal(4+3); // 2 Subjects, 2 PhysicalPartitions + 0x1, 0x10, 0xe
      expect(elementCodeValueMap.size).to.equal(4);
      elementCodeValueMap.forEach((codeValue: string, elementId: Id64String) => {
        const sourceElementId = transformer.context.findTargetElementId(elementId);
        expect(sourceElementId).to.not.be.undefined;
        const sourceElement = sourceDb.elements.getElement(sourceElementId);
        expect(sourceElement.code.value).to.equal(codeValue);
      });

      transformer.dispose();

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should correctly reverse synchronize changes when targetDb was a clone of sourceDb", async () => {
    const seedFileName = path.join(outputDir, `seed.bim`);
    if (IModelJsFs.existsSync(seedFileName))
      IModelJsFs.removeSync(seedFileName);

    const seedDb = SnapshotDb.createEmpty(seedFileName, { rootSubject: { name: "TransformerSource" } });
    const subjectId1 = Subject.insert(seedDb, IModel.rootSubjectId, "S1");
    const modelId1 = PhysicalModel.insert(seedDb, subjectId1, "PM1");
    const categoryId1 = SpatialCategory.insert(seedDb, IModel.dictionaryId, "C1", {});
    const physicalElementProps1: PhysicalElementProps = {
      category: categoryId1,
      model: modelId1,
      classFullName: PhysicalObject.classFullName,
      code: Code.createEmpty(),
    };
    seedDb.elements.insertElement(physicalElementProps1);
    seedDb.saveChanges();
    seedDb.close();

    let sourceIModelId: string | undefined;
    let targetIModelId: string | undefined;

    try {
      sourceIModelId = await IModelHost.hubAccess.createNewIModel({ iTwinId, iModelName: "TransformerSource", description: "source", version0: seedFileName, noLocks: true });

      // open/upgrade sourceDb
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
      // creating changesets for source
      for (let i = 0; i < 4; i++) {
        const physicalElementProps: PhysicalElementProps = {
          category: categoryId1,
          model: modelId1,
          classFullName: PhysicalObject.classFullName,
          code: Code.createEmpty(),
        };
        sourceDb.elements.insertElement(physicalElementProps);
        sourceDb.saveChanges();
        await sourceDb.pushChanges({description: `Inserted ${i} PhysicalObject`});
      }
      sourceDb.performCheckpoint(); // so we can use as a seed

      // forking target
      targetIModelId = await IModelHost.hubAccess.createNewIModel({ iTwinId, iModelName: "TransformerTarget", description: "target", version0: sourceDb.pathName, noLocks: true });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });

      // fork provenance init
      let transformer = new IModelTransformer(sourceDb, targetDb, { wasSourceIModelCopiedToTarget: true });
      await transformer.processAll();
      targetDb.saveChanges();
      await targetDb.pushChanges({description: "fork init"});
      transformer.dispose();

      const targetSubjectId = Subject.insert(targetDb, IModel.rootSubjectId, "S2");
      const targetModelId = PhysicalModel.insert(targetDb, targetSubjectId, "PM2");
      const targetCategoryId = SpatialCategory.insert(targetDb, IModel.dictionaryId, "C2", {});

      // adding more changesets to target
      for(let i = 0; i < 2; i++){
        const targetPhysicalElementProps: PhysicalElementProps = {
          category: targetCategoryId,
          model: targetModelId,
          classFullName: PhysicalObject.classFullName,
          code: Code.createEmpty(),
        };
        targetDb.elements.insertElement(targetPhysicalElementProps);
        targetDb.saveChanges();
        await targetDb.pushChanges({description: `Inserted ${i} PhysicalObject`});
      }

      // running reverse synchronization
      transformer = new IModelTransformer(targetDb, sourceDb, { isReverseSynchronization: true });

      await transformer.processChanges({accessToken});
      transformer.dispose();

      expect(count(sourceDb, PhysicalObject.classFullName)).to.equal(7);
      expect(count(targetDb, PhysicalObject.classFullName)).to.equal(7);

      expect(count(sourceDb, Subject.classFullName)).to.equal(2+1); // 2 inserted manually + root subject
      expect(count(targetDb, Subject.classFullName)).to.equal(2+1); // 2 inserted manually + root subject

      expect(count(sourceDb, SpatialCategory.classFullName)).to.equal(2);
      expect(count(targetDb, SpatialCategory.classFullName)).to.equal(2);

      expect(count(sourceDb, PhysicalModel.classFullName)).to.equal(2);
      expect(count(targetDb, PhysicalModel.classFullName)).to.equal(2);

      expect(count(sourceDb, PhysicalPartition.classFullName)).to.equal(2);
      expect(count(targetDb, PhysicalPartition.classFullName)).to.equal(2);

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        if (sourceIModelId)
          await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        if (targetIModelId)
          await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should delete branch-deleted elements in reverse synchronization", async () => {
    const masterIModelName = "ReSyncDeleteMaster";
    const masterIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: masterIModelName, noLocks: true });
    let branchIModelId!: GuidString;
    assert.isTrue(Guid.isGuid(masterIModelId));

    try {
      const masterDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: masterIModelId });

      // populate master
      const categId = SpatialCategory.insert(masterDb, IModel.dictionaryId, "category", new SubCategoryAppearance());
      const modelToDeleteWithElemId = PhysicalModel.insert(masterDb, IModel.rootSubjectId, "model-to-delete-with-elem");
      const makePhysObjCommonProps = (num: number) => ({
        classFullName: PhysicalObject.classFullName,
        category: categId,
        geom: IModelTransformerTestUtils.createBox(Point3d.create(num, num, num)),
        placement: {
          origin: Point3d.create(num, num, num),
          angles: YawPitchRollAngles.createDegrees(num, num, num),
        },
      } as const);
      const elemInModelToDeleteId = new PhysicalObject({
        ...makePhysObjCommonProps(1),
        model: modelToDeleteWithElemId,
        code: new Code({ spec: IModelDb.rootSubjectId, scope: IModelDb.rootSubjectId, value: "elem-in-model-to-delete" }),
        userLabel: "elem-in-model-to-delete",
      }, masterDb).insert();
      const notDeletedModelId = PhysicalModel.insert(masterDb, IModel.rootSubjectId, "not-deleted-model");
      const elemToDeleteWithChildrenId = new PhysicalObject({
        ...makePhysObjCommonProps(2),
        model: notDeletedModelId,
        code: new Code({ spec: IModelDb.rootSubjectId, scope: IModelDb.rootSubjectId, value: "deleted-elem-with-children" }),
        userLabel: "deleted-elem-with-children",
      }, masterDb).insert();
      const childElemOfDeletedId = new PhysicalObject({
        ...makePhysObjCommonProps(3),
        model: notDeletedModelId,
        code: new Code({ spec: IModelDb.rootSubjectId, scope: IModelDb.rootSubjectId, value: "child-elem-of-deleted" }),
        userLabel: "child-elem-of-deleted",
        parent: new ElementOwnsChildElements(elemToDeleteWithChildrenId),
      }, masterDb).insert();
      const childSubjectId = Subject.insert(masterDb, IModel.rootSubjectId, "child-subject");
      const modelInChildSubjectId = PhysicalModel.insert(masterDb, childSubjectId, "model-in-child-subject");
      const childSubjectChildId = Subject.insert(masterDb, childSubjectId, "child-subject-child");
      const modelInChildSubjectChildId = PhysicalModel.insert(masterDb, childSubjectChildId, "model-in-child-subject-child");
      masterDb.performCheckpoint();
      await masterDb.pushChanges({ accessToken, description: "setup master" });

      // create and initialize branch from master
      const branchIModelName = "RevSyncDeleteBranch";
      branchIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: branchIModelName, noLocks: true, version0: masterDb.pathName });
      assert.isTrue(Guid.isGuid(branchIModelId));
      const branchDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: branchIModelId });
      await branchDb.importSchemas([BisCoreSchema.schemaFilePath, GenericSchema.schemaFilePath]);
      assert.isTrue(branchDb.containsClass(ExternalSourceAspect.classFullName), "Expect BisCore to be updated and contain ExternalSourceAspect");
      const provenanceInitializer = new IModelTransformer(masterDb, branchDb, { wasSourceIModelCopiedToTarget: true });
      await provenanceInitializer.processSchemas();
      await provenanceInitializer.processAll();
      provenanceInitializer.dispose();
      branchDb.saveChanges();
      await branchDb.pushChanges({ accessToken, description: "setup branch" });

      const modelToDeleteWithElem = {
        entity: branchDb.models.getModel(modelToDeleteWithElemId),
        aspects: branchDb.elements.getAspects(modelToDeleteWithElemId),
      };
      const elemToDeleteWithChildren = {
        entity: branchDb.elements.getElement(elemToDeleteWithChildrenId),
        aspects: branchDb.elements.getAspects(elemToDeleteWithChildrenId),
      };
      const childElemOfDeleted = {
        aspects: branchDb.elements.getAspects(childElemOfDeletedId),
      };
      const elemInModelToDelete = {
        aspects: branchDb.elements.getAspects(elemInModelToDeleteId),
      };
      const childSubject = {
        entity: branchDb.elements.getElement(childSubjectId),
        aspects: branchDb.elements.getAspects(childSubjectId),
      };
      const modelInChildSubject = {
        entity: branchDb.models.getModel(modelInChildSubjectId),
        aspects: branchDb.elements.getAspects(modelInChildSubjectId),
      };
      const childSubjectChild = {
        entity: branchDb.elements.getElement(childSubjectChildId),
        aspects: branchDb.elements.getAspects(childSubjectChildId),
      };
      const modelInChildSubjectChild = {
        entity: branchDb.models.getModel(modelInChildSubjectChildId),
        aspects: branchDb.elements.getAspects(modelInChildSubjectChildId),
      };

      elemToDeleteWithChildren.entity.delete();
      modelToDeleteWithElem.entity.delete();
      deleteElementTree(branchDb, modelToDeleteWithElemId);
      deleteElementTree(branchDb, childSubjectId);
      branchDb.saveChanges();
      await branchDb.pushChanges({ accessToken, description: "branch deletes" });

      // verify the branch state
      expect(branchDb.models.tryGetModel(modelToDeleteWithElemId)).to.be.undefined;
      expect(branchDb.elements.tryGetElement(elemInModelToDeleteId)).to.be.undefined;
      expect(branchDb.models.tryGetModel(notDeletedModelId)).not.to.be.undefined;
      expect(branchDb.elements.tryGetElement(elemToDeleteWithChildrenId)).to.be.undefined;
      expect(branchDb.elements.tryGetElement(childElemOfDeletedId)).to.be.undefined;
      expect(branchDb.elements.tryGetElement(childSubjectId)).to.be.undefined;
      expect(branchDb.elements.tryGetElement(modelInChildSubjectId)).to.be.undefined;
      expect(branchDb.elements.tryGetElement(childSubjectChildId)).to.be.undefined;
      expect(branchDb.elements.tryGetElement(modelInChildSubjectChildId)).to.be.undefined;

      // expected extracted changed ids
      const branchDbChangesets = await IModelHost.hubAccess.downloadChangesets({ accessToken, iModelId: branchIModelId, targetDir: BriefcaseManager.getChangeSetsPath(branchIModelId) });
      expect(branchDbChangesets).to.have.length(2);
      const latestChangeset = branchDbChangesets[1];
      const extractedChangedIds = branchDb.nativeDb.extractChangedInstanceIdsFromChangeSets([latestChangeset.pathname]);
      const aspectDeletions = [
        ...modelToDeleteWithElem.aspects,
        ...childSubject.aspects,
        ...modelInChildSubject.aspects,
        ...childSubjectChild.aspects,
        ...modelInChildSubjectChild.aspects,
        ...elemInModelToDelete.aspects,
        ...elemToDeleteWithChildren.aspects,
        ...childElemOfDeleted.aspects,
      ].map((a) => a.id);

      const expectedChangedIds: IModelJsNative.ChangedInstanceIdsProps = {
        ...aspectDeletions.length > 0 && {
          aspect: {
            delete: aspectDeletions,
          },
        },
        element: {
          delete: [
            modelToDeleteWithElemId,
            elemInModelToDeleteId,
            elemToDeleteWithChildrenId,
            childElemOfDeletedId,
            childSubjectId,
            modelInChildSubjectId,
            childSubjectChildId,
            modelInChildSubjectChildId,
          ],
        },
        model: {
          update: [IModelDb.rootSubjectId, notDeletedModelId], // containing model will also get last modification time updated
          delete: [modelToDeleteWithElemId, modelInChildSubjectId, modelInChildSubjectChildId],
        },
      };
      expect(extractedChangedIds.result).to.deep.equal(expectedChangedIds);

      const synchronizer = new IModelTransformer(branchDb, masterDb, {
        // NOTE: not using a targetScopeElementId because this test deals with temporary dbs, but that is a bad practice, use one
        isReverseSynchronization: true,
      });
      await synchronizer.processChanges({ accessToken });
      branchDb.saveChanges();
      await branchDb.pushChanges({ accessToken, description: "synchronize" });
      synchronizer.dispose();

      const getFromTarget = (sourceEntityId: Id64String, type: "elem" | "model") => {
        const sourceEntity = masterDb.elements.tryGetElement(sourceEntityId);
        if (sourceEntity === undefined)
          return undefined;
        const codeVal = sourceEntity.code.value;
        assert(codeVal !== undefined, "all tested elements must have a code value");
        const targetId = IModelTransformerTestUtils.queryByCodeValue(masterDb, codeVal);
        if (Id64.isInvalid(targetId))
          return undefined;
        return type === "model"
          ? masterDb.models.tryGetModel(targetId)
          : masterDb.elements.tryGetElement(targetId);
      };

      // verify the master state
      expect(getFromTarget(modelToDeleteWithElemId, "model")).to.be.undefined;
      expect(getFromTarget(elemInModelToDeleteId, "elem")).to.be.undefined;
      expect(getFromTarget(notDeletedModelId, "model")).not.to.be.undefined;
      expect(getFromTarget(elemToDeleteWithChildrenId, "elem")).to.be.undefined;
      expect(getFromTarget(childElemOfDeletedId, "elem")).to.be.undefined;
      expect(getFromTarget(childSubjectId, "elem")).to.be.undefined;
      expect(getFromTarget(modelInChildSubjectId, "model")).to.be.undefined;
      expect(getFromTarget(childSubjectChildId, "elem")).to.be.undefined;
      expect(getFromTarget(modelInChildSubjectChildId, "model")).to.be.undefined;

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
    } finally {
      // delete iModel briefcases
      await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: masterIModelId });
      if (branchIModelId) {
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: branchIModelId });
      }
    }
  });

  it("should not download more changesets than necessary", async () => {
    const timeline: Timeline = {
      0: { master: { 1:1 } },
      1: { branch: { branch: "master" } },
      2: { branch: { 1:2, 2:1 } },
      3: { branch: { 3:3 } },
    };

    const { trackedIModels, timelineStates, tearDown } = await runTimeline(timeline, { iTwinId, accessToken });

    const master = trackedIModels.get("master")!;
    const branch = trackedIModels.get("branch")!;
    const branchAt2Changeset = timelineStates.get(1)?.changesets.branch;
    assert(branchAt2Changeset?.index);
    const branchAt2 = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: branch.id, asOf: { first: true } });
    await branchAt2.pullChanges({ toIndex: branchAt2Changeset.index, accessToken });

    const syncer = new IModelTransformer(branchAt2, master.db, {
      isReverseSynchronization: true,
    });
    const queryChangeset = sinon.spy(HubMock, "queryChangeset");
    await syncer.processChanges({ accessToken, startChangeset: branchAt2Changeset });
    expect(queryChangeset.alwaysCalledWith({
      accessToken,
      iModelId: branch.id,
      changeset: {
        id: branchAt2Changeset.id,
      },
    })).to.be.true;

    syncer.dispose();
    await tearDown();
    sinon.restore();
  });

  it("should reverse synchronize forked iModel when an element was updated", async () => {
    const sourceIModelName: string = IModelTransformerTestUtils.generateUniqueName("Master");
    const sourceIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: sourceIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string = IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: targetIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });

      const categoryId = SpatialCategory.insert(sourceDb, IModel.dictionaryId, "C1", {});
      const modelId = PhysicalModel.insert(sourceDb, IModel.rootSubjectId, "PM1");
      const physicalElement: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        userLabel: "Element1",
      };
      const originalElementId = sourceDb.elements.insertElement(physicalElement);

      sourceDb.saveChanges();
      await sourceDb.pushChanges({ description: "insert physical element" });

      let transformer = new IModelTransformer(sourceDb, targetDb);
      await transformer.processAll();
      const forkedElementId = transformer.context.findTargetElementId(originalElementId);
      expect(forkedElementId).not.to.be.undefined;
      transformer.dispose();
      targetDb.saveChanges();
      await targetDb.pushChanges({ description: "initial transformation" });

      const forkedElement = targetDb.elements.getElement(forkedElementId);
      forkedElement.userLabel = "Element1_updated";
      forkedElement.update();
      targetDb.saveChanges();
      await targetDb.pushChanges({ description: "update forked element's userLabel" });

      transformer = new IModelTransformer(targetDb, sourceDb, {isReverseSynchronization: true});
      await transformer.processChanges({startChangeset: targetDb.changeset});
      sourceDb.saveChanges();
      await sourceDb.pushChanges({ description: "change processing transformation" });

      const masterElement = sourceDb.elements.getElement(originalElementId);
      expect(masterElement).to.not.be.undefined;
      expect(masterElement.userLabel).to.be.equal("Element1_updated");
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should preserve FederationGuid when element is recreated", async () => {
    const sourceIModelName: string = IModelTransformerTestUtils.generateUniqueName("Source");
    const sourceIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: sourceIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string = IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: targetIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
    const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
    const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });

    const constSubjectFedGuid = Guid.createValue();
    const originalSubjectId = sourceDb.elements.insertElement({
      classFullName: Subject.classFullName,
      code: Code.createEmpty(),
      model: IModel.repositoryModelId,
      parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
      federationGuid: constSubjectFedGuid,
      userLabel: "A",
    });

    const constPartitionFedGuid = Guid.createValue();
    const originalPartitionId = sourceDb.elements.insertElement({
      model: IModel.repositoryModelId,
      code: PhysicalPartition.createCode(sourceDb, IModel.rootSubjectId, "original partition"),
      classFullName: PhysicalPartition.classFullName,
      federationGuid: constPartitionFedGuid,
      parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
    });
    const originalModelId = sourceDb.models.insertModel({
      classFullName: PhysicalModel.classFullName,
      modeledElement: {id: originalPartitionId},
      isPrivate: true,
    });

    sourceDb.saveChanges();
    await sourceDb.pushChanges({ description: "inserted elements & models" });

    let transformer = new IModelTransformer(sourceDb, targetDb);
    await transformer.processAll();
    transformer.dispose();
    targetDb.saveChanges();
    await targetDb.pushChanges({ description: "initial transformation" });

    const originalTargetElement = targetDb.elements.getElement<Subject>({federationGuid: constSubjectFedGuid}, Subject);
    expect(originalTargetElement?.userLabel).to.equal("A");
    const originalTargetPartition = targetDb.elements.getElement<PhysicalPartition>({federationGuid: constPartitionFedGuid}, PhysicalPartition);
    expect(originalTargetPartition.code.value).to.be.equal("original partition");
    const originalTargetModel = targetDb.models.getModel<PhysicalModel>(originalTargetPartition.id, PhysicalModel);
    expect(originalTargetModel.isPrivate).to.be.true;

    sourceDb.elements.deleteElement(originalSubjectId);
    sourceDb.elements.insertElement({
      classFullName: Subject.classFullName,
      code: Code.createEmpty(),
      model: IModel.repositoryModelId,
      parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
      federationGuid: constSubjectFedGuid,
      userLabel: "B",
    });

    sourceDb.models.deleteModel(originalModelId);
    sourceDb.elements.deleteElement(originalPartitionId);
    const recreatedPartitionId = sourceDb.elements.insertElement({
      model: IModel.repositoryModelId,
      code: PhysicalPartition.createCode(sourceDb, IModel.rootSubjectId, "recreated partition"),
      classFullName: PhysicalPartition.classFullName,
      federationGuid: constPartitionFedGuid,
      parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
    });
    sourceDb.models.insertModel({
      classFullName: PhysicalModel.classFullName,
      modeledElement: {id: recreatedPartitionId},
      isPrivate: false,
    });

    sourceDb.saveChanges();
    await sourceDb.pushChanges({ description: "recreated elements & models" });

    transformer = new IModelTransformer(sourceDb, targetDb);
    await transformer.processChanges({startChangeset: sourceDb.changeset});
    targetDb.saveChanges();
    await targetDb.pushChanges({ description: "change processing transformation" });

    const targetElement = targetDb.elements.getElement<Subject>({federationGuid: constSubjectFedGuid}, Subject);
    expect(targetElement?.userLabel).to.equal("B");
    const targetPartition = targetDb.elements.getElement<PhysicalPartition>({federationGuid: constPartitionFedGuid}, PhysicalPartition);
    expect(targetPartition.code.value).to.be.equal("recreated partition");
    const targetModel = targetDb.models.getModel<PhysicalModel>(targetPartition.id, PhysicalModel);
    expect(targetModel.isPrivate).to.be.false;

    expect(count(sourceDb, Subject.classFullName, `Parent.Id = ${IModel.rootSubjectId}`)).to.equal(1);
    expect(count(targetDb, Subject.classFullName, `Parent.Id = ${IModel.rootSubjectId}`)).to.equal(1);
    expect(count(sourceDb, PhysicalPartition.classFullName)).to.equal(1);
    expect(count(targetDb, PhysicalPartition.classFullName)).to.equal(1);
    expect(count(sourceDb, PhysicalModel.classFullName)).to.equal(1);
    expect(count(targetDb, PhysicalModel.classFullName)).to.equal(1);

    } finally {
      try {
        // delete iModel briefcases
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should delete model when its partition was recreated, but model was left deleted", async () => {
    const sourceIModelName: string = IModelTransformerTestUtils.generateUniqueName("Source");
    const sourceIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: sourceIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string = IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({ accessToken, iTwinId, iModelName: targetIModelName, noLocks: true });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
    const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
    const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });

    const constPartitionFedGuid = Guid.createValue();
    const originalPartitionId = sourceDb.elements.insertElement({
      model: IModel.repositoryModelId,
      code: PhysicalPartition.createCode(sourceDb, IModel.rootSubjectId, "original partition"),
      classFullName: PhysicalPartition.classFullName,
      federationGuid: constPartitionFedGuid,
      parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
    });
    const modelId = sourceDb.models.insertModel({
      classFullName: PhysicalModel.classFullName,
      modeledElement: {id: originalPartitionId},
      isPrivate: true,
    });

    sourceDb.saveChanges();
    await sourceDb.pushChanges({ description: "inserted elements & models" });

    let transformer = new IModelTransformer(sourceDb, targetDb);
    await transformer.processAll();
    transformer.dispose();
    targetDb.saveChanges();
    await targetDb.pushChanges({ description: "initial transformation" });

    const originalTargetPartition = targetDb.elements.getElement<PhysicalPartition>({federationGuid: constPartitionFedGuid}, PhysicalPartition);
    expect(originalTargetPartition.code.value).to.be.equal("original partition");
    const originalTargetModel = targetDb.models.getModel<PhysicalModel>(originalTargetPartition.id, PhysicalModel);
    expect(originalTargetModel.isPrivate).to.be.true;

    sourceDb.models.deleteModel(modelId);
    sourceDb.elements.deleteElement(originalPartitionId);
    sourceDb.elements.insertElement({
      model: IModel.repositoryModelId,
      code: PhysicalPartition.createCode(sourceDb, IModel.rootSubjectId, "recreated partition"),
      classFullName: PhysicalPartition.classFullName,
      federationGuid: constPartitionFedGuid,
      parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
    });

    sourceDb.saveChanges();
    await sourceDb.pushChanges({ description: "recreated elements & models" });

    transformer = new IModelTransformer(sourceDb, targetDb);
    await transformer.processChanges({startChangeset: sourceDb.changeset});
    targetDb.saveChanges();
    await targetDb.pushChanges({ description: "change processing transformation" });

    const targetPartition = targetDb.elements.getElement<PhysicalPartition>({federationGuid: constPartitionFedGuid}, PhysicalPartition);
    expect(targetPartition.code.value).to.be.equal("recreated partition");

    expect(count(sourceDb, PhysicalPartition.classFullName)).to.equal(1);
    expect(count(targetDb, PhysicalPartition.classFullName)).to.equal(1);
    expect(count(sourceDb, PhysicalModel.classFullName)).to.equal(0);
    expect(count(targetDb, PhysicalModel.classFullName)).to.equal(0);

    } finally {
      try {
        // delete iModel briefcases
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should update aspects when processing changes and detachedAspectProcessing is turned on", async () => {
    let elementIds: Id64String[] = [];
    const aspectIds: Id64String[] = [];
    const sourceIModelId = await createPopulatedIModelHubIModel("TransformerSource", (sourceSeedDb) => {
      elementIds = [
        Subject.insert(sourceSeedDb, IModel.rootSubjectId, "Subject1"),
        Subject.insert(sourceSeedDb, IModel.rootSubjectId, "Subject2"),
      ];

      // 10 aspects in total (5 per element)
      elementIds.forEach((element) => {
        for (let i = 0; i < 5; ++i) {
          const aspectProps: ExternalSourceAspectProps = {
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(element),
            identifier: `${i}`,
            kind: "Document",
            scope: { id: IModel.rootSubjectId, relClassName: "BisCore:ElementScopesExternalSourceIdentifier" },
          };

          const aspectId = sourceSeedDb.elements.insertAspect(aspectProps);
          aspectIds.push(aspectId); // saving for later deletion
        }
      });
    });

    const targetIModelId = await createPopulatedIModelHubIModel("TransformerTarget");

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: sourceIModelId });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: targetIModelId });

      const exporter = new IModelExporter(sourceDb, DetachedExportElementAspectsStrategy);
      const transformer = new IModelTransformer(exporter, targetDb, {
        includeSourceProvenance: true,
      });

      // run first transformation
      await transformer.processChanges({ accessToken });
      await saveAndPushChanges(targetDb, "First transformation");

      const addedAspectProps: ExternalSourceAspectProps = {
        classFullName: ExternalSourceAspect.classFullName,
        element: new ElementOwnsExternalSourceAspects(elementIds[0]),
        identifier: `aspectAddedAfterFirstTransformation`,
        kind: "Document",
        scope: { id: IModel.rootSubjectId, relClassName: "BisCore:ElementScopesExternalSourceIdentifier" },
      };
      sourceDb.elements.insertAspect(addedAspectProps);

      await saveAndPushChanges(sourceDb, "Update source");

      await transformer.processChanges({ accessToken, startChangeset: sourceDb.changeset });
      await saveAndPushChanges(targetDb, "Second transformation");

      const targetElementIds = targetDb.queryEntityIds({ from: Subject.classFullName, where: "Parent.Id != ?", bindings: [IModel.rootSubjectId] });
      targetElementIds.forEach((elementId) => {
        const targetAspects = targetDb.elements.getAspects(elementId, ExternalSourceAspect.classFullName) as ExternalSourceAspect[];
        const sourceAspects = sourceDb.elements.getAspects(elementId, ExternalSourceAspect.classFullName) as ExternalSourceAspect[];
        expect(targetAspects.length).to.be.equal(sourceAspects.length + 1); // +1 because provenance aspect was added
        const aspectAddedAfterFirstTransformation = targetAspects.find((aspect) => aspect.identifier === "aspectAddedAfterFirstTransformation");
        expect(aspectAddedAfterFirstTransformation).to.not.be.undefined;
      });
    } finally {
      await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: sourceIModelId });
      await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: targetIModelId });
    }
  });

  // will fix in separate PR, tracked here: https://github.com/iTwin/imodel-transformer/issues/27
  it.skip("should delete definition elements when processing changes", async () => {
    let spatialViewDef: SpatialViewDefinition;
    let displayStyle: DisplayStyle3d;

    const timeline: Timeline = {
      0: {
        master: {
          manualUpdate(db) {
            const modelSelectorId = ModelSelector.create(db, IModelDb.dictionaryId, "modelSelector", []).insert();
            const categorySelectorId = CategorySelector.insert(db, IModelDb.dictionaryId, "categorySelector", []);
            displayStyle = DisplayStyle3d.create(db, IModelDb.dictionaryId, "displayStyle");
            const displayStyleId = displayStyle.insert();
            db.elements.insertElement({
              classFullName: SpatialViewDefinition.classFullName,
              model: IModelDb.dictionaryId,
              code: Code.createEmpty().toJSON(),
              camera: {
                eye: { x: 0, y: 0, z: 0 },
                lens: { radians: 0 },
                focusDist: 0,
              },
              userLabel: "spatialViewDef",
              extents: { x: 0, y: 0, z: 0 },
              origin: { x: 0, y: 0, z: 0 },
              cameraOn: false,
              displayStyleId,
              categorySelectorId,
              modelSelectorId,
            } as SpatialViewDefinitionProps);
          },
        },
      },
      1: { branch: { branch: "master" } },
      2: {
        master: {
          manualUpdate(db) {
            const notDeleted = db.elements.deleteDefinitionElements([spatialViewDef.id, displayStyle.id]);
            assert(notDeleted.size === 0);
          },
        },
      },
      3: { branch: { sync: ["master", { since: 2 }] } },
    };

    const { trackedIModels, tearDown } = await runTimeline(timeline, { iTwinId, accessToken });

    const master = trackedIModels.get("master")!;
    const branch = trackedIModels.get("branch")!;

    expect(master.db.elements.tryGetElement(spatialViewDef!.code)).to.be.undefined;
    expect(master.db.elements.tryGetElement(displayStyle!.code)).to.be.undefined;

    expect(branch.db.elements.tryGetElement(spatialViewDef!.code)).to.be.undefined;
    expect(branch.db.elements.tryGetElement(displayStyle!.code)).to.be.undefined;

    await tearDown();
    sinon.restore();
  });

  it("should be able to handle a transformation which deletes a relationship and then elements of that relationship", async () => {
    const masterIModelName = "MasterDeleteRelAndEnds";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = {40:1, 2:2, 41:3, 42:4} as TimelineIModelElemState;
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, { rootSubject: { name: masterIModelName } });
    masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);

    const noFedGuidElemIds = masterSeedDb.queryEntityIds({ from: "Bis.Element", where: "UserLabel IN ('41', '42')" });
    for (const elemId of noFedGuidElemIds)
      masterSeedDb.withSqliteStatement(
        `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
        (s) => { expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE); }
      );
    masterSeedDb.performCheckpoint();

    // hard to check this without closing the db...
    const seedSecondConn = SnapshotDb.openFile(masterSeedDb.pathName);
    for (const elemId of noFedGuidElemIds)
      expect(seedSecondConn.elements.getElement(elemId).federationGuid).to.be.undefined;
    seedSecondConn.close();

    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };

    const expectedRelationships = [
      { sourceLabel: "40", targetLabel: "2", idInBranch: "not inserted yet", sourceFedGuid: true, targetFedGuid: true },
      { sourceLabel: "41", targetLabel: "42", idInBranch: "not inserted yet", sourceFedGuid: false, targetFedGuid: false },
    ];

    let aspectIdForRelationship: Id64String | undefined;
    const timeline: Timeline = [
      { master: { seed: masterSeed } },
      { branch: { branch: "master" } },
      {
        branch: {
          manualUpdate(db) {
            expectedRelationships.map(
              ({ sourceLabel, targetLabel }, i) => {
                const sourceId = IModelTestUtils.queryByUserLabel(db, sourceLabel);
                const targetId = IModelTestUtils.queryByUserLabel(db, targetLabel);
                assert(sourceId && targetId);
                const rel = ElementGroupsMembers.create(db, sourceId, targetId, 0);
                expectedRelationships[i].idInBranch = rel.insert();
              }
            );
          },
        },
      },
      { master: { sync: ["branch"] } }, // first master<-branch reverse sync
      {
        assert({branch}) {
          // expectedRelationships[1] has no fedguids, so expect to find 2 esas. One for the relationship and one for the element's own provenance.
          const sourceId = IModelTestUtils.queryByUserLabel(branch.db, expectedRelationships[1].sourceLabel);
          const aspects = branch.db.elements.getAspects(sourceId, ExternalSourceAspect.classFullName) as ExternalSourceAspect[];
          assert(aspects.length === 2);
          let foundElementEsa = false;
          for (const aspect of aspects) {
            if (aspect.kind === "Element")
              foundElementEsa = true;
            else if (aspect.kind === "Relationship")
              aspectIdForRelationship = aspect.id;
          }
          assert(aspectIdForRelationship && Id64.isValid(aspectIdForRelationship) && foundElementEsa);
        },
      },
      {
        master: {
          manualUpdate(db) {
            expectedRelationships.forEach(
              ({ sourceLabel, targetLabel }) => {
                const sourceId = IModelTestUtils.queryByUserLabel(db, sourceLabel);
                const targetId = IModelTestUtils.queryByUserLabel(db, targetLabel);
                assert(sourceId && targetId);
                const rel = db.relationships.getInstance(
                  ElementGroupsMembers.classFullName,
                  { sourceId, targetId }
                );
                rel.delete();
                db.elements.deleteElement(sourceId);
                db.elements.deleteElement(targetId);
              }
            );
          },
        },
      },
      { branch: { sync: ["master"]}}, // master->branch forward sync
      {
        assert({branch}) {
          for (const rel of expectedRelationships) {
            expect(branch.db.relationships.tryGetInstance(
              ElementGroupsMembers.classFullName,
              rel.idInBranch,
            ), `had ${rel.sourceLabel}->${rel.targetLabel}`).to.be.undefined;
            const sourceId = IModelTestUtils.queryByUserLabel(branch.db, rel.sourceLabel);
            const targetId = IModelTestUtils.queryByUserLabel(branch.db, rel.targetLabel);
            // Since we deleted both elements in the previous manualUpdate
            assert(Id64.isInvalid(sourceId) && Id64.isInvalid(targetId), `SourceId is ${sourceId}, TargetId is ${targetId}. Expected both to be ${Id64.invalid}.`);
            expect(() => branch.db.relationships.tryGetInstance(
              ElementGroupsMembers.classFullName,
              { sourceId, targetId },
            ), `had ${rel.sourceLabel}->${rel.targetLabel}`).to.throw; // TODO: This shouldn't throw but it does in core due to failing to bind ids of 0.

            expect(() => branch.db.elements.getAspect(aspectIdForRelationship!)).to.throw("not found", `Expected aspectId: ${aspectIdForRelationship} to no longer be present in branch imodel.`);
          }
        },
      },
    ];
    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    await tearDown();
    masterSeedDb.close();
  });

  // FIXME: As a side effect of fixing a bug in findRangeContaining, we error out with no changesummary data because we now properly skip changesetindices
  // i.e. a range [4,4] with skip 4 now properly gets skipped. so we have no changesummary data. We need to revisit this after switching to affan's new API
  // to read changesets directly.
  it.skip("should skip provenance changesets made to branch during reverse sync", async () => {
    const timeline: Timeline = [
      { master: { 1:1 } },
      { master: { 2:2 } },
      { master: { 3:1 } },
      { branch: { branch: "master" } },
      { branch: { 1:2, 4:1 } },
      // eslint-disable-next-line @typescript-eslint/no-shadow
      { assert({ master, branch }) {
        expect(master.db.changeset.index).to.equal(3);
        expect(branch.db.changeset.index).to.equal(2);
        expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(0);
        expect(count(branch.db, ExternalSourceAspect.classFullName)).to.equal(9);

        const scopeProvenanceCandidates = branch.db.elements
          .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
          .filter((a) => (a as ExternalSourceAspect).identifier === master.db.iModelId);
        expect(scopeProvenanceCandidates).to.have.length(1);
        const targetScopeProvenance = scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

        expect(targetScopeProvenance).to.deep.subsetEqual({
          identifier: master.db.iModelId,
          version: `${master.db.changeset.id};${master.db.changeset.index}`,
          jsonProperties: JSON.stringify({
            pendingReverseSyncChangesetIndices: [],
            pendingSyncChangesetIndices: [],
            reverseSyncVersion: ";0", // not synced yet
          }),
        } as ExternalSourceAspectProps);
      }},
      { master: { sync: ["branch"] } },
      // eslint-disable-next-line @typescript-eslint/no-shadow
      { assert({ master, branch }) {
        expect(master.db.changeset.index).to.equal(4);
        expect(branch.db.changeset.index).to.equal(3);
        expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(0);
        // added because the root was modified
        expect(count(branch.db, ExternalSourceAspect.classFullName)).to.equal(11);

        const scopeProvenanceCandidates = branch.db.elements
          .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
          .filter((a) => (a as ExternalSourceAspect).identifier === master.db.iModelId);
        expect(scopeProvenanceCandidates).to.have.length(1);
        const targetScopeProvenance = scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

        expect(targetScopeProvenance.version).to.match(/;3$/);
        const targetScopeJsonProps = JSON.parse(targetScopeProvenance.jsonProperties);
        expect(targetScopeJsonProps).to.deep.subsetEqual({
          pendingReverseSyncChangesetIndices: [3],
          pendingSyncChangesetIndices: [4],
        });
        expect(targetScopeJsonProps.reverseSyncVersion).to.match(/;2$/);
      }},
      { branch: { sync: ["master"] } },
      { branch: { 5:1 } },
      { master: { sync: ["branch"] } },
      { assert({ master, branch }) {
        const expectedState = { 1:2, 2:2, 3:1, 4:1, 5:1 };
        expect(master.state).to.deep.equal(expectedState);
        expect(branch.state).to.deep.equal(expectedState);
        assertElemState(master.db, expectedState);
        assertElemState(branch.db, expectedState);
      }},
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
      transformerOpts: {
        // force aspects so that reverse sync has to edit the target
        forceExternalSourceAspectProvenance: true,
      },
    });

    await tearDown();
  });

  it("should successfully remove element in master iModel after reverse synchronization when elements have random ExternalSourceAspects", async () => {
    const timeline: Timeline = [
      { master: { 1:1 } },
      { master: { manualUpdate(masterDb) {
        const elemId = IModelTestUtils.queryByUserLabel(masterDb, "1");
        masterDb.elements.insertAspect({
          classFullName: ExternalSourceAspect.classFullName,
          element: { id: elemId },
          scope: { id: IModel.dictionaryId },
          kind: "Element",
          identifier: "bar code",
        } as ExternalSourceAspectProps);
      }}},
      { branch: { branch: "master" } },
      { branch: { 1:deleted } },
      { master: { sync: ["branch"]} },
      { assert({ master, branch }) {
        for (const imodel of [branch, master]) {
          const elemId = IModelTestUtils.queryByUserLabel(imodel.db, "1");
          const name = imodel.id === master.id ? "master" : "branch";
          expect(elemId, `db ${name} did not delete ${elemId}`).to.equal(Id64.invalid);
        }
      }},
    ];

    const { tearDown } = await runTimeline(timeline, { iTwinId, accessToken });
    await tearDown();
  });

  it("should delete definition elements and models when processing changes", async () => {
    let definitionPartitionId1: string;
    let definitionPartitionModelId1: string;
    let definitionPartitionId2: string;
    let definitionPartitionModelId2: string;
    let definitionContainerId1: string;
    let definitionContainerModelId1: string;

    const timeline: Timeline = {
      0: {
        master: {
          manualUpdate(db) {
            const definitionPartitionProps: InformationPartitionElementProps = {
              classFullName: DefinitionPartition.classFullName,
              model: IModel.repositoryModelId,
              parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
              code: Code.createEmpty(),
            };
            definitionPartitionId1 = db.elements.insertElement(definitionPartitionProps);
            definitionPartitionId2 = db.elements.insertElement(definitionPartitionProps);

            const definitionModelProps1: ModelProps = {
              classFullName: DefinitionModel.classFullName,
              modeledElement: { id: definitionPartitionId1 },
              parentModel: IModel.repositoryModelId,
            };
            definitionPartitionModelId1 = db.models.insertModel(definitionModelProps1);

            const definitionModelProps2: ModelProps = {
              classFullName: DefinitionModel.classFullName,
              modeledElement: { id: definitionPartitionId2 },
              parentModel: IModel.repositoryModelId,
            };
            definitionPartitionModelId2 = db.models.insertModel(definitionModelProps2);

            const definitionContainerProps1: DefinitionElementProps = {
              classFullName: DefinitionContainer.classFullName,
              model: definitionPartitionModelId1,
              code: Code.createEmpty(),
            };
            definitionContainerId1 = db.elements.insertElement(definitionContainerProps1);

            const definitionModelProps3: ModelProps = {
              classFullName: DefinitionModel.classFullName,
              modeledElement: { id: definitionContainerId1 },
              parentModel: definitionPartitionModelId1,
            };
            definitionContainerModelId1 = db.models.insertModel(definitionModelProps3);
          },
        },
      },
      1: { branch: { branch: "master" } },
      2: {
        master: {
          manualUpdate(db) {
            db.models.deleteModel(definitionContainerModelId1);
            db.models.deleteModel(definitionPartitionModelId1);
          },
        },
      },
      3: { branch: { sync: ["master"] } },
      4: {
        master: {
          manualUpdate(db) {
            db.models.deleteModel(definitionPartitionModelId2);
          },
        },
      },
      5: { branch: { sync: ["master"] } },
    };

    const { trackedIModels, tearDown } = await runTimeline(timeline, { iTwinId, accessToken });

    const master = trackedIModels.get("master")!;
    const branch = trackedIModels.get("branch")!;

    expect(master.db.models.tryGetModel(definitionContainerModelId1!)).to.be.undefined;
    expect(master.db.elements.tryGetElement(definitionContainerId1!)).to.be.undefined;
    expect(master.db.models.tryGetModel(definitionPartitionModelId1!)).to.be.undefined;
    expect(master.db.elements.tryGetElement(definitionPartitionId2!)).to.not.be.undefined;
    expect(master.db.models.tryGetModel(definitionPartitionModelId2!)).to.be.undefined;

    expect(branch.db.models.tryGetModel(definitionContainerModelId1!)).to.be.undefined;
    expect(branch.db.elements.tryGetElement(definitionContainerId1!)).to.be.undefined;
    expect(branch.db.models.tryGetModel(definitionPartitionModelId1!)).to.be.undefined;
    expect(branch.db.elements.tryGetElement(definitionPartitionId2!)).to.not.be.undefined;
    expect(branch.db.models.tryGetModel(definitionPartitionModelId2!)).to.be.undefined;

    await tearDown();
    sinon.restore();
  });

  it("should use the lastMod of provenanceDb's element as the provenance aspect version", async () => {
    const timeline: Timeline = [
      { master: { 1:1 } },
      { branch: { branch: "master" } },
      { branch: { 1:2 } },
      { master: { sync: ["branch"] } },
      { assert({ master, branch }) {
        const elem1InMaster = TestUtils.IModelTestUtils.queryByUserLabel(master.db, "1");
        expect(elem1InMaster).not.to.be.undefined;
        const elem1InBranch = TestUtils.IModelTestUtils.queryByUserLabel(branch.db, "1");
        expect(elem1InBranch).not.to.be.undefined;
        const lastModInMaster = master.db.elements.queryLastModifiedTime(elem1InMaster);

        const physElem1Esas = branch.db.elements.getAspects(elem1InBranch, ExternalSourceAspect.classFullName) as ExternalSourceAspect[];
        expect(physElem1Esas).to.have.lengthOf(1);
        expect(physElem1Esas[0].version).to.equal(lastModInMaster);
      }},
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
      transformerOpts: { forceExternalSourceAspectProvenance: true },
    });

    await tearDown();
    sinon.restore();
  });
});
