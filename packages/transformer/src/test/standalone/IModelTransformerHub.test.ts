/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "chai";
import * as path from "path";
import * as semver from "semver";
import {
  BisCoreSchema,
  BriefcaseDb,
  BriefcaseManager,
  CategorySelector,
  DefinitionContainer,
  DefinitionModel,
  DefinitionPartition,
  deleteElementTree,
  DisplayStyle3d,
  DocumentListModel,
  Drawing,
  DrawingModel,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  ElementOwnsChildElements,
  ElementOwnsExternalSourceAspects,
  ElementRefersToElements,
  ExternalSourceAspect,
  GenericSchema,
  GeometricModel,
  IModelDb,
  IModelHost,
  IModelJsFs,
  ModelSelector,
  NativeLoggerCategory,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  PhysicalType,
  SnapshotDb,
  SpatialCategory,
  SpatialViewDefinition,
  Subject,
  SubjectOwnsPartitionElements,
  SubjectOwnsSubjects,
  withEditTxn,
} from "@itwin/core-backend";
import { _hubAccess } from "@itwin/core-backend/lib/cjs/internal/Symbols";
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock";
import * as TestUtils from "../TestUtils";
import {
  AccessToken,
  DbResult,
  Guid,
  GuidString,
  Id64,
  Id64Array,
  Id64String,
  Logger,
  LogLevel,
} from "@itwin/core-bentley";
import {
  BisCodeSpec,
  Code,
  ColorDef,
  DefinitionElementProps,
  ElementProps,
  ExternalSourceAspectProps,
  GeometricElementProps,
  IModel,
  IModelError,
  IModelVersion,
  InformationPartitionElementProps,
  ModelProps,
  PhysicalElementProps,
  Placement3d,
  SpatialViewDefinitionProps,
  SubCategoryAppearance,
  SubjectProps,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import {
  ChangedInstanceIds,
  IModelExporter,
  IModelImporter,
  IModelTransformer,
  IModelTransformOptions,
  ProcessChangesOptions,
  TransformerLoggerCategory,
} from "../../imodel-transformer";
import { ProvenanceManager } from "../../ProvenanceManager";
import {
  CountingIModelImporter,
  createStartedEditTxn,
  HubWrappers,
  IModelToTextFileExporter,
  IModelTransformerTestUtils,
  PhysicalModelConsolidator,
  TestIModelTransformer,
  TransformerExtensiveTestScenario as TransformerExtensiveTestScenario,
} from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils";

import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests
import * as sinon from "sinon";
import {
  assertElemState,
  deleted,
  populateTimelineSeed,
  runTimeline,
  Timeline,
  TimelineIModelElemState,
  TimelineIModelState,
} from "../TestUtils/TimelineTestUtil";
import { DetachedExportElementAspectsStrategy } from "../../DetachedExportElementAspectsStrategy";

const { count } = IModelTestUtils;
const countElementExternalSourceAspects = (
  db: IModelDb,
  elementId: Id64String
) =>
  db.elements
    .getAspects(elementId, ExternalSourceAspect.classFullName)
    .filter(
      (aspect) =>
        (aspect as ExternalSourceAspect).kind ===
        ExternalSourceAspect.Kind.Element
    ).length;

describe("IModelTransformerHub", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "IModelTransformerHub"
  );
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  let saveAndPushChanges: (db: BriefcaseDb, desc: string) => Promise<void>;

  before(async () => {
    HubMock.startup("IModelTransformerHub", KnownTestLocations.outputDir);
    iTwinId = HubMock.iTwinId;
    IModelJsFs.recursiveMkDirSync(outputDir);

    accessToken = await HubWrappers.getAccessToken(
      TestUtils.TestUserType.Regular
    );

    saveAndPushChanges = IModelTestUtils.saveAndPushChanges.bind(
      IModelTestUtils,
      accessToken
    );

    // initialize logging
    if (process.env.TRANSFORMER_TESTS_USE_LOG) {
      Logger.initializeToConsole();
      Logger.setLevelDefault(LogLevel.Error);
      Logger.setLevel(TransformerLoggerCategory.IModelExporter, LogLevel.Trace);
      Logger.setLevel(TransformerLoggerCategory.IModelImporter, LogLevel.Trace);
      Logger.setLevel(
        TransformerLoggerCategory.IModelTransformer,
        LogLevel.Trace
      );
      Logger.setLevel(NativeLoggerCategory.Changeset, LogLevel.Trace);
    }
  });
  after(() => HubMock.shutdown());

  const createPopulatedIModelHubIModel = async (
    iModelName: string,
    prepareIModel?: (iModel: SnapshotDb) => void | Promise<void>
  ): Promise<string> => {
    // Create and push seed of IModel
    const seedFileName = path.join(outputDir, `${iModelName}.bim`);
    if (IModelJsFs.existsSync(seedFileName))
      IModelJsFs.removeSync(seedFileName);

    const seedDb = SnapshotDb.createEmpty(seedFileName, {
      rootSubject: { name: iModelName },
    });
    assert.isTrue(IModelJsFs.existsSync(seedFileName));
    await prepareIModel?.(seedDb);
    seedDb.close();

    const iModelId = await IModelHost[_hubAccess].createNewIModel({
      iTwinId,
      iModelName,
      description: "source",
      version0: seedFileName,
      noLocks: true,
    });
    return iModelId;
  };

  it("save reverse sync version for processAll transformations", async () => {
    const sourceIModelId = await HubWrappers.createIModel(
      accessToken,
      iTwinId,
      "source"
    );

    const targetIModelId = await HubWrappers.createIModel(
      accessToken,
      iTwinId,
      "target"
    );
    assert.isTrue(Guid.isGuid(sourceIModelId));
    assert.isTrue(Guid.isGuid(targetIModelId));
    try {
      // download and open briefcase on source imodel
      const sourceBriefcase = await HubWrappers.downloadAndOpenBriefcase({
        accessToken: await IModelHost.getAccessToken(),
        iTwinId,
        iModelId: sourceIModelId,
        asOf: IModelVersion.latest().toJSON(),
      });
      await sourceBriefcase.locks.acquireLocks({
        shared: "0x10",
        exclusive: "0x1",
      });
      assert.isTrue(sourceBriefcase.isBriefcaseDb());
      assert.isFalse(sourceBriefcase.isSnapshot);

      // set up physical models
      const { sourceModelId0, sourceModelId1 } = withEditTxn(
        sourceBriefcase,
        "insert physical models M0 and M1",
        (txn) => ({
          sourceModelId0: PhysicalModel.insert(txn, IModel.rootSubjectId, "M0"),
          sourceModelId1: PhysicalModel.insert(txn, IModel.rootSubjectId, "M1"),
        })
      );
      assert.isDefined(sourceModelId0);
      assert.isDefined(sourceModelId1);

      await sourceBriefcase.pushChanges({
        description: "source changes for inserting physical elements M0 and M1",
        retainLocks: true,
      });

      // download and open briefcase on target imodel
      const targetBriefcase = await HubWrappers.downloadAndOpenBriefcase({
        accessToken: await IModelHost.getAccessToken(),
        iTwinId,
        iModelId: targetIModelId,
        asOf: IModelVersion.latest().toJSON(),
      });
      assert.isTrue(targetBriefcase.isBriefcaseDb());
      assert.isFalse(targetBriefcase.isSnapshot);

      await targetBriefcase.locks.acquireLocks({
        shared: "0x10",
        exclusive: "0x1",
      });

      // we do not expect to save reverse sync version by default for processAll transformations
      const targetEditTxn1 = createStartedEditTxn(targetBriefcase);
      const transformer1 = new IModelTransformer({
        source: sourceBriefcase,
        target: targetEditTxn1,
      });
      await transformer1.initialize();
      await transformer1.updateSynchronizationVersion();
      const scopeEsaBeforeProcess =
        await ProvenanceManager.queryScopeExternalSourceAspect(
          targetBriefcase,
          {
            id: undefined,
            classFullName: ExternalSourceAspect.classFullName,
            scope: { id: IModel.rootSubjectId },
            kind: ExternalSourceAspect.Kind.Scope,
            element: { id: IModel.rootSubjectId },
            identifier: sourceBriefcase.iModelId,
          }
        );
      assert.equal(
        scopeEsaBeforeProcess?.version,
        "",
        "initialization without processing should not update the synchronization version"
      );
      await transformer1.process();
      const scopeEsaResult1 =
        await ProvenanceManager.queryScopeExternalSourceAspect(
          targetBriefcase,
          {
            id: undefined,
            classFullName: ExternalSourceAspect.classFullName,
            scope: { id: IModel.rootSubjectId },
            kind: ExternalSourceAspect.Kind.Scope,
            element: { id: IModel.rootSubjectId },
            identifier: sourceBriefcase.iModelId,
          }
        );
      const jsonProps1 = JSON.parse(scopeEsaResult1?.jsonProperties ?? "{}");
      assert.isEmpty(jsonProps1.reverseSyncVersion ?? "");
      targetEditTxn1.end();
      await targetBriefcase.pushChanges({
        description: "target changes for transformation 1",
        retainLocks: true,
      });

      const sourceModelId2 = withEditTxn(
        sourceBriefcase,
        "insert physical model M2",
        (txn) => PhysicalModel.insert(txn, IModel.rootSubjectId, "M2")
      );
      assert.isDefined(sourceModelId2);
      await sourceBriefcase.pushChanges({
        description: "source changes for inserting physical elements M2",
        retainLocks: true,
      });

      // when initializeReverseSyncVersion is set to true, we expect to save reverse sync version
      const targetEditTxn2 = createStartedEditTxn(targetBriefcase);
      const transformer2 = new IModelTransformer({
        source: sourceBriefcase,
        target: targetEditTxn2,
      });
      await transformer2.process();
      await transformer2.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      const scopeEsaResult2 =
        await ProvenanceManager.queryScopeExternalSourceAspect(
          targetBriefcase,
          {
            id: undefined,
            classFullName: ExternalSourceAspect.classFullName,
            scope: { id: IModel.rootSubjectId },
            kind: ExternalSourceAspect.Kind.Scope,
            element: { id: IModel.rootSubjectId },
            identifier: sourceBriefcase.iModelId,
          }
        );
      const jsonProps2 = JSON.parse(scopeEsaResult2?.jsonProperties ?? "{}");
      const reverseSyncVersion2 = jsonProps2.reverseSyncVersion;
      assert.isNotEmpty(reverseSyncVersion2);
      const expectedReverseSyncVersion1 = `${targetBriefcase.changeset.id};${targetBriefcase.changeset.index}`;
      assert.equal(reverseSyncVersion2, expectedReverseSyncVersion1);
      // the recently pushed PendingReverseSync index should be equal to the latest target changeset index + 1
      const lastPendingReverseSyncIndex1 =
        jsonProps2.pendingReverseSyncChangesetIndices?.pop();
      assert.equal(
        lastPendingReverseSyncIndex1,
        (targetBriefcase.changeset.index ?? 0) + 1
      );
      targetEditTxn2.end();
      await targetBriefcase.pushChanges({
        description: "target changes for transformation 2",
        retainLocks: true,
      });
    } finally {
      try {
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should handle sequential deletes after processAll with default processChanges options", async () => {
    const sourceIModelId = await createPopulatedIModelHubIModel(
      IModelTransformerTestUtils.generateUniqueName(
        "ProcessChangesDeletesSource"
      )
    );
    const targetIModelId = await createPopulatedIModelHubIModel(
      IModelTransformerTestUtils.generateUniqueName(
        "ProcessChangesDeletesTarget"
      )
    );
    let sourceDb: BriefcaseDb | undefined;
    let targetDb: BriefcaseDb | undefined;

    try {
      sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
      await sourceDb.locks.acquireLocks({
        shared: "0x10",
        exclusive: "0x1",
      });
      await targetDb.locks.acquireLocks({
        shared: "0x10",
        exclusive: "0x1",
      });

      const [physicalElement1Id, physicalElement2Id] = withEditTxn(
        sourceDb,
        "insert source physical elements",
        (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "SourceModel"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "SourceCategory",
            {}
          );
          const insertPhysicalElement = (name: string) => {
            const element: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category: categoryId,
              code: new Code({ scope: "0x1", spec: "0x1", value: name }),
              userLabel: name,
            };
            return txn.insertElement(element);
          };
          return [
            insertPhysicalElement("PhysicalOne"),
            insertPhysicalElement("PhysicalTwo"),
          ];
        }
      );
      await sourceDb.pushChanges({
        accessToken,
        description: "Initial source data",
        retainLocks: true,
      });

      const processAllEditTxn = createStartedEditTxn(targetDb);
      const processAllTransformer = new IModelTransformer({
        source: sourceDb,
        target: processAllEditTxn,
      });
      await processAllTransformer.process();
      const syncVersionAfterProcessAll =
        await processAllTransformer[
          "_provenanceManager"
        ].getSynchronizationVersion();
      expect(syncVersionAfterProcessAll.index).to.equal(
        sourceDb.changeset.index,
        "processAll should persist the source synchronization version"
      );
      processAllTransformer.dispose();
      processAllEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Initial processAll transformation",
        retainLocks: true,
      });

      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "PhysicalOne")
      ).to.not.be.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "PhysicalTwo")
      ).to.not.be.equal(Id64.invalid);

      const processChanges = async (description: string) => {
        const editTxn = createStartedEditTxn(targetDb!);
        const transformer = new IModelTransformer(
          { source: sourceDb!, target: editTxn },
          { argsForProcessChanges: {} }
        );
        await transformer.process();
        transformer.dispose();
        editTxn.end();
        await targetDb!.pushChanges({
          accessToken,
          description,
          retainLocks: true,
        });
      };

      const deleteAndProcess = async (elementId: Id64String, name: string) => {
        withEditTxn(
          sourceDb!,
          `delete ${name} source physical element`,
          (txn) => {
            txn.deleteElement(elementId);
          }
        );
        await sourceDb!.pushChanges({
          accessToken,
          description: `Delete ${name} source element`,
          retainLocks: true,
        });
        await processChanges(`Process ${name} source deletion`);
      };

      await deleteAndProcess(physicalElement1Id, "first");
      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "PhysicalOne"),
        "PhysicalOne should be deleted after the first processChanges"
      ).to.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "PhysicalTwo")
      ).to.not.be.equal(Id64.invalid);

      await deleteAndProcess(physicalElement2Id, "second");
      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "PhysicalTwo"),
        "PhysicalTwo should be deleted after the second processChanges"
      ).to.equal(Id64.invalid);
    } finally {
      if (sourceDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      if (targetDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
      await IModelHost[_hubAccess].deleteIModel({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      await IModelHost[_hubAccess].deleteIModel({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
    }
  });

  it("Transform source iModel to target iModel", async () => {
    const sourceIModelId = await createPopulatedIModelHubIModel(
      "TransformerSource",
      async (sourceSeedDb) => {
        await TestUtils.ExtensiveTestScenario.prepareDb(sourceSeedDb);
      }
    );

    const targetIModelId = await createPopulatedIModelHubIModel(
      "TransformerTarget",
      async (targetSeedDb) => {
        await TransformerExtensiveTestScenario.prepareTargetDb(targetSeedDb);
        assert.isTrue(targetSeedDb.codeSpecs.hasName("TargetCodeSpec")); // inserted by prepareTargetDb
      }
    );

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
      assert.isTrue(sourceDb.isBriefcaseDb());
      assert.isTrue(targetDb.isBriefcaseDb());
      assert.isFalse(sourceDb.isSnapshot);
      assert.isFalse(targetDb.isSnapshot);
      assert.isTrue(targetDb.codeSpecs.hasName("TargetCodeSpec")); // make sure prepareTargetDb changes were saved and pushed to iModelHub

      if (true) {
        // initial import
        await withEditTxn(sourceDb, "populate source", async () => {
          await TestUtils.ExtensiveTestScenario.populateDb(sourceDb);
        });
        await sourceDb.pushChanges({
          accessToken,
          description: "Populate source",
        });

        // Use IModelExporter.exportChanges to verify the changes to the sourceDb
        const sourceExportFileName: string =
          IModelTransformerTestUtils.prepareOutputFile(
            "IModelTransformer",
            "TransformerSource-ExportChanges-1.txt"
          );
        assert.isFalse(IModelJsFs.existsSync(sourceExportFileName));
        const sourceExporter = new IModelToTextFileExporter(
          sourceDb,
          sourceExportFileName
        );
        sourceExporter.exporter["_resetChangeDataOnExport"] = false;
        await sourceExporter.exportChanges({});
        assert.isTrue(IModelJsFs.existsSync(sourceExportFileName));
        const sourceDbChanges = (sourceExporter.exporter as any)
          ._sourceDbChanges; // access private member for testing purposes
        assert.exists(sourceDbChanges);
        // expect inserts and 1 update from populateSourceDb
        assert.isAtLeast(sourceDbChanges.codeSpec.insertIds.size, 1);
        assert.isAtLeast(sourceDbChanges.element.insertIds.size, 1);
        assert.isAtLeast(sourceDbChanges.aspect.insertIds.size, 1);
        assert.isAtLeast(sourceDbChanges.model.insertIds.size, 1);
        assert.equal(
          sourceDbChanges.model.updateIds.size,
          1,
          "Expect the RepositoryModel to be updated"
        );
        assert.isTrue(
          sourceDbChanges.model.updateIds.has(IModel.repositoryModelId)
        );
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

        // Initial import uses processAll to establish provenance
        const importEditTxn1 = createStartedEditTxn(targetDb);
        const transformer = await TestIModelTransformer.create(
          sourceDb,
          importEditTxn1
        );
        await transformer.process();
        // Verify processAll wrote the sync version so subsequent processChanges starts from correct index
        const syncVersionAfterProcessAll =
          await transformer["_provenanceManager"].getSynchronizationVersion();
        assert.equal(
          syncVersionAfterProcessAll.index,
          sourceDb.changeset.index,
          "processAll should write sync version matching source changeset index"
        );
        transformer.dispose();
        importEditTxn1.end();
        await targetDb.pushChanges({ accessToken, description: "Import #1" });
        TransformerExtensiveTestScenario.assertTargetDbContents(
          sourceDb,
          targetDb
        );

        // Use IModelExporter.exportChanges to verify the changes to the targetDb
        const targetExportFileName: string =
          IModelTransformerTestUtils.prepareOutputFile(
            "IModelTransformer",
            "TransformerTarget-ExportChanges-1.txt"
          );
        assert.isFalse(IModelJsFs.existsSync(targetExportFileName));
        const targetExporter = new IModelToTextFileExporter(
          targetDb,
          targetExportFileName
        );
        targetExporter.exporter["_resetChangeDataOnExport"] = false;
        await targetExporter.exportChanges({});
        assert.isTrue(IModelJsFs.existsSync(targetExportFileName));
        const targetDbChanges: any = (targetExporter.exporter as any)
          ._sourceDbChanges; // access private member for testing purposes
        assert.exists(targetDbChanges);
        // expect inserts and a few updates from transforming the result of populateSourceDb
        assert.isAtLeast(targetDbChanges.codeSpec.insertIds.size, 1);
        assert.isAtLeast(targetDbChanges.element.insertIds.size, 1);
        assert.isAtMost(
          targetDbChanges.element.updateIds.size,
          1,
          "Expect the root Subject to be updated"
        );
        assert.isAtLeast(targetDbChanges.aspect.insertIds.size, 1);
        assert.isAtLeast(targetDbChanges.model.insertIds.size, 1);
        assert.isAtMost(
          targetDbChanges.model.updateIds.size,
          1,
          "Expect the RepositoryModel to be updated"
        );
        assert.isTrue(
          targetDbChanges.model.updateIds.has(IModel.repositoryModelId)
        );
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

      if (true) {
        // second import with no changes to source, should be a no-op
        const numTargetElements: number = count(
          targetDb,
          Element.classFullName
        );
        const numTargetExternalSourceAspects: number = count(
          targetDb,
          ExternalSourceAspect.classFullName
        );
        const numTargetRelationships: number = count(
          targetDb,
          ElementRefersToElements.classFullName
        );
        const hubEditTxn = createStartedEditTxn(targetDb);
        const targetImporter = new CountingIModelImporter(hubEditTxn);
        const transformer = await TestIModelTransformer.create(
          sourceDb,
          targetImporter,
          { argsForProcessChanges: {} }
        );
        await transformer.process();
        assert.equal(targetImporter.numModelsInserted, 0);
        assert.equal(targetImporter.numModelsUpdated, 0);
        assert.equal(targetImporter.numElementsInserted, 0);
        expect(targetImporter.numElementsUpdated).to.equal(0);
        assert.equal(targetImporter.numElementsExplicitlyDeleted, 0);
        assert.equal(targetImporter.numElementAspectsInserted, 0);
        assert.equal(targetImporter.numElementAspectsUpdated, 0);
        assert.equal(targetImporter.numRelationshipsInserted, 0);
        assert.equal(targetImporter.numRelationshipsUpdated, 0);
        assert.equal(
          numTargetElements,
          count(targetDb, Element.classFullName),
          "Second import should not add elements"
        );
        assert.equal(
          numTargetExternalSourceAspects,
          count(targetDb, ExternalSourceAspect.classFullName),
          "Second import should not add aspects"
        );
        assert.equal(
          numTargetRelationships,
          count(targetDb, ElementRefersToElements.classFullName),
          "Second import should not add relationships"
        );
        hubEditTxn.end();
        assert.isFalse(targetDb.txns.hasPendingTxns);
        await targetDb.pushChanges({
          accessToken,
          description: "Should not actually push because there are no changes",
        });
        transformer.dispose();
      }

      if (true) {
        // update source db, then import again
        withEditTxn(sourceDb, "update source", () => {
          TestUtils.ExtensiveTestScenario.updateDb(sourceDb);
        });
        await sourceDb.pushChanges({
          accessToken,
          description: "Update source",
        });

        // Use IModelExporter.exportChanges to verify the changes to the sourceDb
        const sourceExportFileName: string =
          IModelTransformerTestUtils.prepareOutputFile(
            "IModelTransformer",
            "TransformerSource-ExportChanges-2.txt"
          );
        assert.isFalse(IModelJsFs.existsSync(sourceExportFileName));
        const sourceExporter = new IModelToTextFileExporter(
          sourceDb,
          sourceExportFileName
        );
        sourceExporter.exporter["_resetChangeDataOnExport"] = false;
        await sourceExporter.exportChanges({});
        assert.isTrue(IModelJsFs.existsSync(sourceExportFileName));
        const sourceDbChanges: any = (sourceExporter.exporter as any)
          ._sourceDbChanges; // access private member for testing purposes
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

        const importEditTxn2 = createStartedEditTxn(targetDb);
        const transformer = await TestIModelTransformer.create(
          sourceDb,
          importEditTxn2,
          {
            argsForProcessChanges: {},
          }
        );
        await transformer.process();
        transformer.dispose();
        importEditTxn2.end();
        await targetDb.pushChanges({ accessToken, description: "Import #2" });
        TestUtils.ExtensiveTestScenario.assertUpdatesInDb(targetDb);

        // Use IModelExporter.exportChanges to verify the changes to the targetDb
        const targetExportFileName: string =
          IModelTransformerTestUtils.prepareOutputFile(
            "IModelTransformer",
            "TransformerTarget-ExportChanges-2.txt"
          );
        assert.isFalse(IModelJsFs.existsSync(targetExportFileName));
        const targetExporter = new IModelToTextFileExporter(
          targetDb,
          targetExportFileName
        );
        targetExporter.exporter["_resetChangeDataOnExport"] = false;
        await targetExporter.exportChanges({});
        assert.isTrue(IModelJsFs.existsSync(targetExportFileName));
        const targetDbChanges: any = (targetExporter.exporter as any)
          ._sourceDbChanges; // access private member for testing purposes
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

      const sourceIModelChangeSets = await IModelHost[
        _hubAccess
      ].queryChangesets({ accessToken, iModelId: sourceIModelId });
      const targetIModelChangeSets = await IModelHost[
        _hubAccess
      ].queryChangesets({ accessToken, iModelId: targetIModelId });
      assert.equal(sourceIModelChangeSets.length, 2);
      assert.equal(targetIModelChangeSets.length, 2);

      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
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
      await transformer.process();
      transformer.dispose();
      consolidateEditTxn2.end();

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
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
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
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should be able to handle relationship delete using fedguids", async () => {
    const masterIModelName = "MasterNewRelProvenanceFedGuids";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = { 1: 1, 2: 1 };
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    // masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);

    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };
    let relIdInBranch: string | undefined;
    const timeline: Timeline = [
      { master: { seed: masterSeed } }, // masterSeedState is above
      { branch1: { branch: "master" } },
      {
        branch1: {
          manualUpdate(db) {
            // Create relationship in branch iModel
            withEditTxn(db, "insert branch relationship", (txn) => {
              const sourceId = IModelTestUtils.queryByUserLabel(db, "1");
              const targetId = IModelTestUtils.queryByUserLabel(db, "2");
              const rel = ElementGroupsMembers.create(db, sourceId, targetId);
              relIdInBranch = txn.insertRelationship(rel.toJSON());
            });
          },
        },
      },
      {
        master: {
          sync: ["branch1"],
        },
      }, // first master<-branch1 reverse sync picking up new relationship from branch imodel
      {
        assert({ branch1 }) {
          const aspects = branch1.db.elements.getAspects(
            IModelTestUtils.queryByUserLabel(branch1.db, "1"),
            ExternalSourceAspect.classFullName
          ) as ExternalSourceAspect[];
          expect(aspects.length).to.be.equal(0);
        },
      },
      {
        master: {
          manualUpdate(db) {
            // Delete relationship in master iModel
            withEditTxn(db, "delete master relationship", (txn) => {
              const rel = db.relationships.getInstance<ElementGroupsMembers>(
                ElementGroupsMembers.classFullName,
                {
                  sourceId: IModelTestUtils.queryByUserLabel(db, "1"),
                  targetId: IModelTestUtils.queryByUserLabel(db, "2"),
                }
              );
              txn.deleteRelationship(rel.toJSON());
            });
          },
        },
      },
      {
        branch1: {
          sync: ["master"],
        },
      }, // forward sync master->branch1 to pick up delete of relationship
      {
        assert({ branch1 }) {
          // Expect relationship to be gone in branch iModel.
          expect(relIdInBranch, "expected relationship id in branch to be set")
            .to.not.be.undefined;
          expect(() =>
            branch1.db.relationships.getInstance<ElementGroupsMembers>(
              ElementGroupsMembers.classFullName,
              relIdInBranch!
            )
          ).to.throw(IModelError);
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, { iTwinId, accessToken });
    await tearDown();
  });

  it("should be able to handle relationship delete using new relationship provenance method with no fedguids", async () => {
    // SEE: https://github.com/iTwin/imodel-transformer/issues/54 for the scenario this test exercises
    /** This test does the following:
     *  sync master to branch with two elements, x and y, with NULL fed guid to force ESAs to be generated (For future relationship)
     *  create relationship between x and y in branch imodel
     *  reverse sync branch to master
     *  delete relationship between x and y in master
     *  forward sync to branch
     *  expect relationship gets deleted in branch imodel.
     */
    const masterIModelName = "MasterNewRelProvenanceNoFedGuids";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = { 1: 1, 2: 1 };
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    // masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);
    const noFedGuidElemIds = masterSeedDb.queryEntityIds({
      from: "Bis.Element",
      where: "UserLabel IN ('1','2')",
    });
    withEditTxn(masterSeedDb, "null out fedguids", () => {
      for (const elemId of noFedGuidElemIds)
        masterSeedDb.withSqliteStatement(
          `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
          (s) => {
            expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
          }
        );
    });
    masterSeedDb.performCheckpoint();

    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };
    let relIdInBranch: string | undefined;
    const timeline: Timeline = [
      { master: { seed: masterSeed } }, // masterSeedState is above
      { branch1: { branch: "master" } },
      {
        branch1: {
          manualUpdate(db) {
            // Create relationship in branch iModel
            withEditTxn(db, "insert branch relationship", (txn) => {
              const sourceId = IModelTestUtils.queryByUserLabel(db, "1");
              const targetId = IModelTestUtils.queryByUserLabel(db, "2");
              const rel = ElementGroupsMembers.create(db, sourceId, targetId);
              relIdInBranch = txn.insertRelationship(rel.toJSON());
            });
          },
        },
      },
      {
        master: {
          sync: ["branch1"],
        },
      }, // first master<-branch1 reverse sync picking up new relationship from branch imodel
      {
        assert({ branch1 }) {
          const aspects = branch1.db.elements.getAspects(
            IModelTestUtils.queryByUserLabel(branch1.db, "1"),
            ExternalSourceAspect.classFullName
          ) as ExternalSourceAspect[];
          expect(aspects.length).to.be.equal(2);
          for (const aspect of aspects) {
            if (aspect.kind === "Relationship") {
              // When forceOldRelationshipProvenanceMethod is not set to true, provenanceRelInstanceId is defined on jsonProperties.
              expect(aspect.jsonProperties).to.not.be.undefined;
              expect(JSON.parse(aspect.jsonProperties!).provenanceRelInstanceId)
                .to.not.be.undefined;
            }
          }
        },
      },
      {
        master: {
          manualUpdate(db) {
            // Delete relationship in master iModel
            withEditTxn(db, "delete master relationship", (txn) => {
              const rel = db.relationships.getInstance<ElementGroupsMembers>(
                ElementGroupsMembers.classFullName,
                {
                  sourceId: IModelTestUtils.queryByUserLabel(db, "1"),
                  targetId: IModelTestUtils.queryByUserLabel(db, "2"),
                }
              );
              txn.deleteRelationship(rel.toJSON());
            });
          },
        },
      },
      {
        branch1: {
          sync: ["master"],
        },
      }, // forward sync master->branch1 to pick up delete of relationship
      {
        assert({ branch1 }) {
          // Expect relationship to be gone in branch iModel.
          expect(relIdInBranch, "expected relationship id in branch to be set")
            .to.not.be.undefined;
          expect(() =>
            branch1.db.relationships.getInstance<ElementGroupsMembers>(
              ElementGroupsMembers.classFullName,
              relIdInBranch!
            )
          ).to.throw(IModelError);
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, { iTwinId, accessToken });
    await tearDown();
  });

  it("should be able to handle relationship delete using old relationship provenance method with no fedguids", async () => {
    // SEE: https://github.com/iTwin/imodel-transformer/issues/54 for the scenario this test exercises
    /** This test does the following:
     *  sync master to branch with two elements, x and y, with NULL fed guid to force ESAs to be generated (For future relationship)
     *  create relationship between x and y in branch imodel
     *  reverse sync branch to master with forceOldRelationshipProvenanceMethod = true
     *  delete relationship between x and y in master
     *  forward sync to branch
     *  expect relationship gets deleted in branch imodel.
     */
    const masterIModelName = "MasterOldRelProvenanceNoFedGuids";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = { 1: 1, 2: 1 };
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    // masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);
    const noFedGuidElemIds = masterSeedDb.queryEntityIds({
      from: "Bis.Element",
      where: "UserLabel IN ('1','2')",
    });
    withEditTxn(masterSeedDb, "null out fedguids", () => {
      for (const elemId of noFedGuidElemIds)
        masterSeedDb.withSqliteStatement(
          `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
          (s) => {
            expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
          }
        );
    });
    masterSeedDb.performCheckpoint();

    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };
    let relIdInBranch: string | undefined;
    const setForceOldRelationshipProvenanceMethod = (
      transformer: IModelTransformer
    ) => (transformer["_forceOldRelationshipProvenanceMethod"] = true);
    const timeline: Timeline = [
      { master: { seed: masterSeed } }, // masterSeedState is above
      { branch1: { branch: "master" } },
      {
        branch1: {
          manualUpdate(db) {
            // Create relationship in branch iModel
            withEditTxn(db, "insert branch relationship", (txn) => {
              const sourceId = IModelTestUtils.queryByUserLabel(db, "1");
              const targetId = IModelTestUtils.queryByUserLabel(db, "2");
              const rel = ElementGroupsMembers.create(db, sourceId, targetId);
              relIdInBranch = txn.insertRelationship(rel.toJSON());
            });
          },
        },
      },
      {
        master: {
          sync: [
            "branch1",
            {
              initTransformer: setForceOldRelationshipProvenanceMethod,
            },
          ],
        },
      }, // first master<-branch1 reverse sync picking up new relationship from branch imodel
      {
        assert({ branch1 }) {
          // Lets make sure that forceOldRelationshipProvenance worked by reading the json properties of the ESA for the relationship.
          const aspects = branch1.db.elements.getAspects(
            IModelTestUtils.queryByUserLabel(branch1.db, "1"),
            ExternalSourceAspect.classFullName
          ) as ExternalSourceAspect[];
          expect(aspects.length).to.be.equal(2);
          let foundRelationshipAspect = false;
          for (const aspect of aspects) {
            if (aspect.kind === "Relationship") {
              foundRelationshipAspect = true;
              // When forceOldRelationshipProvenanceMethod is true, targetRelInstanceId is defined on jsonProperties.
              expect(aspect.jsonProperties).to.not.be.undefined;
              expect(JSON.parse(aspect.jsonProperties!).targetRelInstanceId).to
                .not.be.undefined;
            }
          }
          expect(foundRelationshipAspect).to.be.true;
        },
      },
      {
        master: {
          manualUpdate(db) {
            // Delete relationship in master iModel
            withEditTxn(db, "delete master relationship", (txn) => {
              const rel = db.relationships.getInstance<ElementGroupsMembers>(
                ElementGroupsMembers.classFullName,
                {
                  sourceId: IModelTestUtils.queryByUserLabel(db, "1"),
                  targetId: IModelTestUtils.queryByUserLabel(db, "2"),
                }
              );
              txn.deleteRelationship(rel.toJSON());
            });
          },
        },
      },
      {
        branch1: {
          sync: [
            "master",
            {
              initTransformer: setForceOldRelationshipProvenanceMethod,
            },
          ],
        },
      }, // forward sync master->branch1 to pick up delete of relationship
      {
        assert({ branch1 }) {
          // Expect relationship to be gone in branch iModel.
          expect(relIdInBranch, "expected relationship id in branch to be set")
            .to.not.be.undefined;
          expect(() =>
            branch1.db.relationships.getInstance<ElementGroupsMembers>(
              ElementGroupsMembers.classFullName,
              relIdInBranch!
            )
          ).to.throw(IModelError);
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, { iTwinId, accessToken });
    await tearDown();
  });

  it("should not include 'initialized branch provenance' changeset in a reverse sync", async () => {
    const validateCsFileProps = (transformer: IModelTransformer) => {
      const csFileProps = transformer["_csFileProps"];
      expect(
        csFileProps?.some((csFileProp) =>
          csFileProp.description.includes("initialized branch provenance")
        )
      ).to.be.false;
    };
    const timeline: Timeline = [
      { master: { 1: 1, 2: 2, 3: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2, 4: 1 } },
      {
        master: {
          sync: [
            "branch",
            { assert: { afterProcessChanges: validateCsFileProps } },
          ],
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    await tearDown();
  });

  it("should merge changes made on a branch back to master", async () => {
    const masterIModelName = "Master";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = { 1: 1, 2: 1, 20: 1, 21: 1, 40: 1, 41: 2, 42: 3 };
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    // masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);

    // 20 will be deleted, so it's important to know remapping deleted elements still works if there is no fedguid
    const noFedGuidElemIds = masterSeedDb.queryEntityIds({
      from: "Bis.Element",
      where: "UserLabel IN ('1','20','41','42')",
    });
    for (const elemId of noFedGuidElemIds)
      masterSeedDb.withSqliteStatement(
        `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
        (s) => {
          expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
        }
      );
    masterSeedDb.performCheckpoint();

    // hard to check this without closing the db...
    const seedSecondConn = SnapshotDb.openFile(masterSeedDb.pathName);
    for (const elemId of noFedGuidElemIds)
      expect(seedSecondConn.elements.getElement(elemId).federationGuid).to.be
        .undefined;
    seedSecondConn.close();

    const expectedRelationships = [
      {
        sourceLabel: "40",
        targetLabel: "2",
        idInBranch1: "not inserted yet",
        sourceFedGuid: true,
        targetFedGuid: true,
      },
      {
        sourceLabel: "41",
        targetLabel: "42",
        idInBranch1: "not inserted yet",
        sourceFedGuid: false,
        targetFedGuid: false,
      },
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
      { master: { 40: 5 } },
      { branch2: { branch: "master" } },
      { branch1: { 2: 2, 3: 1, 4: 1 } },
      {
        branch1: {
          manualUpdate(db) {
            withEditTxn(db, "insert expected relationships", (txn) => {
              expectedRelationships.map(({ sourceLabel, targetLabel }, i) => {
                const sourceId = IModelTestUtils.queryByUserLabel(
                  db,
                  sourceLabel
                );
                const targetId = IModelTestUtils.queryByUserLabel(
                  db,
                  targetLabel
                );
                assert(sourceId && targetId);
                const rel = ElementGroupsMembers.create(
                  db,
                  sourceId,
                  targetId,
                  0
                );
                expectedRelationships[i].idInBranch1 = txn.insertRelationship(
                  rel.toJSON()
                );
              });
            });
          },
        },
      },
      {
        branch1: {
          manualUpdate(db) {
            withEditTxn(db, "update expected relationship", (txn) => {
              const rel = db.relationships.getInstance<ElementGroupsMembers>(
                ElementGroupsMembers.classFullName,
                expectedRelationships[0].idInBranch1
              );
              rel.memberPriority = 1;
              txn.updateRelationship(rel.toJSON());
            });
          },
        },
      },
      { branch1: { 1: 2, 3: deleted, 5: 1, 6: 1, 20: deleted, 21: 2 } },
      { branch1: { 21: deleted, 30: 1 } },
      { master: { sync: ["branch1"] } }, // first master<-branch1 reverse sync
      {
        async assert({ master, branch1 }) {
          await assertElemState(master.db, {
            // relationship props are a lot to type out so let's grab those from the branch
            ...branch1.state,
            // double check deletions propagated by sync
            20: undefined as any,
            21: undefined as any,
            40: 5, // this element was not changed in the branch, so the sync won't update it
          });
        },
      },
      { branch2: { sync: ["master"] } }, // first master->branch2 forward sync
      {
        async assert({ master, branch2 }) {
          await assertElemState(branch2.db, master.state);
        },
      },
      { branch2: { 7: 1, 8: 1 } },
      // insert 9 and a conflicting state for 7 on master
      { master: { 7: 2, 9: 1 } },
      { master: { sync: ["branch2"] } }, // first master<-branch2 reverse sync
      {
        async assert({ master, branch1, branch2 }) {
          for (const { db } of [master, branch1, branch2]) {
            const elem1Id = IModelTestUtils.queryByUserLabel(db, "1");
            expect(db.elements.getElement(elem1Id).federationGuid).to.be
              .undefined;

            for (const rel of expectedRelationships) {
              const sourceId = IModelTestUtils.queryByUserLabel(
                db,
                rel.sourceLabel
              );
              const targetId = IModelTestUtils.queryByUserLabel(
                db,
                rel.targetLabel
              );
              expect(
                db.elements.getElement(sourceId).federationGuid !== undefined
              ).to.be.equal(rel.sourceFedGuid);
              expect(
                db.elements.getElement(targetId).federationGuid !== undefined
              ).to.be.equal(rel.targetFedGuid);
            }
          }

          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(
            0
          );

          for (const branch of [branch1, branch2]) {
            const elem1Id = IModelTestUtils.queryByUserLabel(branch.db, "1");
            expect(branch.db.elements.getElement(elem1Id).federationGuid).to.be
              .undefined;
            const aspects = [
              ...branch.db.queryEntityIds({
                from: "BisCore.ExternalSourceAspect",
              }),
            ].map((aspectId) =>
              branch.db.elements.getAspect(aspectId).toJSON()
            ) as ExternalSourceAspectProps[];
            expect(aspects).to.deep.subsetEqual([
              {
                element: { id: IModelDb.rootSubjectId },
                identifier: master.db.iModelId,
              },
              {
                element: { id: elem1Id },
                identifier: elem1Id,
              },
            ]);
            expect(Date.parse(aspects[3].version!)).not.to.be.NaN;
          }

          // branch2 won the conflict since it is the synchronization source
          await assertElemState(master.db, { 7: 1 }, { subset: true });
        },
      },
      { master: { 6: 2 } },
      {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "delete expected relationships", (txn) => {
              expectedRelationships.forEach(({ sourceLabel, targetLabel }) => {
                const sourceId = IModelTestUtils.queryByUserLabel(
                  db,
                  sourceLabel
                );
                const targetId = IModelTestUtils.queryByUserLabel(
                  db,
                  targetLabel
                );
                assert(sourceId && targetId);
                const rel = db.relationships.getInstance(
                  ElementGroupsMembers.classFullName,
                  { sourceId, targetId }
                );
                txn.deleteRelationship(rel.toJSON());
              });
            });
          },
        },
      },
      { branch1: { sync: ["master"] } }, // first master->branch1 forward sync
      {
        async assert({ branch1 }) {
          for (const rel of expectedRelationships) {
            expect(
              branch1.db.relationships.tryGetInstance(
                ElementGroupsMembers.classFullName,
                rel.idInBranch1
              ),
              `had ${rel.sourceLabel}->${rel.targetLabel}`
            ).to.be.undefined;
            const sourceId = IModelTestUtils.queryByUserLabel(
              branch1.db,
              rel.sourceLabel
            );
            const targetId = IModelTestUtils.queryByUserLabel(
              branch1.db,
              rel.targetLabel
            );
            assert(sourceId && targetId);
            expect(
              branch1.db.relationships.tryGetInstance(
                ElementGroupsMembers.classFullName,
                { sourceId, targetId }
              ),
              `had ${rel.sourceLabel}->${rel.targetLabel}`
            ).to.be.undefined;

            // check rel aspect was deleted
            const srcElemAspects = branch1.db.elements.getAspects(
              sourceId,
              ExternalSourceAspect.classFullName
            ) as ExternalSourceAspect[];
            expect(
              !srcElemAspects.some((a) => a.identifier === rel.idInBranch1)
            ).to.be.true;
            expect(srcElemAspects.length).to.lessThanOrEqual(1);
          }
          await assertElemState(branch1.db, { 7: 1 }, { subset: true });
        },
      },
      // 7 originally came from branch2. Modify it.
      { branch1: { 7: 10 } },
      // Reverse sync branch 1 to master with the change to 7.
      { master: { sync: ["branch1"] } },
      // Forward sync master to branch2 with the change to 7.
      { branch2: { sync: ["master"] } },
      {
        async assert({ master, branch1, branch2 }) {
          for (const imodel of [master, branch1, branch2]) {
            await assertElemState(imodel.db, { 7: 10 }, { subset: true });
          }
        },
      },
    ];

    const { trackedIModels, tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });
    masterSeedDb.close();

    // create empty iModel meant to contain replayed master history
    const replayedIModelName = "Replayed";
    const replayedIModelId = await IModelHost[_hubAccess].createNewIModel({
      iTwinId,
      iModelName: replayedIModelName,
      description: "blank",
      noLocks: true,
    });

    const replayedDb = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: replayedIModelId,
    });
    assert.isTrue(replayedDb.isBriefcaseDb());
    assert.equal(replayedDb.iTwinId, iTwinId);

    try {
      const master = trackedIModels.get("master");
      assert(master);

      const masterDbChangesets = await IModelHost[
        _hubAccess
      ].downloadChangesets({
        accessToken,
        iModelId: master.id,
        targetDir: BriefcaseManager.getChangeSetsPath(master.id),
      });
      assert.equal(masterDbChangesets.length, 7);
      const masterDeletedElementIds = new Set<Id64String>();
      const masterDeletedRelationshipIds = new Set<Id64String>();
      for (const masterDbChangeset of masterDbChangesets) {
        assert.isDefined(masterDbChangeset.id);
        assert.isDefined(masterDbChangeset.description); // test code above always included a change description when pushChanges was called
        // below is one way of determining the set of elements that were deleted in a specific changeset
        const changedInstanceIds = await ChangedInstanceIds.initialize({
          iModel: master.db,
          csFileProps: [masterDbChangeset],
        });
        const result = changedInstanceIds;
        if (result === undefined) throw Error("expected to be defined");

        if (result.element.deleteIds) {
          result.element.deleteIds.forEach((id: Id64String) =>
            masterDeletedElementIds.add(id)
          );
        }
        if (result.relationship.deleteIds) {
          result.relationship.deleteIds.forEach((id: Id64String) =>
            masterDeletedRelationshipIds.add(id)
          );
        }
      }
      expect(masterDeletedElementIds.size).to.equal(2); // elem '3' is never seen by master
      expect(masterDeletedRelationshipIds.size).to.equal(2);

      // replay master history to create replayed iModel
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: master.id,
        asOf: IModelVersion.first().toJSON(),
      });
      const makeReplayTransformer = (
        argsForProcessChanges?: ProcessChangesOptions
      ) => {
        const editTxn = createStartedEditTxn(replayedDb);
        const transformer = new IModelTransformer(
          { source: sourceDb, target: editTxn },
          {
            argsForProcessChanges,
          }
        );
        // this replay strategy pretends that deleted elements never existed
        for (const elementId of masterDeletedElementIds) {
          transformer.exporter.excludeElement(elementId);
        }
        return { editTxn, transformer };
      };

      // NOTE: this test knows that there were no schema changes, so does not call `processSchemas`
      const replayInitTransformer = makeReplayTransformer();
      await replayInitTransformer.transformer.process(); // process any elements that were part of the "seed"
      replayInitTransformer.transformer.dispose();
      replayInitTransformer.editTxn.end();

      await saveAndPushChanges(replayedDb, "changes from source seed");
      for (const masterDbChangeset of masterDbChangesets) {
        await sourceDb.pullChanges({
          accessToken,
          toIndex: masterDbChangeset.index,
        });
        const replayTransformer = makeReplayTransformer({
          startChangeset: sourceDb.changeset,
        });
        await replayTransformer.transformer.process();
        replayTransformer.editTxn.end();
        await saveAndPushChanges(
          replayedDb,
          masterDbChangeset.description ?? ""
        );
        replayTransformer.transformer.dispose();
      }
      sourceDb.close();
      await assertElemState(replayedDb, master.state); // should have same ending state as masterDb

      // make sure there are no deletes in the replay history (all elements that were eventually deleted from masterDb were excluded)
      const replayedDbChangesets = await IModelHost[
        _hubAccess
      ].downloadChangesets({
        accessToken,
        iModelId: replayedIModelId,
        targetDir: BriefcaseManager.getChangeSetsPath(replayedIModelId),
      });
      assert.isAtLeast(replayedDbChangesets.length, masterDbChangesets.length); // replayedDb will have more changesets when seed contains elements
      const replayedDeletedElementIds = new Set<Id64String>();
      for (const replayedDbChangeset of replayedDbChangesets) {
        assert.isDefined(replayedDbChangeset.id);
        const changesetPath = replayedDbChangeset.pathname;
        assert.isTrue(IModelJsFs.existsSync(changesetPath));
        // below is one way of determining the set of elements that were deleted in a specific changeset
        const changedInstanceIds = await ChangedInstanceIds.initialize({
          iModel: replayedDb,
          csFileProps: [replayedDbChangeset],
        });
        const result = changedInstanceIds;
        if (result === undefined) throw Error("expected to be defined");

        assert.isDefined(result.element);
        if (result.element.deleteIds) {
          result.element.deleteIds.forEach((id: Id64String) =>
            replayedDeletedElementIds.add(id)
          );
        }
      }
      assert.equal(replayedDeletedElementIds.size, 0);
    } finally {
      await tearDown();
      replayedDb.close();
      await IModelHost[_hubAccess].deleteIModel({
        iTwinId,
        iModelId: replayedIModelId,
      });
    }
  });

  it("ModelSelector processChanges", async () => {
    const sourceIModelName = "ModelSelectorSource";
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    let targetIModelId!: GuidString;
    assert.isTrue(Guid.isGuid(sourceIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });

      // setup source
      const {
        physModel1Id: _physModel1Id,
        physModel2Id,
        modelSelectorCode,
        modelSelectorId,
      } = withEditTxn(sourceDb, "setup source models and selector", (txn) => {
        const model1Id = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "phys-model-1"
        );
        const model2Id = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "phys-model-2"
        );
        const modelSelectorInSource = ModelSelector.create(
          sourceDb,
          IModelDb.dictionaryId,
          "model-selector",
          [model1Id]
        );
        const code = modelSelectorInSource.code;
        const selectorId = modelSelectorInSource.insert(txn);
        return {
          physModel1Id: model1Id,
          physModel2Id: model2Id,
          modelSelectorCode: code,
          modelSelectorId: selectorId,
        };
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "setup source models and selector",
      });

      // create target branch
      const targetIModelName = "ModelSelectorTarget";
      sourceDb.performCheckpoint();

      targetIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: targetIModelName,
        noLocks: true,
        version0: sourceDb.pathName,
      });
      assert.isTrue(Guid.isGuid(targetIModelId));
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
      const provenanceInitEditTxn = createStartedEditTxn(targetDb);
      const provenanceInitializer = new IModelTransformer(
        { source: sourceDb, target: provenanceInitEditTxn },
        { wasSourceIModelCopiedToTarget: true }
      );
      await provenanceInitializer.processSchemas();
      await provenanceInitializer.process();
      provenanceInitializer.dispose();
      provenanceInitEditTxn.end();

      // update source (add model2 to model selector)
      // (it's important that we only change the model selector here to keep the changes isolated)
      withEditTxn(sourceDb, "add model2 to model selector", (txn) => {
        const modelSelectorUpdate = sourceDb.elements.getElement<ModelSelector>(
          modelSelectorId,
          ModelSelector
        );
        modelSelectorUpdate.models = [
          ...modelSelectorUpdate.models,
          physModel2Id,
        ];
        modelSelectorUpdate.update(txn);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "add model2 to model selector",
      });

      // check that the model selector has the expected change in the source
      const modelSelectorUpdate2 = sourceDb.elements.getElement<ModelSelector>(
        modelSelectorId,
        ModelSelector
      );
      expect(modelSelectorUpdate2.models).to.have.length(2);

      // test extracted changed ids
      const sourceDbChangesets = await IModelHost[
        _hubAccess
      ].downloadChangesets({
        accessToken,
        iModelId: sourceIModelId,
        targetDir: BriefcaseManager.getChangeSetsPath(sourceIModelId),
      });
      expect(sourceDbChangesets).to.have.length(2);
      const latestChangeset = sourceDbChangesets[1];
      const changedInstanceIds = await ChangedInstanceIds.initialize({
        iModel: sourceDb,
        csFileProps: [latestChangeset],
      });
      const result = changedInstanceIds;
      if (result === undefined) throw Error("expected to be defined");
      const expectedElementUpdateIds = new Set<Id64String>([modelSelectorId]);
      const expectedModelUpdateIds = new Set<Id64String>([IModel.dictionaryId]); // containing model will also get last modification time updated

      expect(result.element.updateIds).to.deep.equal(expectedElementUpdateIds);
      expect(result.model.updateIds).to.deep.equal(expectedModelUpdateIds);

      // synchronize
      let didExportModelSelector = false,
        didImportModelSelector = false;
      class IModelImporterInjected extends IModelImporter {
        public override async importElement(
          sourceElement: ElementProps
        ): Promise<Id64String> {
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

      const injectedEditTxn = createStartedEditTxn(targetDb);
      const synchronizer = new IModelTransformerInjected(
        {
          source: sourceDb,
          target: new IModelImporterInjected(injectedEditTxn),
        },
        { argsForProcessChanges: {} }
      );
      await synchronizer.process();
      expect(didExportModelSelector).to.be.true;
      expect(didImportModelSelector).to.be.true;
      synchronizer.dispose();
      injectedEditTxn.end();
      await targetDb.pushChanges({ accessToken, description: "synchronize" });

      // check that the model selector has the expected change in the target
      const modelSelectorInTargetId =
        targetDb.elements.queryElementIdByCode(modelSelectorCode);
      assert(
        modelSelectorInTargetId !== undefined,
        `expected obj ${modelSelectorInTargetId} to be defined`
      );

      const modelSelectorInTarget = targetDb.elements.getElement<ModelSelector>(
        modelSelectorInTargetId,
        ModelSelector
      );
      expect(modelSelectorInTarget.models).to.have.length(2);

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        assert.fail(err, undefined, "failed to clean up");
      }
    }
  });

  it("should correctly initialize provenance map for change processing", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Source");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Target");
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

      const subject2Id = withEditTxn(
        sourceDb,
        "create subjects and model",
        (txn) => {
          const subject1 = Subject.create(sourceDb, IModel.rootSubjectId, "S1");
          const subject2 = Subject.create(sourceDb, IModel.rootSubjectId, "S2");
          subject2.federationGuid = Guid.empty; // Empty guid will force the element to have an undefined federation guid.
          subject1.insert(txn);
          const subj2Id = subject2.insert(txn);
          PhysicalModel.insert(txn, subj2Id, "PM1");
          return subj2Id;
        }
      );
      await sourceDb.pushChanges({
        accessToken,
        description: "subject with no fed guid",
      });

      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
      const initialTargetEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialTargetEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      initialTargetEditTxn.end();

      withEditTxn(sourceDb, "insert PM2", (txn) => {
        PhysicalModel.insert(txn, subject2Id, "PM2");
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "PhysicalPartition",
      });

      const changeTargetEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changeTargetEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: { id: sourceDb.changeset.id },
          },
        }
      );
      await transformer.process();

      const elementCodeValueMap = new Map<Id64String, string>();
      const sql = `SELECT ECInstanceId, CodeValue FROM ${Element.classFullName} WHERE ECInstanceId NOT IN (0x1, 0x10, 0xe)`;
      for await (const row of targetDb.createQueryReader(sql)) {
        elementCodeValueMap.set(row[0], row[1]);
      }

      // make sure provenance was tracked for all elements
      expect(count(sourceDb, Element.classFullName)).to.equal(4 + 3); // 2 Subjects, 2 PhysicalPartitions + 0x1, 0x10, 0xe
      expect(elementCodeValueMap.size).to.equal(4);
      elementCodeValueMap.forEach(
        (codeValue: string, elementId: Id64String) => {
          const sourceElementId =
            transformer.context.findTargetElementId(elementId);
          expect(sourceElementId).to.not.be.undefined;
          const sourceElement = sourceDb.elements.getElement(sourceElementId);
          expect(sourceElement.code.value).to.equal(codeValue);
        }
      );

      transformer.dispose();
      changeTargetEditTxn.end();

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should be able to synchronize iModel that is not at the tip", async () => {
    const pushChangesets = async (
      db: BriefcaseDb,
      category: Id64String,
      model: Id64String,
      numChangesets: number
    ) => {
      for (let i = 0; i < numChangesets; i++) {
        withEditTxn(db, `insert PhysicalObject ${i}`, (txn) => {
          const physicalElementProps: PhysicalElementProps = {
            category,
            model,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
          };
          txn.insertElement(physicalElementProps);
        });
        await db.pushChanges({
          description: `Inserted ${i} PhysicalObject`,
        });
      }
    };

    const seedFileName = path.join(outputDir, "notAtTipTestSeed.bim");
    if (IModelJsFs.existsSync(seedFileName))
      IModelJsFs.removeSync(seedFileName);

    const seedDb = SnapshotDb.createEmpty(seedFileName, {
      rootSubject: { name: "TransformerSource" },
    });
    const { categoryId1, modelId1 } = withEditTxn(
      seedDb,
      "create seed elements",
      (txn) => {
        const subjectId1 = Subject.insert(txn, IModel.rootSubjectId, "S1");
        const modId = PhysicalModel.insert(txn, subjectId1, "PM1");
        const catId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "C1",
          {}
        );
        const physicalElementProps1: PhysicalElementProps = {
          category: catId,
          model: modId,
          classFullName: PhysicalObject.classFullName,
          code: Code.createEmpty(),
        };
        txn.insertElement(physicalElementProps1);
        return { categoryId1: catId, modelId1: modId };
      }
    );
    seedDb.close();

    const sourceIModelId = await IModelHost[_hubAccess].createNewIModel({
      iTwinId,
      iModelName: "TransformerSource",
      description: "source",
      version0: seedFileName,
      noLocks: true,
    });

    // open/upgrade sourceDb
    const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: sourceIModelId,
    });
    // creating changesets for source
    await pushChangesets(sourceDb, categoryId1, modelId1, 1);
    sourceDb.performCheckpoint(); // so we can use as a seed

    // forking target
    const targetIModelId = await IModelHost[_hubAccess].createNewIModel({
      iTwinId,
      iModelName: "TransformerTarget",
      description: "target",
      version0: sourceDb.pathName,
      noLocks: true,
    });
    const targetDb = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: targetIModelId,
    });

    // fork provenance init
    const forkInitEditTxn = createStartedEditTxn(targetDb);
    let transformer = new IModelTransformer(
      { source: sourceDb, target: forkInitEditTxn },
      { wasSourceIModelCopiedToTarget: true }
    );
    await transformer.process();
    forkInitEditTxn.end();
    await targetDb.pushChanges({ description: "fork init" });
    const catIdInTarget = transformer.context.findTargetElementId(categoryId1);
    const modelIdInTarget = transformer.context.findTargetElementId(modelId1);
    transformer.dispose();

    // Push change to target db so we have changes to process during our reverse sync.
    await pushChangesets(targetDb, catIdInTarget, modelIdInTarget, 1);
    targetDb.performCheckpoint();

    // Push changesets twice to sourcedb, I only want to sync up to the first changeset I'm adding.
    await pushChangesets(sourceDb, categoryId1, modelId1, 1);
    const sourceDbChangesetNotAtTip = sourceDb.changeset;
    await pushChangesets(sourceDb, categoryId1, modelId1, 1);
    sourceDb.performCheckpoint();

    // Reverse Sync to add a pendingsyncchangesetindex
    const reverseSyncEditTxn = createStartedEditTxn(sourceDb);
    const reverseSyncSourceEditTxn = createStartedEditTxn(targetDb);
    transformer = new IModelTransformer(
      { source: targetDb, target: reverseSyncEditTxn },
      { argsForProcessChanges: {}, sourceEditTxn: reverseSyncSourceEditTxn }
    );
    await transformer.process();
    reverseSyncEditTxn.end("save", "reverse sync");
    reverseSyncSourceEditTxn.end("save", "reverse sync provenance");
    // Query scope ESA from database instead of reaching into private internals
    let scopeEsaResult = await ProvenanceManager.queryScopeExternalSourceAspect(
      targetDb,
      {
        id: undefined,
        classFullName: ExternalSourceAspect.classFullName,
        scope: { id: IModel.rootSubjectId },
        kind: ExternalSourceAspect.Kind.Scope,
        element: { id: IModel.rootSubjectId },
        identifier: sourceDb.iModelId,
      }
    );
    let scopeJsonProps = JSON.parse(scopeEsaResult?.jsonProperties ?? "{}");
    expect(scopeJsonProps.pendingSyncChangesetIndices?.length).to.equal(1);
    expect(scopeJsonProps.pendingSyncChangesetIndices[0]).to.equal(4);
    transformer.dispose();

    // Open sourceDb not at tip
    const tipChangesetOfSourceDb = sourceDb.changeset;
    sourceDb.close();
    const sourceDbNotAtTip = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: sourceIModelId,
      asOf: { afterChangeSetId: sourceDbChangesetNotAtTip.id },
    });
    expect(sourceDbNotAtTip.changeset).to.deep.equal(sourceDbChangesetNotAtTip);
    expect(sourceDbNotAtTip.changeset.index!).to.be.lessThan(
      tipChangesetOfSourceDb.index!
    );

    // Forward Sync. We expect 4 is still there because we didnt process it (as a result of our sourceDb not being at the tip)
    const forwardSyncEditTxn = createStartedEditTxn(targetDb);
    transformer = new IModelTransformer(
      { source: sourceDbNotAtTip, target: forwardSyncEditTxn },
      { argsForProcessChanges: {} }
    );
    await transformer.process();
    forwardSyncEditTxn.end();
    scopeEsaResult = await ProvenanceManager.queryScopeExternalSourceAspect(
      targetDb,
      {
        id: undefined,
        classFullName: ExternalSourceAspect.classFullName,
        scope: { id: IModel.rootSubjectId },
        kind: ExternalSourceAspect.Kind.Scope,
        element: { id: IModel.rootSubjectId },
        identifier: sourceDbNotAtTip.iModelId,
      }
    );
    scopeJsonProps = JSON.parse(scopeEsaResult?.jsonProperties ?? "{}");
    expect(scopeJsonProps.pendingSyncChangesetIndices).to.deep.equal([4]);
    transformer.dispose();
  });

  it("should properly delete element in master when element in branch is deleted alongside all of its ESAs.", async () => {
    // This test exercises elemIdToScopeESAs map in IModelTransformer.
    // create masterdb
    // create branch
    // insert multiple elements and relationships into master.
    // forward sync causing ESAs to be created for the elements and relationships.
    // delete all the aspects and the element that had those aspects on them in the branch
    // reverse sync.
    // expect that the correct element in master db was deleted.
    const masterIModelName = "MasterMultipleESAsDifferentKinds";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = { 1: 1, 2: 1 };
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    // masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);
    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };
    const timeline: Timeline = [
      { master: { seed: masterSeed } }, // masterSeedState is above
      { branch1: { branch: "master" } },
      { master: { 3: 3, 4: 4, 5: 5 } },
      {
        master: {
          manualUpdate(db) {
            // Create relationships in master iModel. Each one will introduce a new aspect of kind "Relationship".
            withEditTxn(
              db,
              "insert relationship provenance test data",
              (txn) => {
                const sourceId = IModelTestUtils.queryByUserLabel(db, "3");
                const targetId = IModelTestUtils.queryByUserLabel(db, "2");
                const targetId2 = IModelTestUtils.queryByUserLabel(db, "1");
                const targetId3 = IModelTestUtils.queryByUserLabel(db, "4");
                const targetId4 = IModelTestUtils.queryByUserLabel(db, "5");
                ElementGroupsMembers.insert(txn, sourceId, targetId);
                ElementGroupsMembers.insert(txn, sourceId, targetId2);
                ElementGroupsMembers.insert(txn, sourceId, targetId3);
                ElementGroupsMembers.insert(txn, sourceId, targetId4);
              }
            );
          },
        },
      },
      {
        branch1: {
          sync: ["master"],
        },
      }, // first master->branch1 forward sync picking up new relationship from master imodel
      {
        assert({ branch1 }) {
          const elemId = IModelTestUtils.queryByUserLabel(branch1.db, "3");
          const aspects = branch1.db.elements.getAspects(
            elemId,
            ExternalSourceAspect.classFullName
          ) as ExternalSourceAspect[];
          expect(aspects.length).to.be.equal(5); // 4 relationships + 1 element.
          aspects.forEach((a, index) => {
            if (index === 0)
              expect(a.kind).to.be.equal(ExternalSourceAspect.Kind.Element);
            else
              expect(a.kind).to.be.equal(
                ExternalSourceAspect.Kind.Relationship
              );
          });
        },
      },
      {
        branch1: {
          manualUpdate(db) {
            withEditTxn(
              db,
              "delete relationship provenance test data",
              (txn) => {
                const elemId = IModelTestUtils.queryByUserLabel(db, "3");
                const aspects = db.elements.getAspects(
                  elemId
                ) as ExternalSourceAspect[];
                aspects.forEach((a) => txn.deleteAspect(a.id));
                txn.deleteElement(elemId);
              }
            );
          },
        },
      },
      {
        master: {
          sync: ["branch1"],
        },
      }, // sync branch1 into master picking up deletes
      {
        assert({ master, branch1 }) {
          const elem = IModelTestUtils.queryByUserLabel(branch1.db, "3");
          expect(elem).to.be.equal(Id64.invalid);
          const elemInMaster = IModelTestUtils.queryByUserLabel(master.db, "3");
          expect(elemInMaster).to.be.equal(Id64.invalid);
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
      transformerOpts: {
        forceExternalSourceAspectProvenance: true,
      },
    });
    await tearDown();
  });

  it("should correctly reverse synchronize changes when targetDb was a clone of sourceDb", async () => {
    const seedFileName = path.join(outputDir, "seed.bim");
    if (IModelJsFs.existsSync(seedFileName))
      IModelJsFs.removeSync(seedFileName);

    const seedDb = SnapshotDb.createEmpty(seedFileName, {
      rootSubject: { name: "TransformerSource" },
    });
    const {
      subjectId1: _subjectId1,
      modelId1,
      categoryId1,
    } = withEditTxn(seedDb, "create seed elements", (txn) => {
      const subjId = Subject.insert(txn, IModel.rootSubjectId, "S1");
      const modId = PhysicalModel.insert(txn, subjId, "PM1");
      const catId = SpatialCategory.insert(txn, IModel.dictionaryId, "C1", {});
      const physicalElementProps1: PhysicalElementProps = {
        category: catId,
        model: modId,
        classFullName: PhysicalObject.classFullName,
        code: Code.createEmpty(),
      };
      txn.insertElement(physicalElementProps1);
      return { subjectId1: subjId, modelId1: modId, categoryId1: catId };
    });
    seedDb.close();

    let sourceIModelId: string | undefined;
    let targetIModelId: string | undefined;

    try {
      sourceIModelId = await IModelHost[_hubAccess].createNewIModel({
        iTwinId,
        iModelName: "TransformerSource",
        description: "source",
        version0: seedFileName,
        noLocks: true,
      });

      // open/upgrade sourceDb
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      // creating changesets for source
      for (let i = 0; i < 4; i++) {
        withEditTxn(sourceDb, `insert PhysicalObject ${i}`, (txn) => {
          const physicalElementProps: PhysicalElementProps = {
            category: categoryId1,
            model: modelId1,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
          };
          txn.insertElement(physicalElementProps);
        });
        await sourceDb.pushChanges({
          description: `Inserted ${i} PhysicalObject`,
        });
      }
      sourceDb.performCheckpoint(); // so we can use as a seed

      // forking target
      targetIModelId = await IModelHost[_hubAccess].createNewIModel({
        iTwinId,
        iModelName: "TransformerTarget",
        description: "target",
        version0: sourceDb.pathName,
        noLocks: true,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });

      // fork provenance init
      const forkInitEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer(
        { source: sourceDb, target: forkInitEditTxn },
        { wasSourceIModelCopiedToTarget: true }
      );
      await transformer.process();
      forkInitEditTxn.end();
      await targetDb.pushChanges({ description: "fork init" });
      transformer.dispose();

      const {
        targetSubjectId: _targetSubjectId,
        targetModelId,
        targetCategoryId,
      } = withEditTxn(targetDb, "create target elements", (txn) => {
        const subjId = Subject.insert(txn, IModel.rootSubjectId, "S2");
        const modId = PhysicalModel.insert(txn, subjId, "PM2");
        const catId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "C2",
          {}
        );
        return {
          targetSubjectId: subjId,
          targetModelId: modId,
          targetCategoryId: catId,
        };
      });

      // adding more changesets to target
      for (let i = 0; i < 2; i++) {
        withEditTxn(targetDb, `insert target PhysicalObject ${i}`, (txn) => {
          const targetPhysicalElementProps: PhysicalElementProps = {
            category: targetCategoryId,
            model: targetModelId,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
          };
          txn.insertElement(targetPhysicalElementProps);
        });
        await targetDb.pushChanges({
          description: `Inserted ${i} PhysicalObject`,
        });
      }

      // running reverse synchronization
      const reverseSyncEditTxn = createStartedEditTxn(sourceDb);
      const reverseSyncSourceEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: targetDb, target: reverseSyncEditTxn },
        { argsForProcessChanges: {}, sourceEditTxn: reverseSyncSourceEditTxn }
      );

      await transformer.process();
      transformer.dispose();
      reverseSyncEditTxn.end();
      reverseSyncSourceEditTxn.end();

      expect(count(sourceDb, PhysicalObject.classFullName)).to.equal(7);
      expect(count(targetDb, PhysicalObject.classFullName)).to.equal(7);

      expect(count(sourceDb, Subject.classFullName)).to.equal(2 + 1); // 2 inserted manually + root subject
      expect(count(targetDb, Subject.classFullName)).to.equal(2 + 1); // 2 inserted manually + root subject

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
          await IModelHost[_hubAccess].deleteIModel({
            iTwinId,
            iModelId: sourceIModelId,
          });
        if (targetIModelId)
          await IModelHost[_hubAccess].deleteIModel({
            iTwinId,
            iModelId: targetIModelId,
          });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should delete branch-deleted elements in reverse synchronization", async () => {
    const masterIModelName = "ReSyncDeleteMaster";
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: masterIModelName,
      noLocks: true,
    });
    let branchIModelId!: GuidString;
    assert.isTrue(Guid.isGuid(masterIModelId));

    try {
      const masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });

      // populate master
      const {
        categId: _categId,
        modelToDeleteWithElemId,
        elemInModelToDeleteId,
        notDeletedModelId,
        elemToDeleteWithChildrenId,
        childElemOfDeletedId,
        childSubjectId,
        modelInChildSubjectId,
        childSubjectChildId,
        modelInChildSubjectChildId,
      } = withEditTxn(masterDb, "setup master data", (txn) => {
        const catId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "category",
          new SubCategoryAppearance()
        );
        const modelToDelWithElemId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "model-to-delete-with-elem"
        );
        const makePhysObjCommonProps = (num: number) =>
          ({
            classFullName: PhysicalObject.classFullName,
            category: catId,
            geom: IModelTransformerTestUtils.createBox(
              Point3d.create(num, num, num)
            ),
            placement: {
              origin: Point3d.create(num, num, num),
              angles: YawPitchRollAngles.createDegrees(num, num, num),
            },
          }) as const;
        const elemInModelToDelId = new PhysicalObject(
          {
            ...makePhysObjCommonProps(1),
            model: modelToDelWithElemId,
            code: new Code({
              spec: IModelDb.rootSubjectId,
              scope: IModelDb.rootSubjectId,
              value: "elem-in-model-to-delete",
            }),
            userLabel: "elem-in-model-to-delete",
          },
          masterDb
        ).insert(txn);
        const notDelModelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "not-deleted-model"
        );
        const elemToDelWithChildrenId = new PhysicalObject(
          {
            ...makePhysObjCommonProps(2),
            model: notDelModelId,
            code: new Code({
              spec: IModelDb.rootSubjectId,
              scope: IModelDb.rootSubjectId,
              value: "deleted-elem-with-children",
            }),
            userLabel: "deleted-elem-with-children",
          },
          masterDb
        ).insert(txn);
        const childElemOfDelId = new PhysicalObject(
          {
            ...makePhysObjCommonProps(3),
            model: notDelModelId,
            code: new Code({
              spec: IModelDb.rootSubjectId,
              scope: IModelDb.rootSubjectId,
              value: "child-elem-of-deleted",
            }),
            userLabel: "child-elem-of-deleted",
            parent: new ElementOwnsChildElements(elemToDelWithChildrenId),
          },
          masterDb
        ).insert(txn);
        const childSubjId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "child-subject"
        );
        const modelInChildSubjId = PhysicalModel.insert(
          txn,
          childSubjId,
          "model-in-child-subject"
        );
        const childSubjChildId = Subject.insert(
          txn,
          childSubjId,
          "child-subject-child"
        );
        const modelInChildSubjChildId = PhysicalModel.insert(
          txn,
          childSubjChildId,
          "model-in-child-subject-child"
        );
        return {
          categId: catId,
          modelToDeleteWithElemId: modelToDelWithElemId,
          elemInModelToDeleteId: elemInModelToDelId,
          notDeletedModelId: notDelModelId,
          elemToDeleteWithChildrenId: elemToDelWithChildrenId,
          childElemOfDeletedId: childElemOfDelId,
          childSubjectId: childSubjId,
          modelInChildSubjectId: modelInChildSubjId,
          childSubjectChildId: childSubjChildId,
          modelInChildSubjectChildId: modelInChildSubjChildId,
        };
      });
      masterDb.performCheckpoint();
      await masterDb.pushChanges({ accessToken, description: "setup master" });

      // create and initialize branch from master
      const branchIModelName = "RevSyncDeleteBranch";
      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: branchIModelName,
        noLocks: true,
        version0: masterDb.pathName,
      });
      assert.isTrue(Guid.isGuid(branchIModelId));
      const branchDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: branchIModelId,
      });
      await branchDb.importSchemas([
        BisCoreSchema.schemaFilePath,
        GenericSchema.schemaFilePath,
      ]);
      assert.isTrue(
        branchDb.containsClass(ExternalSourceAspect.classFullName),
        "Expect BisCore to be updated and contain ExternalSourceAspect"
      );
      const branchInitEditTxn = createStartedEditTxn(branchDb);
      const provenanceInitializer = new IModelTransformer(
        { source: masterDb, target: branchInitEditTxn },
        { wasSourceIModelCopiedToTarget: true }
      );
      await provenanceInitializer.processSchemas();
      await provenanceInitializer.process();
      provenanceInitializer.dispose();
      branchInitEditTxn.end();
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

      withEditTxn(branchDb, "branch deletes", (txn) => {
        elemToDeleteWithChildren.entity.delete(txn);
        modelToDeleteWithElem.entity.delete(txn);
        deleteElementTree(txn, modelToDeleteWithElemId);
        deleteElementTree(txn, childSubjectId);
      });
      await branchDb.pushChanges({
        accessToken,
        description: "branch deletes",
      });

      // verify the branch state
      expect(branchDb.models.tryGetModel(modelToDeleteWithElemId)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(elemInModelToDeleteId)).to.be
        .undefined;
      expect(branchDb.models.tryGetModel(notDeletedModelId)).not.to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(elemToDeleteWithChildrenId)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(childElemOfDeletedId)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(childSubjectId)).to.be.undefined;
      expect(branchDb.elements.tryGetElement(modelInChildSubjectId)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(childSubjectChildId)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(modelInChildSubjectChildId)).to.be
        .undefined;

      // expected extracted changed ids
      const branchDbChangesets = await IModelHost[
        _hubAccess
      ].downloadChangesets({
        accessToken,
        iModelId: branchIModelId,
        targetDir: BriefcaseManager.getChangeSetsPath(branchIModelId),
      });
      expect(branchDbChangesets).to.have.length(2);
      const latestChangeset = branchDbChangesets[1];

      const changedInstanceIds = await ChangedInstanceIds.initialize({
        iModel: branchDb,
        csFileProps: [latestChangeset],
      });
      const result = changedInstanceIds;
      if (result === undefined) throw Error("expected to be defined");
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
      const expectedAspectDeleteIds = aspectDeletions.length
        ? new Set<Id64String>(aspectDeletions)
        : new Set<Id64String>();
      const expectedElementDeleteIds = new Set<Id64String>([
        modelToDeleteWithElemId,
        elemInModelToDeleteId,
        elemToDeleteWithChildrenId,
        childElemOfDeletedId,
        childSubjectId,
        modelInChildSubjectId,
        childSubjectChildId,
        modelInChildSubjectChildId,
      ]);
      const expectedModelDeleteIds = new Set<Id64String>([
        modelToDeleteWithElemId,
        modelInChildSubjectId,
        modelInChildSubjectChildId,
      ]);
      const expectedModelUpdateIds = new Set<Id64String>([
        IModelDb.rootSubjectId,
        notDeletedModelId,
      ]); // containing model will also get last modification time updated

      expect(result.aspect.deleteIds).to.deep.equal(expectedAspectDeleteIds);
      expect(result.element.deleteIds).to.deep.equal(expectedElementDeleteIds);
      expect(result.model.deleteIds).to.deep.equal(expectedModelDeleteIds);
      expect(result.model.updateIds).to.deep.equal(expectedModelUpdateIds);

      // NOTE: not using a targetScopeElementId because this test deals with temporary dbs, but that is a bad practice, use one
      // __PUBLISH_EXTRACT_START__ EditTxnInTransformer.reverse-synchronization
      // Reverse sync writes provenance to the source, so both databases need an EditTxn.
      const masterSyncEditTxn = createStartedEditTxn(masterDb);
      const reverseSyncSourceEditTxn = createStartedEditTxn(branchDb);
      const synchronizer = new IModelTransformer(
        { source: branchDb, target: masterSyncEditTxn },
        {
          argsForProcessChanges: {},
          sourceEditTxn: reverseSyncSourceEditTxn,
        }
      );
      await synchronizer.process();
      masterSyncEditTxn.end("save", "synchronize");
      reverseSyncSourceEditTxn.end("save", "synchronize provenance");
      // __PUBLISH_EXTRACT_END__
      await branchDb.pushChanges({ accessToken, description: "synchronize" });
      synchronizer.dispose();

      const getFromTarget = (
        sourceEntityId: Id64String,
        type: "elem" | "model"
      ) => {
        const sourceEntity = masterDb.elements.tryGetElement(sourceEntityId);
        if (sourceEntity === undefined) return undefined;
        const codeVal = sourceEntity.code.value;
        assert(
          codeVal !== undefined,
          "all tested elements must have a code value"
        );
        const targetId = IModelTransformerTestUtils.queryByCodeValue(
          masterDb,
          codeVal
        );
        if (Id64.isInvalid(targetId)) return undefined;
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
      expect(getFromTarget(modelInChildSubjectChildId, "model")).to.be
        .undefined;

      // close iModel briefcases
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
    } finally {
      // delete iModel briefcases
      await IModelHost[_hubAccess].deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
      if (branchIModelId) {
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
      }
    }
  });

  it("should not download more changesets than necessary", async () => {
    const timeline: Timeline = {
      0: { master: { 1: 1 } },
      1: { branch: { branch: "master" } },
      2: { branch: { 1: 2, 2: 1 } },
      3: { branch: { 3: 3 } },
    };

    const { trackedIModels, timelineStates, tearDown } = await runTimeline(
      timeline,
      { iTwinId, accessToken }
    );

    const master = trackedIModels.get("master")!;
    const branch = trackedIModels.get("branch")!;
    const branchAt2Changeset = timelineStates.get(1)?.changesets.branch;
    assert(branchAt2Changeset?.index);
    const branchAt2 = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: branch.id,
      asOf: { first: true },
    });
    await branchAt2.pullChanges({
      toIndex: branchAt2Changeset.index,
      accessToken,
    });

    const syncEditTxn = createStartedEditTxn(master.db);
    const reverseSyncSourceEditTxn = createStartedEditTxn(branchAt2);
    const syncer = new IModelTransformer(
      { source: branchAt2, target: syncEditTxn },
      {
        argsForProcessChanges: {
          startChangeset: branchAt2Changeset,
        },
        sourceEditTxn: reverseSyncSourceEditTxn,
      }
    );
    const queryChangeset = sinon.spy(BriefcaseManager, "queryChangeset");
    await syncer.process();
    expect(
      queryChangeset.alwaysCalledWith({
        iModelId: branch.id,
        changeset: {
          id: branchAt2Changeset.id,
        },
      })
    ).to.be.true;

    syncer.dispose();
    syncEditTxn.end();
    reverseSyncSourceEditTxn.end();
    await tearDown();
    sinon.restore();
  });

  it("should reverse synchronize forked iModel when an element was updated", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Master");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });

      const originalElementId = withEditTxn(
        sourceDb,
        "insert physical element",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PM1"
          );
          const physicalElement: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "Element1",
          };
          return txn.insertElement(physicalElement);
        }
      );
      await sourceDb.pushChanges({ description: "insert physical element" });

      const initialForkEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialForkEditTxn,
      });
      await transformer.process();
      const forkedElementId =
        transformer.context.findTargetElementId(originalElementId);
      expect(forkedElementId).not.to.be.undefined;
      transformer.dispose();
      initialForkEditTxn.end();
      await targetDb.pushChanges({ description: "initial transformation" });

      withEditTxn(targetDb, "update forked element", (txn) => {
        const forkedElement = targetDb.elements.getElement(forkedElementId);
        forkedElement.userLabel = "Element1_updated";
        forkedElement.update(txn);
      });
      await targetDb.pushChanges({
        description: "update forked element's userLabel",
      });

      const reverseForkEditTxn = createStartedEditTxn(sourceDb);
      const reverseForkSourceEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: targetDb, target: reverseForkEditTxn },
        {
          argsForProcessChanges: { startChangeset: targetDb.changeset },
          sourceEditTxn: reverseForkSourceEditTxn,
        }
      );
      await transformer.process();
      reverseForkEditTxn.end();
      reverseForkSourceEditTxn.end();
      await sourceDb.pushChanges({
        description: "change processing transformation",
      });

      const masterElement = sourceDb.elements.getElement(originalElementId);
      expect(masterElement).to.not.be.undefined;
      expect(masterElement.userLabel).to.be.equal("Element1_updated");
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should preserve FederationGuid when element is recreated within the same changeset and across changesets", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Source");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });

      const constSubjectFedGuid = Guid.createValue();
      const constPartitionFedGuid = Guid.createValue();
      const { originalSubjectId, originalPartitionId, originalModelId } =
        withEditTxn(sourceDb, "insert elements & models", (txn) => {
          const subjId = txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: constSubjectFedGuid,
            userLabel: "A",
          });

          const partId = txn.insertElement({
            model: IModel.repositoryModelId,
            code: PhysicalPartition.createCode(
              sourceDb,
              IModel.rootSubjectId,
              "original partition"
            ),
            classFullName: PhysicalPartition.classFullName,
            federationGuid: constPartitionFedGuid,
            parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
          });
          const modId = txn.insertModel({
            classFullName: PhysicalModel.classFullName,
            modeledElement: { id: partId },
            isPrivate: true,
          });
          return {
            originalSubjectId: subjId,
            originalPartitionId: partId,
            originalModelId: modId,
          };
        });
      await sourceDb.pushChanges({ description: "inserted elements & models" });

      const initialTargetEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialTargetEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      initialTargetEditTxn.end();
      await targetDb.pushChanges({ description: "initial transformation" });

      const originalTargetElement = targetDb.elements.getElement<Subject>(
        { federationGuid: constSubjectFedGuid },
        Subject
      );
      expect(originalTargetElement?.userLabel).to.equal("A");
      const originalTargetPartition =
        targetDb.elements.getElement<PhysicalPartition>(
          { federationGuid: constPartitionFedGuid },
          PhysicalPartition
        );
      expect(originalTargetPartition.code.value).to.be.equal(
        "original partition"
      );
      const originalTargetModel = targetDb.models.getModel<PhysicalModel>(
        originalTargetPartition.id,
        PhysicalModel
      );
      expect(originalTargetModel.isPrivate).to.be.true;

      const {
        secondCopyOfSubjectId,
        recreatedPartitionId: _recreatedPartitionId,
      } = withEditTxn(sourceDb, "recreate elements & models", (txn) => {
        txn.deleteElement(originalSubjectId);
        const secondSubjId = txn.insertElement({
          classFullName: Subject.classFullName,
          code: Code.createEmpty(),
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
          federationGuid: constSubjectFedGuid,
          userLabel: "B",
        });

        txn.deleteModel(originalModelId);
        txn.deleteElement(originalPartitionId);
        const recPartId = txn.insertElement({
          model: IModel.repositoryModelId,
          code: PhysicalPartition.createCode(
            sourceDb,
            IModel.rootSubjectId,
            "recreated partition"
          ),
          classFullName: PhysicalPartition.classFullName,
          federationGuid: constPartitionFedGuid,
          parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
        });
        txn.insertModel({
          classFullName: PhysicalModel.classFullName,
          modeledElement: { id: recPartId },
          isPrivate: false,
        });
        return {
          secondCopyOfSubjectId: secondSubjId,
          recreatedPartitionId: recPartId,
        };
      });
      await sourceDb.pushChanges({
        description: "recreated elements & models",
      });

      const changeTargetEditTxn1 = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changeTargetEditTxn1 },
        { argsForProcessChanges: { startChangeset: sourceDb.changeset } }
      );
      await transformer.process();
      changeTargetEditTxn1.end();
      await targetDb.pushChanges({
        description: "change processing transformation",
      });

      const targetElement = targetDb.elements.getElement<Subject>(
        { federationGuid: constSubjectFedGuid },
        Subject
      );
      expect(targetElement?.userLabel).to.equal("B");
      const targetPartition = targetDb.elements.getElement<PhysicalPartition>(
        { federationGuid: constPartitionFedGuid },
        PhysicalPartition
      );
      expect(targetPartition.code.value).to.be.equal("recreated partition");
      const targetModel = targetDb.models.getModel<PhysicalModel>(
        targetPartition.id,
        PhysicalModel
      );
      expect(targetModel.isPrivate).to.be.false;

      expect(
        count(
          sourceDb,
          Subject.classFullName,
          `Parent.Id = ${IModel.rootSubjectId}`
        )
      ).to.equal(1);
      expect(
        count(
          targetDb,
          Subject.classFullName,
          `Parent.Id = ${IModel.rootSubjectId}`
        )
      ).to.equal(1);
      expect(count(sourceDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(targetDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(sourceDb, PhysicalModel.classFullName)).to.equal(1);
      expect(count(targetDb, PhysicalModel.classFullName)).to.equal(1);

      withEditTxn(sourceDb, "delete second copy of subject", (txn) => {
        txn.deleteElement(secondCopyOfSubjectId);
      });
      await sourceDb.pushChanges({
        description: "deleted the second copy of the subject",
      });
      const startChangeset = sourceDb.changeset;
      // readd the subject in a separate changeset
      withEditTxn(sourceDb, "insert third copy of subject", (txn) => {
        txn.insertElement({
          classFullName: Subject.classFullName,
          code: Code.createEmpty(),
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
          federationGuid: constSubjectFedGuid,
          userLabel: "C",
        });
      });
      await sourceDb.pushChanges({
        description: "inserted a third copy of the subject with userLabel C",
      });

      const changeTargetEditTxn2 = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changeTargetEditTxn2 },
        { argsForProcessChanges: { startChangeset } }
      );
      await transformer.process();
      changeTargetEditTxn2.end();
      await targetDb.pushChanges({ description: "transformation" });

      const thirdCopySubject = targetDb.elements.getElement<Subject>(
        { federationGuid: constSubjectFedGuid },
        Subject
      );
      expect(thirdCopySubject?.userLabel).to.equal("C");
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should delete model when its partition was recreated, but model was left deleted", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Source");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });

      const constPartitionFedGuid = Guid.createValue();
      const { originalPartitionId, modelId } = withEditTxn(
        sourceDb,
        "insert elements & models",
        (txn) => {
          const partId = txn.insertElement({
            model: IModel.repositoryModelId,
            code: PhysicalPartition.createCode(
              sourceDb,
              IModel.rootSubjectId,
              "original partition"
            ),
            classFullName: PhysicalPartition.classFullName,
            federationGuid: constPartitionFedGuid,
            parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
          });
          const modId = txn.insertModel({
            classFullName: PhysicalModel.classFullName,
            modeledElement: { id: partId },
            isPrivate: true,
          });
          return { originalPartitionId: partId, modelId: modId };
        }
      );
      await sourceDb.pushChanges({ description: "inserted elements & models" });

      const initialTargetEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialTargetEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      initialTargetEditTxn.end();
      await targetDb.pushChanges({ description: "initial transformation" });

      const originalTargetPartition =
        targetDb.elements.getElement<PhysicalPartition>(
          { federationGuid: constPartitionFedGuid },
          PhysicalPartition
        );
      expect(originalTargetPartition.code.value).to.be.equal(
        "original partition"
      );
      const originalTargetModel = targetDb.models.getModel<PhysicalModel>(
        originalTargetPartition.id,
        PhysicalModel
      );
      expect(originalTargetModel.isPrivate).to.be.true;

      withEditTxn(sourceDb, "recreate elements & models", (txn) => {
        txn.deleteModel(modelId);
        txn.deleteElement(originalPartitionId);
        txn.insertElement({
          model: IModel.repositoryModelId,
          code: PhysicalPartition.createCode(
            sourceDb,
            IModel.rootSubjectId,
            "recreated partition"
          ),
          classFullName: PhysicalPartition.classFullName,
          federationGuid: constPartitionFedGuid,
          parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
        });
      });
      await sourceDb.pushChanges({
        description: "recreated elements & models",
      });

      const changeTargetEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changeTargetEditTxn },
        { argsForProcessChanges: { startChangeset: sourceDb.changeset } }
      );
      await transformer.process();
      changeTargetEditTxn.end();
      await targetDb.pushChanges({
        description: "change processing transformation",
      });

      const targetPartition = targetDb.elements.getElement<PhysicalPartition>(
        { federationGuid: constPartitionFedGuid },
        PhysicalPartition
      );
      expect(targetPartition.code.value).to.be.equal("recreated partition");

      expect(count(sourceDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(targetDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(sourceDb, PhysicalModel.classFullName)).to.equal(0);
      expect(count(targetDb, PhysicalModel.classFullName)).to.equal(0);
    } finally {
      try {
        // delete iModel briefcases
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should update aspects when processing changes and detachedAspectProcessing is turned on", async () => {
    let elementIds: Id64String[] = [];
    const aspectIds: Id64String[] = [];
    const sourceIModelId = await createPopulatedIModelHubIModel(
      "TransformerSource",
      (sourceSeedDb) => {
        elementIds = withEditTxn(
          sourceSeedDb,
          "seed source subjects and aspects",
          (txn) => {
            const createdElementIds = [
              Subject.insert(txn, IModel.rootSubjectId, "Subject1"),
              Subject.insert(txn, IModel.rootSubjectId, "Subject2"),
            ];

            // 10 aspects in total (5 per element)
            createdElementIds.forEach((element) => {
              for (let i = 0; i < 5; ++i) {
                const aspectProps: ExternalSourceAspectProps = {
                  classFullName: ExternalSourceAspect.classFullName,
                  element: new ElementOwnsExternalSourceAspects(element),
                  identifier: `${i}`,
                  kind: "Document",
                  scope: {
                    id: IModel.rootSubjectId,
                    relClassName:
                      "BisCore:ElementScopesExternalSourceIdentifier",
                  },
                };

                const aspectId = txn.insertAspect(aspectProps);
                aspectIds.push(aspectId); // saving for later deletion
              }
            });

            return createdElementIds;
          }
        );
      }
    );

    const targetIModelId =
      await createPopulatedIModelHubIModel("TransformerTarget");

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });

      const exporter = new IModelExporter(
        sourceDb,
        DetachedExportElementAspectsStrategy
      );
      // First transformation uses processAll (no argsForProcessChanges) to establish provenance
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      const transformer = new IModelTransformer(
        { source: exporter, target: firstTransformEditTxn },
        { includeSourceProvenance: true }
      );

      // run first transformation
      await transformer.process();
      firstTransformEditTxn.end();
      await saveAndPushChanges(targetDb, "First transformation");

      const addedAspectProps: ExternalSourceAspectProps = {
        classFullName: ExternalSourceAspect.classFullName,
        element: new ElementOwnsExternalSourceAspects(elementIds[0]),
        identifier: "aspectAddedAfterFirstTransformation",
        kind: "Document",
        scope: {
          id: IModel.rootSubjectId,
          relClassName: "BisCore:ElementScopesExternalSourceIdentifier",
        },
      };
      withEditTxn(sourceDb, "insert detached aspect", (txn) => {
        txn.insertAspect(addedAspectProps);
      });

      await saveAndPushChanges(sourceDb, "Update source");

      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      const transformer2 = new IModelTransformer(
        { source: exporter, target: secondTransformEditTxn },
        {
          includeSourceProvenance: true,
          argsForProcessChanges: {
            startChangeset: sourceDb.changeset,
          },
        }
      );
      await transformer2.process();
      secondTransformEditTxn.end();
      await saveAndPushChanges(targetDb, "Second transformation");

      const targetElementIds = targetDb.queryEntityIds({
        from: Subject.classFullName,
        where: "Parent.Id != ?",
        bindings: [IModel.rootSubjectId],
      });
      targetElementIds.forEach((elementId) => {
        const targetAspects = targetDb.elements.getAspects(
          elementId,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[];
        const sourceAspects = sourceDb.elements.getAspects(
          elementId,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[];
        expect(targetAspects.length).to.be.equal(sourceAspects.length + 1); // +1 because provenance aspect was added
        const aspectAddedAfterFirstTransformation = targetAspects.find(
          (aspect) =>
            aspect.identifier === "aspectAddedAfterFirstTransformation"
        );
        expect(aspectAddedAfterFirstTransformation).to.not.be.undefined;
      });
    } finally {
      await IModelHost[_hubAccess].deleteIModel({
        iTwinId,
        iModelId: sourceIModelId,
      });
      await IModelHost[_hubAccess].deleteIModel({
        iTwinId,
        iModelId: targetIModelId,
      });
    }
  });

  // will fix in separate PR, tracked here: https://github.com/iTwin/imodel-transformer/issues/27
  it.skip("should delete definition elements when processing changes", async () => {
    let spatialViewDefId: Id64String;
    let displayStyleId: Id64String;
    let spatialViewDef: SpatialViewDefinition;
    let displayStyle: DisplayStyle3d;

    const timeline: Timeline = {
      0: {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "create view definition test data", (txn) => {
              const modelSelector = ModelSelector.create(
                db,
                IModelDb.dictionaryId,
                "modelSelector",
                []
              );
              const modelSelectorId = txn.insertElement(modelSelector.toJSON());
              const categorySelectorId = CategorySelector.insert(
                txn,
                IModelDb.dictionaryId,
                "categorySelector",
                []
              );
              displayStyle = DisplayStyle3d.create(
                db,
                IModelDb.dictionaryId,
                "displayStyle"
              );
              displayStyleId = txn.insertElement(displayStyle.toJSON());
              spatialViewDefId = txn.insertElement({
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
              spatialViewDef =
                db.elements.getElement<SpatialViewDefinition>(spatialViewDefId);
            });
          },
        },
      },
      1: { branch: { branch: "master" } },
      2: {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "delete view definition test data", (txn) => {
              const notDeleted = txn.deleteDefinitionElements([
                spatialViewDefId,
                displayStyleId,
              ]);
              assert(notDeleted.size === 0);
            });
          },
        },
      },
      3: { branch: { sync: ["master", { since: 2 }] } },
    };

    const { trackedIModels, tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    const master = trackedIModels.get("master")!;
    const branch = trackedIModels.get("branch")!;

    expect(master.db.elements.tryGetElement(spatialViewDef!.code)).to.be
      .undefined;
    expect(master.db.elements.tryGetElement(displayStyle!.code)).to.be
      .undefined;

    expect(branch.db.elements.tryGetElement(spatialViewDef!.code)).to.be
      .undefined;
    expect(branch.db.elements.tryGetElement(displayStyle!.code)).to.be
      .undefined;

    await tearDown();
    sinon.restore();
  });

  // Regression test for https://github.com/iTwin/imodel-transformer/issues/28
  it("should succeed when element is deleted and element with the same code is re-added in the next changeset", async () => {
    let categoryId: Id64String;
    let modelId: Id64String;
    let elementId: Id64String;
    let displayStyleId: Id64String;

    const timeline: Timeline = {
      0: {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "create display style regression data", (txn) => {
              categoryId = SpatialCategory.insert(
                txn,
                IModel.dictionaryId,
                "TestCategory",
                {}
              );
              modelId = PhysicalModel.insert(
                txn,
                IModel.rootSubjectId,
                "TestPhysicalModel"
              );
              const physicalObjectProps: PhysicalElementProps = {
                classFullName: PhysicalObject.classFullName,
                model: modelId,
                category: categoryId,
                code: Code.createEmpty(),
                userLabel: "TestElement",
                geom: IModelTransformerTestUtils.createBox(
                  Point3d.create(1, 1, 1)
                ),
                placement: Placement3d.fromJSON({
                  origin: { x: 0, y: 0 },
                  angles: {},
                }),
              };
              elementId = txn.insertElement(physicalObjectProps);
              displayStyleId = DisplayStyle3d.insert(
                txn,
                IModel.dictionaryId,
                "TestDisplayStyle",
                { excludedElements: [elementId] }
              );
            });
          },
        },
      },
      1: { branch: { branch: "master" } },
      2: {
        master: {
          manualUpdate(db) {
            // Delete the DisplayStyle3d and re-insert one with the same code
            withEditTxn(db, "replace display style", (txn) => {
              txn.deleteDefinitionElements([displayStyleId]);
              DisplayStyle3d.insert(
                txn,
                IModel.dictionaryId,
                "TestDisplayStyle",
                {
                  excludedElements: [elementId],
                }
              );
            });
          },
        },
      },
      3: { branch: { sync: ["master", { since: 2 }] } },
    };

    const { trackedIModels, tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    const branch = trackedIModels.get("branch")!;
    expect(
      count(branch.db, DisplayStyle3d.classFullName),
      "target should contain one DisplayStyle3d element"
    ).to.equal(1);

    await tearDown();
    sinon.restore();
  });

  it("should be able to handle a transformation which deletes a relationship and then elements of that relationship", async () => {
    const masterIModelName = "MasterDeleteRelAndEnds";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedState = {
      40: 1,
      2: 2,
      41: 3,
      42: 4,
    } as TimelineIModelElemState;
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    // masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);

    const noFedGuidElemIds = masterSeedDb.queryEntityIds({
      from: "Bis.Element",
      where: "UserLabel IN ('41', '42')",
    });
    for (const elemId of noFedGuidElemIds)
      masterSeedDb.withSqliteStatement(
        `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
        (s) => {
          expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
        }
      );
    masterSeedDb.performCheckpoint();

    // hard to check this without closing the db...
    const seedSecondConn = SnapshotDb.openFile(masterSeedDb.pathName);
    for (const elemId of noFedGuidElemIds)
      expect(seedSecondConn.elements.getElement(elemId).federationGuid).to.be
        .undefined;
    seedSecondConn.close();

    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };

    const expectedRelationships = [
      {
        sourceLabel: "40",
        targetLabel: "2",
        idInBranch: "not inserted yet",
        sourceFedGuid: true,
        targetFedGuid: true,
      },
      {
        sourceLabel: "41",
        targetLabel: "42",
        idInBranch: "not inserted yet",
        sourceFedGuid: false,
        targetFedGuid: false,
      },
    ];

    let aspectIdForRelationship: Id64String | undefined;
    const timeline: Timeline = [
      { master: { seed: masterSeed } },
      { branch: { branch: "master" } },
      {
        branch: {
          manualUpdate(db) {
            withEditTxn(db, "insert branch relationships", (txn) => {
              expectedRelationships.map(({ sourceLabel, targetLabel }, i) => {
                const sourceId = IModelTestUtils.queryByUserLabel(
                  db,
                  sourceLabel
                );
                const targetId = IModelTestUtils.queryByUserLabel(
                  db,
                  targetLabel
                );
                assert(sourceId && targetId);
                const rel = ElementGroupsMembers.create(
                  db,
                  sourceId,
                  targetId,
                  0
                );
                expectedRelationships[i].idInBranch = txn.insertRelationship(
                  rel.toJSON()
                );
              });
            });
          },
        },
      },
      { master: { sync: ["branch"] } }, // first master<-branch reverse sync
      {
        assert({ branch }) {
          // expectedRelationships[1] has no fedguids, so expect to find 2 esas. One for the relationship and one for the element's own provenance.
          const sourceId = IModelTestUtils.queryByUserLabel(
            branch.db,
            expectedRelationships[1].sourceLabel
          );
          const aspects = branch.db.elements.getAspects(
            sourceId,
            ExternalSourceAspect.classFullName
          ) as ExternalSourceAspect[];
          assert(aspects.length === 2);
          let foundElementEsa = false;
          for (const aspect of aspects) {
            if (aspect.kind === "Element") foundElementEsa = true;
            else if (aspect.kind === "Relationship")
              aspectIdForRelationship = aspect.id;
          }
          assert(
            aspectIdForRelationship &&
              Id64.isValid(aspectIdForRelationship) &&
              foundElementEsa
          );
        },
      },
      {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "delete relationships and elements", (txn) => {
              expectedRelationships.forEach(({ sourceLabel, targetLabel }) => {
                const sourceId = IModelTestUtils.queryByUserLabel(
                  db,
                  sourceLabel
                );
                const targetId = IModelTestUtils.queryByUserLabel(
                  db,
                  targetLabel
                );
                assert(sourceId && targetId);
                const rel = db.relationships.getInstance(
                  ElementGroupsMembers.classFullName,
                  { sourceId, targetId }
                );
                txn.deleteRelationship(rel.toJSON());
                txn.deleteElement(sourceId);
                txn.deleteElement(targetId);
              });
            });
          },
        },
      },
      { branch: { sync: ["master"] } }, // master->branch forward sync
      {
        assert({ branch }) {
          for (const rel of expectedRelationships) {
            expect(
              branch.db.relationships.tryGetInstance(
                ElementGroupsMembers.classFullName,
                rel.idInBranch
              ),
              `had ${rel.sourceLabel}->${rel.targetLabel}`
            ).to.be.undefined;
            const sourceId = IModelTestUtils.queryByUserLabel(
              branch.db,
              rel.sourceLabel
            );
            const targetId = IModelTestUtils.queryByUserLabel(
              branch.db,
              rel.targetLabel
            );
            // Since we deleted both elements in the previous manualUpdate
            assert(
              Id64.isInvalid(sourceId) && Id64.isInvalid(targetId),
              `SourceId is ${sourceId}, TargetId is ${targetId}. Expected both to be ${Id64.invalid}.`
            );
            expect(
              () =>
                branch.db.relationships.tryGetInstance(
                  ElementGroupsMembers.classFullName,
                  { sourceId, targetId }
                ),
              `had ${rel.sourceLabel}->${rel.targetLabel}`
            ).to.throw; // TODO: This shouldn't throw but it does in core due to failing to bind ids of 0.

            expect(() =>
              branch.db.elements.getAspect(aspectIdForRelationship!)
            ).to.throw(
              "not found",
              `Expected aspectId: ${aspectIdForRelationship} to no longer be present in branch imodel.`
            );
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

  it("should throw when pendingSyncChangesetIndices and pendingReverseSyncChangesetIndices are undefined and then not throw when they're undefined, but 'unsafe-migrate' is set.", async () => {
    let targetScopeProvenanceProps: ExternalSourceAspectProps | undefined;
    const setBranchRelationshipDataBehaviorToUnsafeMigrate = (
      transformer: IModelTransformer
    ) =>
      (transformer["_options"]["branchRelationshipDataBehavior"] =
        "unsafe-migrate");

    const timeline: Timeline = [
      { master: { 1: 1, 2: 2, 3: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2, 4: 1 } },
      {
        assert({ master, branch }) {
          const scopeProvenanceCandidates = branch.db.elements
            .getAspects(
              IModelDb.rootSubjectId,
              ExternalSourceAspect.classFullName
            )
            .filter(
              (a) =>
                (a as ExternalSourceAspect).identifier === master.db.iModelId
            );
          expect(scopeProvenanceCandidates).to.have.length(1);
          const targetScopeProvenance =
            scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

          expect(targetScopeProvenance).to.deep.subsetEqual({
            identifier: master.db.iModelId,
            version: `${master.db.changeset.id};${master.db.changeset.index}`,
            jsonProperties: JSON.stringify({
              pendingReverseSyncChangesetIndices: [1],
              pendingSyncChangesetIndices: [],
              reverseSyncVersion: ";0", // not synced yet
            }),
          } as ExternalSourceAspectProps);
          targetScopeProvenanceProps = targetScopeProvenance;
        },
      },
      {
        branch: {
          manualUpdate(branch) {
            // Check it fails without pendingReverseSync and pendingSync
            const missingPendings = JSON.stringify({
              pendingReverseSyncChangesetIndices: undefined,
              pendingSyncChangesetIndices: undefined,
              reverseSyncVersion: ";0",
            });
            withEditTxn(branch, "update target scope provenance", (txn) => {
              txn.updateAspect({
                ...targetScopeProvenanceProps!,
                jsonProperties: missingPendings as any,
              });
            });
          },
        },
      },
      {
        master: {
          // Our pendingReverseSyncChangesetIndices are undefined, so we expect to throw when we try to read them.
          sync: ["branch", { expectThrow: true }],
        },
      },
      {
        assert({ branch }) {
          const aspect = branch.db.elements.getAspect(
            targetScopeProvenanceProps!.id!
          );
          expect(aspect).to.not.be.undefined;
          expect((aspect as ExternalSourceAspect).jsonProperties).to.equal(
            JSON.stringify({
              pendingReverseSyncChangesetIndices: undefined,
              pendingSyncChangesetIndices: undefined,
              reverseSyncVersion: ";0",
            })
          );
        },
      },
      {
        master: {
          // Our pendingReverseSyncChangesetIndices are undefined, but our branchrelationshipdatabehavior is 'unsafe-migrate' so we expect the transformer to correct the issue.
          sync: [
            "branch",
            {
              expectThrow: false,
              initTransformer: setBranchRelationshipDataBehaviorToUnsafeMigrate,
            },
          ],
        },
      },
      {
        assert({ branch }) {
          const aspect = branch.db.elements.getAspect(
            targetScopeProvenanceProps!.id!
          );
          expect(aspect).to.not.be.undefined;
          const jsonProps = JSON.parse(
            (aspect as ExternalSourceAspect).jsonProperties!
          );
          expect((aspect as any).version).to.match(/;1$/);
          expect(jsonProps.reverseSyncVersion).to.match(/;3$/);
          expect(jsonProps).to.deep.subsetEqual({
            pendingReverseSyncChangesetIndices: [4],
            pendingSyncChangesetIndices: [2],
          });
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    await tearDown();
  });

  it("should set unsafeVersions correctly when branchRelationshipDataBehavior is 'unsafe-migrate'", async () => {
    let targetScopeProvenanceProps: ExternalSourceAspectProps | undefined;
    const setBranchRelationshipDataBehaviorToUnsafeMigrate = (
      transformer: IModelTransformer
    ) => {
      transformer["_options"]["branchRelationshipDataBehavior"] =
        "unsafe-migrate";
      transformer["_options"]["argsForProcessChanges"]![
        "unsafeFallbackReverseSyncVersion"
      ] = ";2";
      transformer["_options"]["argsForProcessChanges"]![
        "unsafeFallbackSyncVersion"
      ] = ";3";
    };

    const timeline: Timeline = [
      { master: { 1: 1, 2: 2, 3: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2, 4: 1 } },
      { branch: { 5: 1 } },
      {
        assert({ master, branch }) {
          const scopeProvenanceCandidates = branch.db.elements
            .getAspects(
              IModelDb.rootSubjectId,
              ExternalSourceAspect.classFullName
            )
            .filter(
              (a) =>
                (a as ExternalSourceAspect).identifier === master.db.iModelId
            );
          expect(scopeProvenanceCandidates).to.have.length(1);
          const targetScopeProvenance =
            scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

          expect(targetScopeProvenance).to.deep.subsetEqual({
            identifier: master.db.iModelId,
            version: `${master.db.changeset.id};${master.db.changeset.index}`,
            jsonProperties: JSON.stringify({
              pendingReverseSyncChangesetIndices: [1],
              pendingSyncChangesetIndices: [],
              reverseSyncVersion: ";0", // not synced yet
            }),
          } as ExternalSourceAspectProps);
          targetScopeProvenanceProps = targetScopeProvenance;
        },
      },
      {
        branch: {
          manualUpdate(branch) {
            // Check it fails without version now.
            withEditTxn(branch, "clear target scope provenance json", (txn) => {
              txn.updateAspect({
                ...targetScopeProvenanceProps!,
                jsonProperties: undefined,
              } as ExternalSourceAspectProps);
            });
          },
        },
      },
      {
        master: {
          // Reverse sync passing along our unsafeReverseSyncVersion which intentionally skips the changeset that added the element with userlabel 4.
          sync: [
            "branch",
            {
              initTransformer: setBranchRelationshipDataBehaviorToUnsafeMigrate,
            },
          ],
        },
      },
      {
        async assert({ master, branch }) {
          // Assert that we skipped the changeset: { branch: { 1: 2, 4: 1 } } during our reverse sync.
          const expectedState = { 1: 1, 2: 2, 3: 1, 5: 1 };
          expect(master.state).to.deep.equal(expectedState);
          expect(branch.state).to.deep.equal({ ...expectedState, 1: 2, 4: 1 });
          await assertElemState(master.db, expectedState);
          await assertElemState(branch.db, { ...expectedState, 1: 2, 4: 1 });
        },
      },
      // repeat all above for forward sync scenario!
      { master: { 2: 4, 6: 1 } },
      { master: { 7: 1 } },
      // Update our targetscopeprovenanceprops
      {
        assert({ master, branch }) {
          const scopeProvenanceCandidates = branch.db.elements
            .getAspects(
              IModelDb.rootSubjectId,
              ExternalSourceAspect.classFullName
            )
            .filter(
              (a) =>
                (a as ExternalSourceAspect).identifier === master.db.iModelId
            );
          expect(scopeProvenanceCandidates).to.have.length(1);
          const targetScopeProvenance =
            scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;
          targetScopeProvenanceProps = targetScopeProvenance;
        },
      },
      {
        branch: {
          manualUpdate(branch) {
            // Check it fails without version now.
            withEditTxn(
              branch,
              "clear target scope provenance version",
              (txn) => {
                txn.updateAspect({
                  ...targetScopeProvenanceProps!,
                  version: undefined,
                } as ExternalSourceAspectProps);
              }
            );
          },
        },
      },
      {
        branch: {
          // Reverse sync passing along our unsafeReverseSyncVersion which intentionally skips the changeset that added the element with userlabel 4.
          sync: [
            "master",
            {
              initTransformer: setBranchRelationshipDataBehaviorToUnsafeMigrate,
            },
          ],
        },
      },
      {
        async assert({ master, branch }) {
          // Assert that we skipped the changeset: { master: { 2: 4, 6: 1, } }, during our forward sync.. making it so those properties didn't make it to the branch.
          const expectedMasterState = { 1: 1, 2: 4, 3: 1, 5: 1, 6: 1, 7: 1 };
          const expectedBranchState = { 1: 2, 2: 2, 3: 1, 4: 1, 5: 1, 7: 1 };
          expect(master.state).to.deep.equal(expectedMasterState);
          expect(branch.state).to.deep.equal(expectedBranchState);
          await assertElemState(master.db, expectedMasterState);
          await assertElemState(branch.db, expectedBranchState);
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    await tearDown();
  });

  it("reverseSyncs should not push extra changesets if the only changeset to process is one found in the pendingReverseSyncIndices, even when handleUnsafeMigrate is true", async () => {
    const timeline: Timeline = [
      { master: { 1: 1, 2: 2, 3: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2, 4: 1 } },
      {
        master: {
          sync: ["branch"],
        },
      },
      {
        assert({ master, branch }) {
          expect(master.db.changeset.index).to.equal(2);
          expect(branch.db.changeset.index).to.equal(3);
          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(
            0
          );
          const expectedProps: TestUtils.ExpectedTargetScopeProvenanceProps = {
            pendingSyncChangesetIndices: [2],
            pendingReverseSyncChangesetIndices: [3],
            syncVersionIndex: "1",
            reverseSyncVersionIndex: "2",
          };
          IModelTestUtils.findAndAssertTargetScopeProvenance(
            master,
            branch,
            expectedProps
          );
        },
      },
      {
        master: {
          sync: ["branch"], // Sync again with no real changes in branch except for the ones made to update targetScopeProvenance
        },
      },
      {
        assert({ master, branch }) {
          expect(master.db.changeset.index).to.equal(2);
          expect(branch.db.changeset.index).to.equal(3);
          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(
            0
          );
          const expectedProps: TestUtils.ExpectedTargetScopeProvenanceProps = {
            pendingSyncChangesetIndices: [2],
            pendingReverseSyncChangesetIndices: [3],
            syncVersionIndex: "1",
            reverseSyncVersionIndex: "2",
          };
          IModelTestUtils.findAndAssertTargetScopeProvenance(
            master,
            branch,
            expectedProps
          );
        },
      },
      {
        master: {
          sync: [
            "branch",
            {
              initTransformer: (transformer) =>
                (transformer["_options"]["branchRelationshipDataBehavior"] =
                  "unsafe-migrate"),
            },
          ], // Sync again with no changes except for ones which may get made by unsafe-migrate.
        },
      },
      {
        assert({ master, branch }) {
          expect(master.db.changeset.index).to.equal(2);
          expect(branch.db.changeset.index).to.equal(3);
          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(
            0
          );
          const expectedProps: TestUtils.ExpectedTargetScopeProvenanceProps = {
            pendingSyncChangesetIndices: [2],
            pendingReverseSyncChangesetIndices: [3],
            syncVersionIndex: "1",
            reverseSyncVersionIndex: "2",
          };
          IModelTestUtils.findAndAssertTargetScopeProvenance(
            master,
            branch,
            expectedProps
          );
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    await tearDown();
  });

  it("should fail processingChanges on pre-version-tracking forks unless branchRelationshipDataBehavior is 'unsafe-migrate'", async () => {
    let targetScopeProvenanceProps: ExternalSourceAspectProps | undefined;
    let targetScopeElementId: Id64String | undefined;
    const setBranchRelationshipDataBehaviorToUnsafeMigrate = (
      transformer: IModelTransformer
    ) =>
      (transformer["_options"]["branchRelationshipDataBehavior"] =
        "unsafe-migrate");
    const timeline: Timeline = [
      { master: { 1: 1, 2: 2, 3: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2, 4: 1 } },
      {
        assert({ master, branch }) {
          const scopeProvenanceCandidates = branch.db.elements
            .getAspects(
              IModelDb.rootSubjectId,
              ExternalSourceAspect.classFullName
            )
            .filter(
              (a) =>
                (a as ExternalSourceAspect).identifier === master.db.iModelId
            );
          expect(scopeProvenanceCandidates).to.have.length(1);
          const targetScopeProvenance =
            scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

          expect(targetScopeProvenance).to.deep.subsetEqual({
            identifier: master.db.iModelId,
            version: `${master.db.changeset.id};${master.db.changeset.index}`,
            jsonProperties: JSON.stringify({
              pendingReverseSyncChangesetIndices: [1],
              pendingSyncChangesetIndices: [],
              reverseSyncVersion: ";0", // not synced yet
            }),
          } as ExternalSourceAspectProps);
          targetScopeProvenanceProps = targetScopeProvenance;

          targetScopeElementId = targetScopeProvenanceProps.scope.id;
        },
      },
      {
        branch: {
          manualUpdate(branch) {
            // Check it fails without jsonprops
            withEditTxn(branch, "clear branch target scope json", (txn) => {
              txn.updateAspect({
                ...targetScopeProvenanceProps!,
                jsonProperties: undefined,
              });
            });
          },
        },
      },
      {
        master: {
          // Reverse sync and reverse sync looks for a 'reverseSyncVersion' inside of jsonProperties which will be missing so expectthrow.
          sync: ["branch", { expectThrow: true }],
        },
      },
      {
        branch: {
          sync: ["master", { expectThrow: false }],
        },
      },
      {
        branch: {
          manualUpdate(branch) {
            // Check it fails without version now.
            withEditTxn(branch, "clear branch target scope version", (txn) => {
              txn.updateAspect({
                ...targetScopeProvenanceProps!,
                version: undefined,
              } as ExternalSourceAspectProps);
            });
          },
        },
      },
      {
        branch: {
          // Forward sync and forward sync looks for a prop 'version' on the ESA which will be missing so expect to throw.
          sync: ["master", { expectThrow: true }],
        },
      },
      {
        master: {
          sync: ["branch", { expectThrow: false }],
        },
      },
      {
        branch: {
          manualUpdate(branch) {
            // Remove both and make sure it passes with both removed + setallowNoBranchRelationshipData
            withEditTxn(
              branch,
              "clear branch target scope relationship data",
              (txn) => {
                txn.updateAspect({
                  ...targetScopeProvenanceProps!,
                  jsonProperties: undefined,
                  version: undefined,
                } as ExternalSourceAspectProps);
              }
            );
          },
        },
      },
      {
        branch: {
          sync: [
            "master",
            {
              expectThrow: false,
              initTransformer: setBranchRelationshipDataBehaviorToUnsafeMigrate,
            },
          ],
        },
      },
      {
        master: {
          sync: [
            "branch",
            {
              expectThrow: false,
              initTransformer: setBranchRelationshipDataBehaviorToUnsafeMigrate,
            },
          ],
        },
      },
      {
        async assert({ master, branch }) {
          expect(master.db.changeset.index).to.equal(3);
          expect(branch.db.changeset.index).to.equal(8);
          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(
            0
          );
          const sql = `
          SELECT e.ECInstanceId as elementId, COUNT(*) as aspectCount FROM bis.ExternalSourceAspect esa
          JOIN bis.Element e ON e.ECInstanceId=esa.Element.Id
          GROUP BY e.ECInstanceId
          `;
          const externalAspectCounts = async (db: IModelDb) => {
            const results = [];
            for await (const row of db.createQueryReader(sql)) {
              results.push(row.toRow());
            }
            return results;
          };

          expect(count(branch.db, "bis.ExternalSourceAspect")).to.be.equal(
            count(master.db, "bis.Element") + 1
          );
          expect(count(branch.db, "bis.Element")).to.be.equal(
            count(master.db, "bis.Element")
          );

          (await externalAspectCounts(branch.db)).forEach((value) => {
            const { elementId, aspectCount } = value;
            if (elementId === targetScopeElementId)
              expect(aspectCount).to.equal(2);
            else expect(aspectCount).to.equal(1);
          });

          const scopeProvenanceCandidates = branch.db.elements
            .getAspects(
              IModelDb.rootSubjectId,
              ExternalSourceAspect.classFullName
            )
            .filter(
              (a) =>
                (a as ExternalSourceAspect).identifier === master.db.iModelId
            );
          expect(scopeProvenanceCandidates).to.have.length(1);
          const targetScopeProvenance =
            scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

          expect(targetScopeProvenance.version).to.match(/;2$/);
          const targetScopeJsonProps = JSON.parse(
            targetScopeProvenance.jsonProperties
          );
          expect(targetScopeJsonProps).to.deep.subsetEqual({
            pendingReverseSyncChangesetIndices: [8],
            pendingSyncChangesetIndices: [3],
          });
          expect(targetScopeJsonProps.reverseSyncVersion).to.match(/;7$/);
        },
      },
      { branch: { sync: ["master"] } },
      { master: { sync: ["branch"] } },
      { branch: { 5: 1 } },
      { master: { sync: ["branch"] } },
      {
        async assert({ master, branch }) {
          const expectedState = { 1: 2, 2: 2, 3: 1, 4: 1, 5: 1 };
          expect(master.state).to.deep.equal(expectedState);
          expect(branch.state).to.deep.equal(expectedState);
          await assertElemState(master.db, expectedState);
          await assertElemState(branch.db, expectedState);
        },
      },
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

  for (const propagateRootElems of [true, false]) {
    it(`${
      propagateRootElems ? "should" : "shouldn't"
    } propagate changes to rootSubject, repositoryModel, realityDataSourcesModel when skipPropagateChangesToRootElements is set to ${!propagateRootElems}`, async () => {
      const timeline: Timeline = [
        { master: { 1: 1 } },
        { branch: { branch: "master" } },
        { branch: { 1: 2, 4: 1 } },
        {
          branch: {
            manualUpdate(branch) {
              withEditTxn(
                branch,
                "update root model and element props",
                (txn) => {
                  // Update models
                  const dictionaryId = IModelDb.dictionaryId;
                  const dict = branch.models.getModelProps(dictionaryId);
                  txn.updateModel({
                    ...dict,
                    jsonProperties: { test: 1 },
                  });

                  const repositoryModel = branch.models.getModelProps(
                    IModelDb.repositoryModelId
                  );
                  txn.updateModel({
                    ...repositoryModel,
                    jsonProperties: { test: 2 },
                  });

                  const realityDataSourcesModel =
                    branch.models.getModelProps("0xe");
                  txn.updateModel({
                    ...realityDataSourcesModel,
                    jsonProperties: { test: 3 },
                  });

                  // Update Elements now.
                  const rootSubjectFromBranch =
                    branch.elements.getElementProps<SubjectProps>("0x1");
                  txn.updateElement({
                    ...rootSubjectFromBranch,
                    description: "test description",
                    jsonProperties: { test: 4 },
                  } as SubjectProps);

                  const realityDataSourcesElement =
                    branch.elements.getElementProps("0xe");
                  txn.updateElement({
                    ...realityDataSourcesElement,
                    jsonProperties: { test: 5 },
                  });

                  const dictionaryElement = branch.elements.getElementProps(
                    IModelDb.dictionaryId
                  );
                  txn.updateElement({
                    ...dictionaryElement,
                    jsonProperties: { test: 6 },
                  });
                }
              );
            },
          },
        },
        { master: { sync: ["branch"] } },
        {
          assert({ master, branch }) {
            const dictionaryModelMaster = master.db.models.getModel(
              IModelDb.dictionaryId
            );
            const dictionaryModelBranch = branch.db.models.getModel(
              IModelDb.dictionaryId
            );
            expect(dictionaryModelMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 1 : undefined
            );
            expect(dictionaryModelBranch.jsonProperties.test).to.equal(1);

            const repositoryModelMaster = master.db.models.getModel(
              IModelDb.repositoryModelId
            );
            const repositoryModelBranch = branch.db.models.getModel(
              IModelDb.repositoryModelId
            );
            expect(repositoryModelMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 2 : undefined
            );
            expect(repositoryModelBranch.jsonProperties.test).to.equal(2);

            const realityDataSourcesModelMaster =
              master.db.models.getModel("0xe");
            const realityDataSourcesModelBranch =
              branch.db.models.getModel("0xe");
            expect(realityDataSourcesModelMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 3 : undefined
            );
            expect(realityDataSourcesModelBranch.jsonProperties.test).to.equal(
              3
            );

            const rootSubjectMaster = master.db.elements.getRootSubject();
            const rootSubjectBranch = branch.db.elements.getRootSubject();
            expect(rootSubjectMaster.description).to.equal(
              propagateRootElems ? "test description" : ""
            );
            expect(rootSubjectBranch.description).to.equal("test description");
            expect(rootSubjectMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 4 : undefined
            );
            expect(rootSubjectBranch.jsonProperties.test).to.equal(4);

            const realityDataSourcesElementMaster =
              master.db.elements.getElementProps("0xe");
            const realityDataSourcesElementBranch =
              branch.db.elements.getElementProps("0xe");
            expect(
              realityDataSourcesElementMaster.jsonProperties?.test
            ).to.equal(propagateRootElems ? 5 : undefined);
            expect(
              realityDataSourcesElementBranch.jsonProperties.test
            ).to.equal(5);

            const dictionaryElementMaster = master.db.elements.getElementProps(
              IModelDb.dictionaryId
            );
            const dictionaryElementBranch = branch.db.elements.getElementProps(
              IModelDb.dictionaryId
            );
            expect(dictionaryElementMaster.jsonProperties?.test).to.equal(
              propagateRootElems ? 6 : undefined
            );
            expect(dictionaryElementBranch.jsonProperties.test).to.equal(6);
          },
        },
      ];

      const { tearDown } = await runTimeline(timeline, {
        iTwinId,
        accessToken,
        transformerOpts: {
          skipPropagateChangesToRootElements: !propagateRootElems,
        },
      });

      await tearDown();
    });
  }

  for (const skipPropagateChangesToRootElements of [true, false]) {
    it(`should ${
      skipPropagateChangesToRootElements ? "skip" : "propagate"
    } a remapped root Subject update during processChanges and synchronize its children`, async () => {
      const sourceIModelId = await HubWrappers.createIModel(
        accessToken,
        iTwinId,
        IModelTransformerTestUtils.generateUniqueName(
          "RemappedRootProcessChangesSource"
        )
      );
      const targetIModelId = await HubWrappers.createIModel(
        accessToken,
        iTwinId,
        IModelTransformerTestUtils.generateUniqueName(
          "RemappedRootProcessChangesTarget"
        )
      );
      let sourceDb: BriefcaseDb | undefined;
      let targetDb: BriefcaseDb | undefined;

      try {
        sourceDb = await HubWrappers.downloadAndOpenBriefcase({
          accessToken,
          iTwinId,
          iModelId: sourceIModelId,
        });
        targetDb = await HubWrappers.downloadAndOpenBriefcase({
          accessToken,
          iTwinId,
          iModelId: targetIModelId,
        });
        await sourceDb.locks.acquireLocks({
          shared: "0x10",
          exclusive: "0x1",
        });
        await targetDb.locks.acquireLocks({
          shared: "0x10",
          exclusive: "0x1",
        });

        const sourceChildSubjectId = withEditTxn(
          sourceDb,
          "insert source child Subject and update root",
          (txn) => {
            const childSubjectId = Subject.insert(
              txn,
              IModel.rootSubjectId,
              "Source child"
            );
            const rootSubjectProps =
              sourceDb!.elements.getElementProps<SubjectProps>(
                IModel.rootSubjectId
              );
            rootSubjectProps.code = Subject.createCode(
              sourceDb!,
              IModel.rootSubjectId,
              "Source root"
            );
            rootSubjectProps.userLabel = "Source root";
            txn.updateElement(rootSubjectProps);
            return childSubjectId;
          }
        );
        const remappedTargetRootSubjectId = withEditTxn(
          targetDb,
          "insert remapped target Subject",
          (txn) => Subject.insert(txn, IModel.rootSubjectId, "Mapped root")
        );
        await sourceDb.pushChanges({
          accessToken,
          description: "insert source child Subject and update root",
          retainLocks: true,
        });
        await targetDb.pushChanges({
          accessToken,
          description: "insert remapped target Subject",
          retainLocks: true,
        });

        const initialTargetEditTxn = createStartedEditTxn(targetDb);
        let transformer = new IModelTransformer(
          { source: sourceDb, target: initialTargetEditTxn },
          {
            targetScopeElementId: remappedTargetRootSubjectId,
            skipPropagateChangesToRootElements: true,
          }
        );
        transformer.context.remapElement(
          IModel.rootSubjectId,
          remappedTargetRootSubjectId
        );
        await transformer.process();
        await transformer.updateSynchronizationVersion({
          initializeReverseSyncVersion: true,
        });
        const targetChildSubjectId =
          transformer.context.findTargetElementId(sourceChildSubjectId);
        transformer.dispose();
        initialTargetEditTxn.end();
        await targetDb.pushChanges({
          accessToken,
          description: "initial transformation",
          retainLocks: true,
        });

        const targetRootBeforeChanges = targetDb.elements.getElement<Subject>(
          remappedTargetRootSubjectId,
          Subject
        );
        const targetRootLabelBeforeChanges = targetRootBeforeChanges.userLabel;
        const targetRootElementAspectCountBeforeChanges =
          countElementExternalSourceAspects(
            targetDb,
            remappedTargetRootSubjectId
          );

        withEditTxn(sourceDb, "update source root and child Subject", (txn) => {
          const rootSubjectProps =
            sourceDb!.elements.getElementProps<SubjectProps>(
              IModel.rootSubjectId
            );
          rootSubjectProps.userLabel = "Updated source root";
          txn.updateElement(rootSubjectProps);

          const childSubjectProps =
            sourceDb!.elements.getElementProps<SubjectProps>(
              sourceChildSubjectId
            );
          childSubjectProps.userLabel = "Updated source child";
          txn.updateElement(childSubjectProps);
        });
        await sourceDb.pushChanges({
          accessToken,
          description: "update source root and child Subject",
          retainLocks: true,
        });

        const processChangesTargetEditTxn = createStartedEditTxn(targetDb);
        transformer = new IModelTransformer(
          { source: sourceDb, target: processChangesTargetEditTxn },
          {
            argsForProcessChanges: {},
            targetScopeElementId: remappedTargetRootSubjectId,
            skipPropagateChangesToRootElements,
          }
        );
        transformer.context.remapElement(
          IModel.rootSubjectId,
          remappedTargetRootSubjectId
        );
        await transformer.process();
        transformer.dispose();
        processChangesTargetEditTxn.end();

        const targetRootAfterChanges = targetDb.elements.getElement<Subject>(
          remappedTargetRootSubjectId,
          Subject
        );
        expect(targetRootAfterChanges.userLabel).to.equal(
          skipPropagateChangesToRootElements
            ? targetRootLabelBeforeChanges
            : "Updated source root"
        );
        expect(
          targetDb.elements.getElement<Subject>(targetChildSubjectId, Subject)
            .userLabel
        ).to.equal("Updated source child");
        if (skipPropagateChangesToRootElements) {
          const targetRootElementAspectCountAfterChanges =
            countElementExternalSourceAspects(
              targetDb,
              remappedTargetRootSubjectId
            );
          expect(targetRootElementAspectCountAfterChanges).to.equal(
            targetRootElementAspectCountBeforeChanges
          );
        }
      } finally {
        if (sourceDb)
          await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
        if (targetDb)
          await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
        await IModelHost[_hubAccess].deleteIModel({
          accessToken,
          iTwinId,
          iModelId: sourceIModelId,
        });
        await IModelHost[_hubAccess].deleteIModel({
          accessToken,
          iTwinId,
          iModelId: targetIModelId,
        });
      }
    });
  }

  it("should skip provenance changesets made to branch during reverse sync", async () => {
    const timeline: Timeline = [
      { master: { 1: 1 } },
      { master: { 2: 2 } },
      { master: { 3: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2, 4: 1 } },
      // eslint-disable-next-line @typescript-eslint/no-shadow
      {
        assert({ master, branch }) {
          expect(master.db.changeset.index).to.equal(3);
          expect(branch.db.changeset.index).to.equal(2);
          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(
            0
          );
          expect(count(branch.db, ExternalSourceAspect.classFullName)).to.equal(
            9
          );

          const scopeProvenanceCandidates = branch.db.elements
            .getAspects(
              IModelDb.rootSubjectId,
              ExternalSourceAspect.classFullName
            )
            .filter(
              (a) =>
                (a as ExternalSourceAspect).identifier === master.db.iModelId
            );
          expect(scopeProvenanceCandidates).to.have.length(1);
          const targetScopeProvenance =
            scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

          expect(targetScopeProvenance).to.deep.subsetEqual({
            identifier: master.db.iModelId,
            version: `${master.db.changeset.id};${master.db.changeset.index}`,
            jsonProperties: JSON.stringify({
              pendingReverseSyncChangesetIndices: [1],
              pendingSyncChangesetIndices: [],
              reverseSyncVersion: ";0", // not synced yet
            }),
          } as ExternalSourceAspectProps);
        },
      },
      { master: { sync: ["branch"] } },
      // eslint-disable-next-line @typescript-eslint/no-shadow
      {
        assert({ master, branch }) {
          expect(master.db.changeset.index).to.equal(4);
          expect(branch.db.changeset.index).to.equal(3);
          expect(count(master.db, ExternalSourceAspect.classFullName)).to.equal(
            0
          );
          // added because the root was modified
          expect(count(branch.db, ExternalSourceAspect.classFullName)).to.equal(
            10
          );

          const scopeProvenanceCandidates = branch.db.elements
            .getAspects(
              IModelDb.rootSubjectId,
              ExternalSourceAspect.classFullName
            )
            .filter(
              (a) =>
                (a as ExternalSourceAspect).identifier === master.db.iModelId
            );
          expect(scopeProvenanceCandidates).to.have.length(1);
          const targetScopeProvenance =
            scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;

          expect(targetScopeProvenance.version).to.match(/;3$/);
          const targetScopeJsonProps = JSON.parse(
            targetScopeProvenance.jsonProperties
          );
          expect(targetScopeJsonProps).to.deep.subsetEqual({
            pendingReverseSyncChangesetIndices: [3],
            pendingSyncChangesetIndices: [4],
          });
          expect(targetScopeJsonProps.reverseSyncVersion).to.match(/;2$/);
        },
      },
      { branch: { sync: ["master"] } },
      { branch: { 5: 1 } },
      { master: { sync: ["branch"] } },
      {
        async assert({ master, branch }) {
          const expectedState = { 1: 2, 2: 2, 3: 1, 4: 1, 5: 1 };
          expect(master.state).to.deep.equal(expectedState);
          expect(branch.state).to.deep.equal(expectedState);
          await assertElemState(master.db, expectedState);
          await assertElemState(branch.db, expectedState);
        },
      },
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
      { master: { 1: 1 } },
      {
        master: {
          manualUpdate(masterDb) {
            withEditTxn(
              masterDb,
              "insert random external source aspect",
              (txn) => {
                const elemId = IModelTestUtils.queryByUserLabel(masterDb, "1");
                txn.insertAspect({
                  classFullName: ExternalSourceAspect.classFullName,
                  element: { id: elemId },
                  scope: { id: IModel.dictionaryId },
                  kind: "Element",
                  identifier: "bar code",
                } as ExternalSourceAspectProps);
              }
            );
          },
        },
      },
      { branch: { branch: "master" } },
      { branch: { 1: deleted } },
      { master: { sync: ["branch"] } },
      {
        assert({ master, branch }) {
          for (const imodel of [branch, master]) {
            const elemId = IModelTestUtils.queryByUserLabel(imodel.db, "1");
            const name = imodel.id === master.id ? "master" : "branch";
            expect(elemId, `db ${name} did not delete ${elemId}`).to.equal(
              Id64.invalid
            );
          }
        },
      },
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
            withEditTxn(db, "create definition hierarchy", (txn) => {
              const definitionPartitionProps: InformationPartitionElementProps =
                {
                  classFullName: DefinitionPartition.classFullName,
                  model: IModel.repositoryModelId,
                  parent: new SubjectOwnsPartitionElements(
                    IModel.rootSubjectId
                  ),
                  code: Code.createEmpty(),
                };
              definitionPartitionId1 = txn.insertElement(
                definitionPartitionProps
              );
              definitionPartitionId2 = txn.insertElement(
                definitionPartitionProps
              );

              const definitionModelProps1: ModelProps = {
                classFullName: DefinitionModel.classFullName,
                modeledElement: { id: definitionPartitionId1 },
                parentModel: IModel.repositoryModelId,
              };
              definitionPartitionModelId1 = txn.insertModel(
                definitionModelProps1
              );

              const definitionModelProps2: ModelProps = {
                classFullName: DefinitionModel.classFullName,
                modeledElement: { id: definitionPartitionId2 },
                parentModel: IModel.repositoryModelId,
              };
              definitionPartitionModelId2 = txn.insertModel(
                definitionModelProps2
              );

              const definitionContainerProps1: DefinitionElementProps = {
                classFullName: DefinitionContainer.classFullName,
                model: definitionPartitionModelId1,
                code: Code.createEmpty(),
              };
              definitionContainerId1 = txn.insertElement(
                definitionContainerProps1
              );

              const definitionModelProps3: ModelProps = {
                classFullName: DefinitionModel.classFullName,
                modeledElement: { id: definitionContainerId1 },
                parentModel: definitionPartitionModelId1,
              };
              definitionContainerModelId1 = txn.insertModel(
                definitionModelProps3
              );
            });
          },
        },
      },
      1: { branch: { branch: "master" } },
      2: {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "delete definition models", (txn) => {
              txn.deleteModel(definitionContainerModelId1);
              txn.deleteModel(definitionPartitionModelId1);
            });
          },
        },
      },
      3: { branch: { sync: ["master"] } },
      4: {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "delete second definition model", (txn) => {
              txn.deleteModel(definitionPartitionModelId2);
            });
          },
        },
      },
      5: { branch: { sync: ["master"] } },
    };

    const { trackedIModels, tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    const master = trackedIModels.get("master")!;
    const branch = trackedIModels.get("branch")!;

    expect(master.db.models.tryGetModel(definitionContainerModelId1!)).to.be
      .undefined;
    expect(master.db.elements.tryGetElement(definitionContainerId1!)).to.be
      .undefined;
    expect(master.db.models.tryGetModel(definitionPartitionModelId1!)).to.be
      .undefined;
    expect(master.db.elements.tryGetElement(definitionPartitionId2!)).to.not.be
      .undefined;
    expect(master.db.models.tryGetModel(definitionPartitionModelId2!)).to.be
      .undefined;

    expect(branch.db.models.tryGetModel(definitionContainerModelId1!)).to.be
      .undefined;
    expect(branch.db.elements.tryGetElement(definitionContainerId1!)).to.be
      .undefined;
    expect(branch.db.models.tryGetModel(definitionPartitionModelId1!)).to.be
      .undefined;
    expect(branch.db.elements.tryGetElement(definitionPartitionId2!)).to.not.be
      .undefined;
    expect(branch.db.models.tryGetModel(definitionPartitionModelId2!)).to.be
      .undefined;

    await tearDown();
    sinon.restore();
  });

  it("should use the lastMod of provenanceDb's element as the provenance aspect version", async () => {
    const timeline: Timeline = [
      { master: { 1: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2 } },
      { master: { sync: ["branch"] } },
      {
        assert({ master, branch }) {
          const elem1InMaster = TestUtils.IModelTestUtils.queryByUserLabel(
            master.db,
            "1"
          );
          expect(elem1InMaster).not.to.be.undefined;
          const elem1InBranch = TestUtils.IModelTestUtils.queryByUserLabel(
            branch.db,
            "1"
          );
          expect(elem1InBranch).not.to.be.undefined;
          const lastModInMaster =
            master.db.elements.queryLastModifiedTime(elem1InMaster);

          const physElem1Esas = branch.db.elements.getAspects(
            elem1InBranch,
            ExternalSourceAspect.classFullName
          ) as ExternalSourceAspect[];
          expect(physElem1Esas).to.have.lengthOf(1);
          expect(physElem1Esas[0].version).to.equal(lastModInMaster);
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
      transformerOpts: { forceExternalSourceAspectProvenance: true },
    });

    await tearDown();
    sinon.restore();
  });

  it("should successfully process changes when codeValues are switched around between elements", async () => {
    const timeline: Timeline = [
      { master: { 1: 1, 2: 2, 3: 3 } },
      { branch: { branch: "master" } },
      {
        master: {
          manualUpdate(masterDb) {
            withEditTxn(masterDb, "swap element code values", (txn) => {
              const elem1Id = IModelTestUtils.queryByCodeValue(masterDb, "1");
              const elem2Id = IModelTestUtils.queryByCodeValue(masterDb, "2");
              const elem3Id = IModelTestUtils.queryByCodeValue(masterDb, "3");
              const elem1 = masterDb.elements.getElement(elem1Id);
              const elem2 = masterDb.elements.getElement(elem2Id);
              const elem3 = masterDb.elements.getElement(elem3Id);
              elem1.code.value = "tempValue"; // need a temp value to avoid conflicts
              txn.updateElement(elem1.toJSON());
              elem2.code.value = "1";
              txn.updateElement(elem2.toJSON());
              elem3.code.value = "2";
              txn.updateElement(elem3.toJSON());
              elem1.code.value = "3";
              txn.updateElement(elem1.toJSON());
            });
          },
        },
      },
      { branch: { sync: ["master"] } },
      {
        assert({ master, branch }) {
          for (const iModel of [branch, master]) {
            const elem1Id = IModelTestUtils.queryByCodeValue(iModel.db, "1");
            const elem2Id = IModelTestUtils.queryByCodeValue(iModel.db, "2");
            const elem3Id = IModelTestUtils.queryByCodeValue(iModel.db, "3");
            const elem1 = iModel.db.elements.getElement(elem1Id);
            const elem2 = iModel.db.elements.getElement(elem2Id);
            const elem3 = iModel.db.elements.getElement(elem3Id);
            expect(elem1.userLabel).to.equal("2");
            expect(elem2.userLabel).to.equal("3");
            expect(elem3.userLabel).to.equal("1");
          }
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, { iTwinId, accessToken });
    await tearDown();
  });

  it("should successfully process changes when Definition Elements' codeValues are switched around", async () => {
    const timeline: Timeline = [
      {
        master: {
          manualUpdate(masterDb) {
            withEditTxn(masterDb, "insert definition categories", (txn) => {
              const categoryA = SpatialCategory.create(
                masterDb,
                IModel.dictionaryId,
                "A"
              );
              const categoryB = SpatialCategory.create(
                masterDb,
                IModel.dictionaryId,
                "B"
              );
              categoryA.userLabel = "A";
              categoryB.userLabel = "B";
              txn.insertElement(categoryA.toJSON());
              txn.insertElement(categoryB.toJSON());
            });
          },
        },
      },
      { branch: { branch: "master" } },
      {
        master: {
          manualUpdate(masterDb) {
            withEditTxn(masterDb, "swap definition category codes", (txn) => {
              const categoryA = masterDb.elements.getElement(
                SpatialCategory.createCode(masterDb, IModel.dictionaryId, "A")
              );
              const categoryB = masterDb.elements.getElement(
                SpatialCategory.createCode(masterDb, IModel.dictionaryId, "B")
              );
              categoryA.code.value = "temp";
              txn.updateElement(categoryA.toJSON());
              categoryB.code.value = "A";
              txn.updateElement(categoryB.toJSON());
              categoryA.code.value = "B";
              txn.updateElement(categoryA.toJSON());
            });
          },
        },
      },
      { branch: { sync: ["master"] } },
      {
        assert({ master, branch }) {
          for (const iModel of [branch, master]) {
            const categoryA = iModel.db.elements.getElement(
              SpatialCategory.createCode(iModel.db, IModel.dictionaryId, "A")
            );
            const categoryB = iModel.db.elements.getElement(
              SpatialCategory.createCode(iModel.db, IModel.dictionaryId, "B")
            );
            expect(categoryA.userLabel).to.equal(
              "B",
              `categoryA.userlabel mismatch in ${iModel.db.name}`
            );
            expect(categoryB.userLabel).to.equal(
              "A",
              `categoryB.userlabel mismatch in ${iModel.db.name}`
            );
          }
        },
      },
    ];

    const { tearDown } = await runTimeline(timeline, { iTwinId, accessToken });
    await tearDown();
  });

  it("should successfully process changes when some parent and child elements have no changes in source and were deleted in target", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Source");
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Target");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: sourceIModelId,
    });
    const targetDb = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: targetIModelId,
    });

    const _changes1ParentSubjectId = withEditTxn(
      sourceDb,
      "change 1 source",
      (txn) => {
        const parentId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "Change 1: Parent"
        );
        Subject.insert(txn, parentId, "Change 1: Child");
        return parentId;
      }
    );
    await sourceDb.pushChanges({ description: "change 1" });
    const { targetChanges1ParentSubjectId, targetChanges1ChildSubjectId } =
      withEditTxn(targetDb, "change 1 target", (txn) => {
        const parentId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "Change 1: Parent"
        );
        const childId = Subject.insert(txn, parentId, "Change 1: Child");
        return {
          targetChanges1ParentSubjectId: parentId,
          targetChanges1ChildSubjectId: childId,
        };
      });

    // process change 1
    const initialTargetEditTxn = createStartedEditTxn(targetDb);
    let transformer = new IModelTransformer(
      { source: sourceDb, target: initialTargetEditTxn },
      { argsForProcessChanges: {}, wasSourceIModelCopiedToTarget: true }
    );
    await transformer.process();
    initialTargetEditTxn.end();

    // Update source iModel
    withEditTxn(sourceDb, "change 2 source", (txn) => {
      const parentId = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "Change 2: Parent"
      );
      Subject.insert(txn, parentId, "Change 2: Child");
    });
    await sourceDb.pushChanges({ description: "change 2" });

    // Update target iModel
    withEditTxn(targetDb, "delete subjects in target", (txn) => {
      txn.deleteElement([
        targetChanges1ChildSubjectId,
        targetChanges1ParentSubjectId,
      ]);
    });

    // process change 2
    const changeTargetEditTxn = createStartedEditTxn(targetDb);
    transformer = new IModelTransformer(
      { source: sourceDb, target: changeTargetEditTxn },
      { argsForProcessChanges: {} }
    );
    await expect(transformer.process()).to.be.eventually.fulfilled;
    changeTargetEditTxn.end();

    const queryReader = targetDb.createQueryReader(
      `SELECT COUNT(*) FROM ${Subject.classFullName}`
    );
    await queryReader.step();
    const subjectCount = queryReader.current.toArray()[0];
    expect(subjectCount).to.equal(3); // RootSubject + 2 created subjects
  });

  describe("addCustomChanges", () => {
    let sourceDb: BriefcaseDb;
    let targetDb: BriefcaseDb;

    beforeEach(async () => {
      sourceDb = await prepareBriefcase("source");
      targetDb = await prepareBriefcase("target");
    });

    afterEach(async () => {
      await closeAndDeleteBriefcase(sourceDb);
      await closeAndDeleteBriefcase(targetDb);
    });
    class CustomChangesTransformer extends IModelTransformer {
      public readonly editTxn: ReturnType<typeof createStartedEditTxn>;

      constructor(
        source: IModelDb,
        target: IModelDb,
        isChangeProcessing: boolean
      ) {
        const editTxn = createStartedEditTxn(target);
        const options: IModelTransformOptions = {
          includeSourceProvenance: true,
        };
        if (isChangeProcessing) {
          options.argsForProcessChanges = {};
        }
        const exporter = new IModelExporter(
          source,
          DetachedExportElementAspectsStrategy
        );
        super({ source: exporter, target: editTxn }, options);
        this.editTxn = editTxn;
      }

      public override async addCustomChanges(
        _sourceDbChanges: ChangedInstanceIds
      ) {}
    }

    it("should call addCustomChanges when processing changes after source and target id map is populated", async () => {
      // set up source
      const sourceModelId0 = withEditTxn(
        sourceDb,
        "insert source model",
        (txn) => PhysicalModel.insert(txn, IModel.rootSubjectId, "M0")
      );
      await sourceDb.pushChanges({
        description: "Initial source data",
        retainLocks: true,
      });

      // process all
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      let addChangesStub = sinon.stub(transformer, "addCustomChanges");
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "target changes for transformation 1",
        retainLocks: true,
      });
      expect(addChangesStub.calledOnce).to.be.false;

      // process changes
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      addChangesStub = sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (_sourceDbChanges) => {
          const targetId =
            transformer.context.findTargetElementId(sourceModelId0);
          expect(
            targetId,
            "addCustomChanges should be called only after elements are mapped in clone context"
          ).to.not.be.equal(Id64.invalid);
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "target changes for transformation 2",
        retainLocks: true,
      });
      expect(addChangesStub.calledOnce).to.be.true;
    });

    it("should update data in target correctly when custom changes are registered for models", async () => {
      // Arrange
      const {
        sourceSubjectId,
        physicalModel1Id,
        categoryId1,
        documentListModel,
      } = withEditTxn(
        sourceDb,
        "insert source subject model and category",
        (txn) => {
          const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
          return {
            sourceSubjectId: subjectId,
            physicalModel1Id: PhysicalModel.insert(txn, subjectId, "PM1"),
            categoryId1: SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              "C1",
              {}
            ),
            documentListModel: DocumentListModel.insert(txn, subjectId, "DL"),
          };
        }
      );
      // Create Drawing model hierarchy
      const parentDrawing = insertDrawingElement(
        sourceDb,
        documentListModel,
        "DrawingParent"
      );
      const childDrawing = insertDrawingElement(
        sourceDb,
        parentDrawing.id!,
        "DrawingChild"
      );
      const physicalElem1 = insertPhysicalElement(
        sourceDb,
        physicalModel1Id,
        categoryId1,
        "PhysicalOne"
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      transformer.exporter.excludeElement(documentListModel);
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(1);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(0);
      expect(IModelTestUtils.queryByCodeValue(targetDb, "PM1")).to.not.be.equal(
        Id64.invalid
      );
      assertElementsExistByCode(targetDb, [physicalElem1]);
      assertElementsDoNotExistByCode(targetDb, [parentDrawing, childDrawing]);

      // === Transformation 2: `process changes` transformation to insert excluded parent model ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            parentDrawing.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 2: inserted previously excluded model",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(2);
      assertModelExistsByName(targetDb, ["PM1", "DL", "DrawingParent"]);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(1);
      assertElementsExistByCode(targetDb, [physicalElem1, parentDrawing]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing]);

      // === Transformation 3: `process changes` transformation to include newly added model  ===
      // Act
      const physicalModel2Id = withEditTxn(
        sourceDb,
        "insert second physical model",
        (txn) => PhysicalModel.insert(txn, sourceSubjectId, "PM2")
      );
      const physicalElem2 = insertPhysicalElement(
        sourceDb,
        physicalModel2Id,
        categoryId1,
        "PhysicalTwo"
      );
      await sourceDb.pushChanges({
        description: "Added new physical model",
        retainLocks: true,
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            physicalModel2Id
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 3: inserted newly created model",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(3);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["PM1", "DL", "DrawingParent", "PM2"]);
      assertElementsExistByCode(targetDb, [
        physicalElem1,
        physicalElem2,
        parentDrawing,
      ]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing]);

      // === Transformation 4: `process changes` transformation to delete existing model  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            physicalModel1Id
          );
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            parentDrawing.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 4: delete exported model",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(1);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(0);
      assertModelExistsByName(targetDb, ["DL", "PM2"]);
      assertModelDoesNotExistsByName(targetDb, ["PM1", "DrawingParent"]);
      assertElementsExistByCode(targetDb, [physicalElem2]);
      assertElementsDoNotExistByCode(targetDb, [
        physicalElem1,
        parentDrawing,
        childDrawing,
      ]);
      // === Transformation 5: `process changes` transformation to delete existing model with newly added elements  ===
      const physicalElem3 = insertPhysicalElement(
        sourceDb,
        physicalModel2Id,
        categoryId1,
        "PhysicalThree"
      );
      await sourceDb.pushChanges({
        description: "Added new physical element into PM2",
        retainLocks: true,
      });
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            physicalModel2Id
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 5: delete model with newly added elements",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(0);
      assertModelDoesNotExistsByName(targetDb, ["PM2"]);
      assertElementsDoNotExistByCode(targetDb, [physicalElem2, physicalElem3]);
    });

    it("should update modeled element and its related data when custom changes are added for it's sub model", async function () {
      // === Transformation 1: Run `process all` transformation ===
      // Arrange
      const { sourceSubjectId, documentListModel } = withEditTxn(
        sourceDb,
        "insert source subject and document list",
        (txn) => {
          const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
          return {
            sourceSubjectId: subjectId,
            documentListModel: DocumentListModel.insert(txn, subjectId, "DL"),
          };
        }
      );
      const parentDrawing1 = insertDrawingElement(
        sourceDb,
        documentListModel,
        "ParentDrawing1"
      );
      const parentDrawing2 = insertDrawingElement(
        sourceDb,
        documentListModel,
        "ParentDrawing2"
      );
      const childDrawing1 = insertDrawingElement(
        sourceDb,
        parentDrawing1.id!,
        "ChildDrawing1"
      );
      const childDrawing2 = insertDrawingElement(
        sourceDb,
        parentDrawing1.id!,
        "ChildDrawing2"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        parentDrawing1.id!,
        "ParentAspect1"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        childDrawing1.id!,
        "TestAspect1"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        childDrawing2.id!,
        "TestAspect2"
      );
      insertElementGroupsElementsRelationship(
        sourceDb,
        parentDrawing1.id!,
        parentDrawing2.id!
      );

      insertElementGroupsElementsRelationship(
        sourceDb,
        childDrawing1.id!,
        childDrawing2.id!
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });
      // Act
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      // Exclude all drawings
      transformer.exporter.excludeElement(parentDrawing1.id!);
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      assertModelExistsByName(targetDb, ["DL", "ParentDrawing2"]);
      assertModelDoesNotExistsByName(targetDb, [
        "ParentDrawing1",
        "ChildDrawing1",
        "ChildDrawing2",
      ]);
      assertElementsDoNotExistByCode(targetDb, [
        parentDrawing1,
        childDrawing1,
        childDrawing2,
      ]);

      // === Transformation 2: `process changes` transformation to include first child element's sub model  ===
      // Act
      // insert first child and keep excluding second child
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      transformer.exporter.excludeElement(childDrawing2.id!);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            childDrawing1.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description:
          "Transformation 2: add first previously excluded child element",
        retainLocks: true,
      });

      assertModelExistsByName(targetDb, [
        "DL",
        "ParentDrawing1",
        "ParentDrawing2",
        "ChildDrawing1",
      ]);
      assertModelDoesNotExistsByName(targetDb, ["ChildDrawing2"]);
      assertElementsExistByCode(targetDb, [parentDrawing1, childDrawing1]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing2]);
      assertElementHasExpectedAspectCount(
        targetDb,
        childDrawing1.federationGuid!,
        1
      );
      assertElementHasExpectedAspectCount(
        targetDb,
        parentDrawing1.federationGuid!,
        1
      );
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);

      // === Transformation 3: `process changes` transformation to include second child element's sub model  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            childDrawing2.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description:
          "Transformation 2: add second previously excluded child element",
        retainLocks: true,
      });
      // Assert
      assertModelExistsByName(targetDb, [
        "DL",
        "ParentDrawing1",
        "ParentDrawing2",
        "ChildDrawing1",
        "ChildDrawing2",
      ]);
      assertElementsExistByCode(targetDb, [
        parentDrawing1,
        childDrawing1,
        childDrawing2,
      ]);
      assertElementHasExpectedAspectCount(
        targetDb,
        childDrawing2.federationGuid!,
        1
      );
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(2);

      // === Transformation 4: `process changes` transformation to delete first child element's sub model  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            childDrawing1.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 3: delete first child element's submodel",
        retainLocks: true,
      });
      assertModelExistsByName(targetDb, [
        "DL",
        "ParentDrawing1",
        "ParentDrawing2",
        "ChildDrawing2",
      ]);
      assertElementsExistByCode(targetDb, [parentDrawing1, childDrawing2]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing1]);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);
    });

    it("should update exported data correctly when custom changes are registered for elements", async function () {
      // Prepare source
      const {
        sourceSubjectId,
        categoryId1,
        physicalModel1Id,
        physicalModel2Id,
      } = withEditTxn(sourceDb, "insert source models and category", (txn) => {
        const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
        return {
          sourceSubjectId: subjectId,
          categoryId1: SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          ),
          physicalModel1Id: PhysicalModel.insert(txn, subjectId, "PM1"),
          physicalModel2Id: PhysicalModel.insert(txn, subjectId, "PM2"),
        };
      });
      const physicalElem1 = insertPhysicalElement(
        sourceDb,
        physicalModel1Id,
        categoryId1,
        "PhysicalOne"
      );
      const physicalElem2 = insertPhysicalElement(
        sourceDb,
        physicalModel2Id,
        categoryId1,
        "PhysicalTwo"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        physicalElem1.id!,
        "TestAspect1"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        physicalElem2.id!,
        "TestAspect2"
      );
      insertElementGroupsElementsRelationship(
        sourceDb,
        physicalElem1.id!,
        physicalElem2.id!
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      // will exclude 'PM2'
      transformer.exporter.excludeElement(physicalModel2Id);
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(1);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(0);
      assertModelExistsByName(targetDb, ["PM1"]);
      assertModelDoesNotExistsByName(targetDb, ["PM2"]);
      assertElementsExistByCode(targetDb, [physicalElem1]);
      assertElementsDoNotExistByCode(targetDb, [physicalElem2]);
      assertElementHasExpectedAspectCount(
        targetDb,
        physicalElem1.federationGuid!,
        1
      );

      // === Transformation 2: `process changes` transformation to include excluded element  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Inserted",
            physicalElem2.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 2: include previously excluded element",
        retainLocks: true,
      });

      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(2);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["PM1", "PM2"]);
      assertElementsExistByCode(targetDb, [physicalElem1, physicalElem2]);
      assertElementHasExpectedAspectCount(
        targetDb,
        physicalElem2.federationGuid!,
        1
      );

      // === Transformation 3: `process changes` transformation to include newly added element  ===
      const physicalModel3Id = withEditTxn(
        sourceDb,
        "insert third physical model",
        (txn) => PhysicalModel.insert(txn, sourceSubjectId, "PM3")
      );
      const physicalElem3 = insertPhysicalElement(
        sourceDb,
        physicalModel3Id,
        categoryId1,
        "PhysicalThree"
      );
      await sourceDb.pushChanges({
        description: "Added new model and physical element",
        retainLocks: true,
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          await sourceDbChanges.addCustomElementChange(
            "Inserted",
            physicalElem3.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 3: include newly added element",
        retainLocks: true,
      });

      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(3);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["PM1", "PM2", "PM3"]);
      assertElementsExistByCode(targetDb, [
        physicalElem1,
        physicalElem2,
        physicalElem3,
      ]);

      // === Transformation 4: `process changes` transformation to delete exported element  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Deleted",
            physicalElem1.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 4: delete exported element",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(3);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(0);
      assertModelExistsByName(targetDb, ["PM1", "PM2", "PM3"]);
      assertElementsExistByCode(targetDb, [physicalElem2, physicalElem3]);
      assertElementsDoNotExistByCode(targetDb, [physicalElem1]);
    });

    it("should reset element values when custom changes to update element are added", async function () {
      // Arrange
      const { categoryId1, physicalModel1Id, physicalModel2Id } = withEditTxn(
        sourceDb,
        "insert reset-test source data",
        (txn) => {
          const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
          return {
            categoryId1: SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              "C1",
              {}
            ),
            physicalModel1Id: PhysicalModel.insert(txn, subjectId, "PM1"),
            physicalModel2Id: PhysicalModel.insert(txn, subjectId, "PM2"),
          };
        }
      );
      const physicalElem1 = insertPhysicalElement(
        sourceDb,
        physicalModel1Id,
        categoryId1,
        "PhysicalOne"
      );
      const physicalElem2 = insertPhysicalElement(
        sourceDb,
        physicalModel2Id,
        categoryId1,
        "PhysicalTwo"
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      // === Transformation 2: `process changes` transformation to update other element  ===
      // Update element in target
      const physicalElem1InTargetProps = targetDb.elements.getElementProps(
        physicalElem1.federationGuid!
      );
      physicalElem1InTargetProps.userLabel = "Updated";
      withEditTxn(targetDb, "update target element", (txn) => {
        txn.updateElement(physicalElem1InTargetProps);
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Updated",
            physicalElem2.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 2: update other element",
        retainLocks: true,
      });

      let physicalElem1InTarget = targetDb.elements.tryGetElement(
        physicalElem1.federationGuid!
      );
      expect(physicalElem1InTarget).to.not.be.undefined;
      expect(physicalElem1InTarget!.userLabel).to.be.equal("Updated");

      // === Transformation 3: `process changes` transformation to update changed element  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Updated",
            physicalElem1.id!
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 2: update changed element",
        retainLocks: true,
      });

      physicalElem1InTarget = targetDb.elements.tryGetElement(
        physicalElem1.federationGuid!
      );
      expect(physicalElem1InTarget).to.not.be.undefined;
      expect(
        physicalElem1InTarget!.userLabel,
        "updated value should be reverted"
      ).to.be.equal("PhysicalOne");
    });

    it("should delete recreated model when custom delete change is registered for it", async () => {
      const constSubjectFedGuid = Guid.createValue();
      const constPartitionFedGuid = Guid.createValue();
      const { originalSubjectId, originalPartitionId, originalModelId } =
        withEditTxn(sourceDb, "insert original elements and model", (txn) => {
          const subjId = txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: constSubjectFedGuid,
            userLabel: "A",
          });
          const partId = txn.insertElement({
            model: IModel.repositoryModelId,
            code: PhysicalPartition.createCode(
              sourceDb,
              IModel.rootSubjectId,
              "original partition"
            ),
            classFullName: PhysicalPartition.classFullName,
            federationGuid: constPartitionFedGuid,
            parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
          });
          return {
            originalSubjectId: subjId,
            originalPartitionId: partId,
            originalModelId: txn.insertModel({
              classFullName: PhysicalModel.classFullName,
              modeledElement: { id: partId },
              isPrivate: true,
            }),
          };
        });

      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      // Assert
      expect(targetDb.elements.tryGetElement(constSubjectFedGuid)).to.not.be
        .undefined;
      expect(targetDb.elements.tryGetElement(constPartitionFedGuid)).to.not.be
        .undefined;
      expect(
        IModelTestUtils.count(targetDb, PhysicalModel.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["original partition"]);

      // === Transformation 1: Run `process all` transformation ===
      const { secondCopyOfSubjectId, recreatedPartitionId } = withEditTxn(
        sourceDb,
        "recreate elements and model",
        (txn) => {
          txn.deleteElement(originalSubjectId);
          const newSubjectId = txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: constSubjectFedGuid,
            userLabel: "B",
          });

          txn.deleteModel(originalModelId);
          txn.deleteElement(originalPartitionId);
          const newPartitionId = txn.insertElement({
            model: IModel.repositoryModelId,
            code: PhysicalPartition.createCode(
              sourceDb,
              IModel.rootSubjectId,
              "recreated partition"
            ),
            classFullName: PhysicalPartition.classFullName,
            federationGuid: constPartitionFedGuid,
            parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
          });
          txn.insertModel({
            classFullName: PhysicalModel.classFullName,
            modeledElement: { id: newPartitionId },
            isPrivate: false,
          });
          return {
            secondCopyOfSubjectId: newSubjectId,
            recreatedPartitionId: newPartitionId,
          };
        }
      );

      await sourceDb.pushChanges({
        description: "Recreated elements",
        retainLocks: true,
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      sinon
        .stub(transformer, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            recreatedPartitionId
          );
          await sourceDbChanges.addCustomElementChange(
            "Deleted",
            secondCopyOfSubjectId
          );
        });
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 2: inserted previously excluded model",
        retainLocks: true,
      });
      expect(targetDb.elements.tryGetElement(constSubjectFedGuid)).to.be
        .undefined;
      expect(targetDb.elements.tryGetElement(constPartitionFedGuid)).to.be
        .undefined;
      expect(
        IModelTestUtils.count(targetDb, PhysicalModel.classFullName)
      ).to.be.equal(0);
    });

    it("should handle custom changes when source iModel has no changesets", async () => {
      // set up source
      const subjectFedGuid1 = Guid.createValue();
      const subjectFedGuid2 = Guid.createValue();
      const { originalSubjectId1, originalSubjectId2 } = withEditTxn(
        sourceDb,
        "insert initial subjects",
        (txn) => ({
          originalSubjectId1: txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: subjectFedGuid1,
            userLabel: "A",
          }),
          originalSubjectId2: txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: subjectFedGuid2,
            userLabel: "B",
          }),
        })
      );

      // process all
      const transformer1 = new CustomChangesTransformer(
        sourceDb,
        targetDb,
        false
      );
      transformer1.exporter.excludeElement(originalSubjectId2);
      await transformer1.process();
      transformer1.editTxn.end();
      await targetDb.pushChanges({
        description: "target changes for process all transformation.",
        retainLocks: true,
      });
      expect(targetDb.elements.tryGetElement(subjectFedGuid1)).to.not.be
        .undefined;
      expect(targetDb.elements.tryGetElement(subjectFedGuid2)).to.be.undefined;

      // process changes
      const transformer2 = new CustomChangesTransformer(
        sourceDb,
        targetDb,
        true
      );
      const addChangesStub = sinon
        .stub(transformer2, "addCustomChanges")
        .callsFake(async (sourceDbChanges) => {
          // Assert that element mapping is set
          const targetId =
            transformer2.context.findTargetElementId(originalSubjectId1);
          expect(
            targetId,
            "addCustomChanges should be called only after elements are mapped in clone context"
          ).to.not.be.equal(Id64.invalid);
          await sourceDbChanges.addCustomElementChange(
            "Deleted",
            originalSubjectId1
          );
          await sourceDbChanges.addCustomElementChange(
            "Inserted",
            originalSubjectId2
          );
        });
      await transformer2.process();
      transformer2.editTxn.end();
      await targetDb.pushChanges({
        description: "target changes for process changes transformation.",
        retainLocks: true,
      });
      expect(addChangesStub.calledOnce).to.be.true;
      expect(targetDb.elements.tryGetElement(subjectFedGuid1)).to.be.undefined;
      expect(targetDb.elements.tryGetElement(subjectFedGuid2)).to.not.be
        .undefined;
    });

    function insertDrawingElement(
      iModel: IModelDb,
      documentListModelId: Id64String,
      drawingName: string
    ): ElementProps {
      const id = withEditTxn(iModel, `insert drawing ${drawingName}`, (txn) =>
        Drawing.insert(txn, documentListModelId, drawingName)
      );
      return iModel.elements.getElementProps(id);
    }

    function insertPhysicalElement(
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
      // re-read element to populate federationGuid value
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

      return withEditTxn(iModel, `insert aspect ${identifier}`, (txn) =>
        txn.insertAspect(aspectProps)
      );
    }

    function insertElementGroupsElementsRelationship(
      iModel: IModelDb,
      sourceId: Id64String,
      targetId: Id64String
    ) {
      const rel = ElementGroupsMembers.create(iModel, sourceId, targetId, 0);
      const id = withEditTxn(
        iModel,
        "insert element groups relationship",
        (txn) => txn.insertRelationship(rel.toJSON())
      );
      return iModel.relationships.getInstance(
        ElementGroupsMembers.classFullName,
        id
      );
    }

    function assertElementsExistByCode(
      iModel: IModelDb,
      properties: ElementProps[]
    ) {
      properties.forEach((elemProp) => {
        expect(elemProp.code.value).to.not.be.undefined;
        expect(
          IModelTestUtils.queryByCodeValue(iModel, elemProp.code.value!),
          `Element '${elemProp.code.value}' should exist in iModel.`
        ).to.not.be.equal(Id64.invalid);
      });
    }

    function assertModelExistsByName(iModel: IModelDb, names: string[]) {
      names.forEach((name) => {
        expect(
          IModelTestUtils.queryModelIddByModeledElementCodeValue(iModel, name),
          `Model '${name}' should exist in iModel.`
        ).to.not.be.equal(Id64.invalid);
      });
    }

    function assertModelDoesNotExistsByName(iModel: IModelDb, names: string[]) {
      names.forEach((name) => {
        expect(
          IModelTestUtils.queryModelIddByModeledElementCodeValue(iModel, name),
          `Model '${name}' should not exist in iModel.`
        ).to.be.equal(Id64.invalid);
      });
    }

    function assertElementsDoNotExistByCode(
      iModel: IModelDb,
      properties: ElementProps[]
    ) {
      properties.forEach((elemProp) => {
        expect(elemProp.code.value).to.not.be.undefined;
        expect(
          IModelTestUtils.queryByCodeValue(iModel, elemProp.code.value!),
          `Element '${elemProp.code.value}' should not exist in iModel.`
        ).to.be.equal(Id64.invalid);
      });
    }

    function assertElementHasExpectedAspectCount(
      iModel: IModelDb,
      federationGuid: GuidString,
      expectedAspectCount: number
    ) {
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
  });

  describe("processChanges", () => {
    let sourceDb: BriefcaseDb;
    let targetDb: BriefcaseDb;

    beforeEach(async () => {
      sourceDb = await prepareBriefcase("source");
      targetDb = await prepareBriefcase("target");
    });

    afterEach(async () => {
      await closeAndDeleteBriefcase(sourceDb);
      await closeAndDeleteBriefcase(targetDb);
    });

    it("should skip unchanged parent elements but still export changed child elements during processChanges", async () => {
      // Create a model with a parent element and a child element
      const { parentElementId, childElementId } = withEditTxn(
        sourceDb,
        "create model with parent and child elements",
        (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "TestPhysicalModel"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "TestCategory",
            {}
          );
          const parentId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "ParentElement",
          } as GeometricElementProps);
          const childId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "ChildElement",
            parent: new ElementOwnsChildElements(parentId),
          } as GeometricElementProps);
          return {
            physicalModelId: modelId,
            parentElementId: parentId,
            childElementId: childId,
          };
        }
      );
      await sourceDb.pushChanges({
        description: "Initial model and elements",
        retainLocks: true,
      });

      // Run initial processAll transformation
      const firstEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      firstEditTxn.end();
      await targetDb.pushChanges({
        description: "Initial transformation",
        retainLocks: true,
      });

      // Update only the child element (not the parent) to trigger a change
      withEditTxn(sourceDb, "update child element only", (txn) => {
        const childProps = sourceDb.elements.getElementProps(childElementId);
        txn.updateElement({
          ...childProps,
          userLabel: "ChildElement-Updated",
        });
      });
      await sourceDb.pushChanges({
        description: "Child element update",
        retainLocks: true,
      });

      // Run processChanges and spy on onExportElement
      const secondEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondEditTxn },
        { argsForProcessChanges: {} }
      );
      const onExportElementSpy = sinon.spy(transformer, "onExportElement");
      await transformer.process();

      // Verify: parent element was NOT exported (short-circuited)
      const parentWasExported = onExportElementSpy
        .getCalls()
        .some((call) => call.args[0].id === parentElementId);
      expect(
        parentWasExported,
        "onExportElement should not have been called for unchanged parent element"
      ).to.be.false;

      // Verify: child element WAS exported (still traversed through unchanged parent)
      const childWasExported = onExportElementSpy
        .getCalls()
        .some((call) => call.args[0].id === childElementId);
      expect(
        childWasExported,
        "onExportElement should have been called for changed child element"
      ).to.be.true;

      transformer.dispose();
      secondEditTxn.end();
    });

    it("should still export updated aspects when the owning element is unchanged during processChanges", async () => {
      // Import a schema with a custom UniqueAspect so we can test aspect-only updates
      // without interference from the provenance system
      const testSchemaPath =
        IModelTransformerTestUtils.getPathToSchemaWithUniqueAspect();
      await sourceDb.importSchemas([testSchemaPath]);
      await targetDb.importSchemas([testSchemaPath]);
      await sourceDb.pushChanges({
        description: "Import test schema",
        retainLocks: true,
      });
      await targetDb.pushChanges({
        description: "Import test schema",
        retainLocks: true,
      });

      // Create an element with a unique aspect
      const elementId = withEditTxn(
        sourceDb,
        "create element with unique aspect",
        (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "TestPhysicalModelForAspect"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "TestCategoryForAspect",
            {}
          );
          const elemId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "ElementWithUniqueAspect",
          } as GeometricElementProps);
          txn.insertAspect({
            classFullName: "TestSchema1:MyUniqueAspect",
            element: { id: elemId },
            myProp1: "original-value",
          } as any);
          return elemId;
        }
      );
      await sourceDb.pushChanges({
        description: "Initial element with unique aspect",
        retainLocks: true,
      });

      // Run initial processAll transformation
      const firstEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      firstEditTxn.end();
      await targetDb.pushChanges({
        description: "Initial transformation",
        retainLocks: true,
      });

      // Verify initial aspect value on target
      const targetElementId = IModelTestUtils.queryByUserLabel(
        targetDb,
        "ElementWithUniqueAspect"
      );
      const targetAspectsBefore = targetDb.elements.getAspects(
        targetElementId,
        "TestSchema1:MyUniqueAspect"
      );
      expect(targetAspectsBefore).to.have.lengthOf(1);
      expect((targetAspectsBefore[0] as any).myProp1).to.equal(
        "original-value"
      );

      // Update only the aspect (not the element directly)
      withEditTxn(sourceDb, "update unique aspect only", (txn) => {
        const aspects = sourceDb.elements.getAspects(
          elementId,
          "TestSchema1:MyUniqueAspect"
        );
        txn.updateAspect({
          ...aspects[0].toJSON(),
          myProp1: "updated-value",
        } as any);
      });
      await sourceDb.pushChanges({
        description: "Aspect-only update",
        retainLocks: true,
      });

      // Run processChanges — the aspect change should propagate to the target
      const secondEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondEditTxn },
        { argsForProcessChanges: {} }
      );
      await transformer.process();
      transformer.dispose();
      secondEditTxn.end();

      // Verify: the aspect on the target element was updated
      const targetAspectsAfter = targetDb.elements.getAspects(
        targetElementId,
        "TestSchema1:MyUniqueAspect"
      );
      expect(targetAspectsAfter).to.have.lengthOf(1);
      expect(
        (targetAspectsAfter[0] as any).myProp1,
        "target aspect should have been updated to 'updated-value' by processChanges"
      ).to.equal("updated-value");
    });

    it("should process changes successfully when element is deleted after existing elements were expanded into overflow table", async () => {
      // Import initial schema with property count that does not require overflow table
      const initialSchema = generateSchema(1, "SourceProperty", 5);
      await sourceDb.importSchemaStrings([initialSchema]);
      const elementId = createPhysicalElement(
        sourceDb,
        "DynamicTestSchema:DynamicPhysicalElement"
      );
      await sourceDb.pushChanges({
        description: "Initial schema and element creation",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await transformer.processSchemas();
      await transformer.process();
      firstTransformEditTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      // Assert that element was transformed
      const targetElement = IModelTestUtils.queryByUserLabel(
        targetDb,
        "TestClassElement"
      );
      expect(targetElement).to.not.equal(Id64.invalid);

      // Update schema: Add enough properties to spill into overflow table (more than 32)
      const expandedSchema = generateSchema(2, "SourceProperty", 100);
      await sourceDb.importSchemaStrings([expandedSchema]);
      await sourceDb.pushChanges({
        description: "Updated schema",
        retainLocks: true,
      });

      // Delete the element
      withEditTxn(sourceDb, "recreate elements & models", (txn) => {
        txn.deleteElement(elementId);
      });
      await sourceDb.pushChanges({
        description: "Deleted element",
        retainLocks: true,
      });

      // === Transformation 2: Run `process changes` transformation ===
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        { argsForProcessChanges: {} }
      );
      await transformer.processSchemas();
      await transformer.process();
      secondTransformEditTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 2: Process Changes with deletion",
        retainLocks: true,
      });

      // Assert: Verify element is deleted in target
      const targetElement2 = IModelTestUtils.queryByUserLabel(
        targetDb,
        "TestClassElement"
      );
      expect(
        targetElement2,
        "Element should be deleted in target iModel"
      ).to.equal(Id64.invalid);
    });

    it("should leave model contents correct when model partition was recreated with different federation guid and the same code value", async () => {
      // Arrange
      const specId = sourceDb.codeSpecs.getByName(
        BisCodeSpec.physicalMaterial
      ).id;
      const { subjectId, physicalModelId, categoryId, physicalObjectId } =
        withEditTxn(sourceDb, "recreate elements & models", (txn) => {
          // prepare source - create initial subject, model, and element
          const subjId = Subject.insert(txn, IModel.rootSubjectId, "Subject1");
          const physModId = PhysicalModel.insert(txn, subjId, "PhysicalModel");
          const catId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: physModId,
            category: catId,
            code: new Code({
              value: "PO1",
              scope: IModel.rootSubjectId,
              spec: specId,
            }),
          };
          const physicalObjId = txn.insertElement(physicalObjectProps);
          return {
            subjectId: subjId,
            physicalModelId: physModId,
            categoryId: catId,
            physicalObjectId: physicalObjId,
          };
        });
      await sourceDb.pushChanges({
        accessToken,
        description: "First changes",
        retainLocks: true,
      });

      // Run first transform
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      firstTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "First transformation",
        retainLocks: true,
      });

      // Recreate source model partition with different federation guid
      withEditTxn(sourceDb, "delete and recreate model", (txn) => {
        txn.deleteElement(physicalObjectId);
        txn.deleteModel(physicalModelId);
        txn.deleteElement(physicalModelId);
        const physicalModel2Id = PhysicalModel.insert(
          txn,
          subjectId,
          "PhysicalModel"
        );
        const physicalObject2Props: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: physicalModel2Id,
          category: categoryId,
          code: new Code({
            value: "PO2",
            scope: IModel.rootSubjectId,
            spec: specId,
          }),
        };
        txn.insertElement(physicalObject2Props);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Second changes",
      });

      // Act - run second transform with change processing
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        {
          argsForProcessChanges: {},
        }
      );
      await transformer.process();
      transformer.dispose();
      secondTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Second transformation",
      });

      // Assert - verify that new elements and models exist with correct values
      expect(
        IModelTransformerTestUtils.queryByCodeValue(targetDb, "PO2")
      ).to.not.be.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryModelIddByModeledElementCodeValue(
          targetDb,
          "PhysicalModel"
        )
      ).to.not.be.equal(Id64.invalid);
    });

    it("should delete model when model partition was recreated with different federation guid and the same code value but model was left deleted", async () => {
      // Arrange
      const specId = sourceDb.codeSpecs.getByName(
        BisCodeSpec.physicalMaterial
      ).id;
      const { subjectId, physicalModelId, physicalObjectId } = withEditTxn(
        sourceDb,
        "recreate elements & models",
        (txn) => {
          // prepare source - create initial subject, model, and element
          const subjId = Subject.insert(txn, IModel.rootSubjectId, "Subject1");
          const physModId = PhysicalModel.insert(txn, subjId, "PhysicalModel");
          const catId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: physModId,
            category: catId,
            code: new Code({
              value: "PO1",
              scope: IModel.rootSubjectId,
              spec: specId,
            }),
          };
          const physicalObjId = txn.insertElement(physicalObjectProps);
          return {
            subjectId: subjId,
            physicalModelId: physModId,
            physicalObjectId: physicalObjId,
          };
        }
      );
      await sourceDb.pushChanges({
        accessToken,
        description: "First changes",
        retainLocks: true,
      });

      // Run first transform
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      firstTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "First transformation",
        retainLocks: true,
      });

      // Recreate source model partition with different federation guid
      withEditTxn(sourceDb, "delete and recreate model", (txn) => {
        txn.deleteElement(physicalObjectId);
        txn.deleteModel(physicalModelId);
        txn.deleteElement(physicalModelId);
        const partitionProps: InformationPartitionElementProps = {
          classFullName: PhysicalPartition.classFullName,
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsPartitionElements(subjectId),
          code: PhysicalPartition.createCode(
            txn.iModel,
            subjectId,
            "PhysicalModel"
          ),
        };
        txn.insertElement(partitionProps);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Second changes",
      });

      // Act - run second transform with change processing
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        {
          argsForProcessChanges: {},
        }
      );
      await transformer.process();
      transformer.dispose();
      secondTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Second transformation",
      });

      // Assert - verify that new elements and models exist with correct values
      expect(
        IModelTransformerTestUtils.queryByCodeValue(targetDb, "PhysicalModel")
      ).to.not.be.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryModelIddByModeledElementCodeValue(
          targetDb,
          "PhysicalModel"
        )
      ).to.be.equal(Id64.invalid);
    });

    function generateSchema(
      schemaVersion: number,
      propertySuffix: string,
      propertyCount: number
    ): string {
      const schemaName = "DynamicTestSchema";
      const properties = Array.from(
        { length: propertyCount },
        (_, index) =>
          `                <ECProperty propertyName="${propertySuffix}${index + 1}" typeName="string"/>`
      ).join("\n");
      const sourceSchema = `<?xml version="1.0" encoding="UTF-8"?>
            <ECSchema schemaName="${schemaName}" alias="DTS" version="0${schemaVersion}.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
                <ECSchemaReference name="CoreCustomAttributes" version="01.00.03" alias="CoreCA"/>
                <ECSchemaReference name="BisCore" version="01.00.16" alias="bis"/>
                <ECCustomAttributes>
                    <DynamicSchema xmlns="CoreCustomAttributes.01.00.03"/>
                </ECCustomAttributes>
                <ECEntityClass typeName="DynamicPhysicalElement" modifier="Sealed">
                    <BaseClass>bis:PhysicalElement</BaseClass>
                    ${properties}
                </ECEntityClass>
            </ECSchema>`;
      return sourceSchema;
    }

    function createPhysicalElement(
      db: IModelDb,
      classFullName: string
    ): Id64String {
      return withEditTxn(db, "recreate elements & models", (txn) => {
        const sourcePhysicalModelId = PhysicalModel.insert(
          txn,
          IModelDb.rootSubjectId,
          "SourcePhysicalModel"
        );
        const sourceCategoryId = SpatialCategory.insert(
          txn,
          IModelDb.dictionaryId,
          "SourceCategory",
          {}
        );
        return txn.insertElement({
          classFullName,
          model: sourcePhysicalModelId,
          category: sourceCategoryId,
          code: PhysicalType.createCode(
            db,
            sourcePhysicalModelId,
            "TestClassElement"
          ),
          userLabel: "TestClassElement",
          SourceProperty1: "value1",
        } as GeometricElementProps);
      });
    }
  });

  async function prepareBriefcase(name: string) {
    const iModelId = await HubWrappers.createIModel(accessToken, iTwinId, name);

    const newBriefcase = await HubWrappers.downloadAndOpenBriefcase({
      accessToken: await IModelHost.getAccessToken(),
      iTwinId,
      iModelId,
      asOf: IModelVersion.latest().toJSON(),
    });
    await newBriefcase.locks.acquireLocks({
      shared: "0x10",
      exclusive: "0x1",
    });
    return newBriefcase;
  }

  async function closeAndDeleteBriefcase(iModel: BriefcaseDb) {
    await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, iModel);
    // eslint-disable-next-line @itwin/no-internal
    await IModelHost[_hubAccess].deleteIModel({
      iTwinId,
      iModelId: iModel.iModelId,
    });
  }
});
