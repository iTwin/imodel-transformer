/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "vitest";
import * as path from "node:path";
import * as semver from "semver";
import {
  BisCoreSchema,
  BriefcaseDb,
  BriefcaseManager,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  ElementRefersToElements,
  ExternalSourceAspect,
  GenericSchema,
  IModelDb,
  IModelJsFs,
  SnapshotDb,
  withEditTxn,
} from "@itwin/core-backend";
import * as TestUtils from "../TestUtils/index.js";
import {
  AccessToken,
  DbResult,
  Guid,
  GuidString,
  Id64String,
} from "@itwin/core-bentley";
import {
  ColorDef,
  ExternalSourceAspectProps,
  IModel,
  IModelVersion,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import {
  ChangedInstanceIds,
  IModelExporter,
  IModelTransformer,
  ProcessChangesOptions,
} from "../../imodel-transformer.js";
import {
  CountingIModelImporter,
  createStartedEditTxn,
  HubWrappers,
  IModelToTextFileExporter,
  IModelTransformerTestUtils,
  TestIModelTransformer,
  TransformerExtensiveTestScenario as TransformerExtensiveTestScenario,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import {
  assertElemState,
  deleted,
  populateTimelineSeed,
  runTimeline,
  Timeline,
  TimelineIModelState,
} from "../TestUtils/TimelineTestUtil.js";
import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

const { count } = IModelTestUtils;

/** Broad scenarios that exercise many transformation behaviors at once. */
describe("End-to-end scenarios", () => {
  const hub = useTransformerHub("EndToEndScenarios");
  const { outputDir, createPopulatedIModelHubIModel, saveAndPushChanges } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
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
        // ElementAspect rebuilds may reinsert replaceable aspects after cleanup.
        assert.isAtLeast(targetDbChanges.aspect.insertIds.size, 1);
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

      const sourceIModelChangeSets = await transformerTestHub.queryChangesets({
        accessToken,
        iModelId: sourceIModelId,
      });
      const targetIModelChangeSets = await transformerTestHub.queryChangesets({
        accessToken,
        iModelId: targetIModelId,
      });
      assert.equal(sourceIModelChangeSets.length, 2);
      assert.equal(targetIModelChangeSets.length, 2);

      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, sourceDb);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, targetDb);
    } finally {
      try {
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
    const replayedIModelId = await transformerTestHub.createNewIModel({
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

      const masterDbChangesets = await transformerTestHub.downloadChangesets({
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
      const replayedDbChangesets = await transformerTestHub.downloadChangesets({
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
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: replayedIModelId,
      });
    }
  });
});
