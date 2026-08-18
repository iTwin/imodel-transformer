/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "vitest";

import {
  BriefcaseDb,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementRefersToElements,
  ExternalSourceAspect,
  IModelHost,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import * as TestUtils from "../TestUtils";
import {
  AccessToken,
  Guid,
  GuidString,
  Id64,
  Id64String,
} from "@itwin/core-bentley";
import {
  Code,
  IModel,
  IModelVersion,
  PhysicalElementProps,
} from "@itwin/core-common";

import { IModelTransformer } from "../../imodel-transformer";
import { ProvenanceManager } from "../../ProvenanceManager";
import {
  CountingIModelImporter,
  createStartedEditTxn,
  HubWrappers,
  IModelToTextFileExporter,
  IModelTransformerTestUtils,
  TestIModelTransformer,
  TransformerExtensiveTestScenario as TransformerExtensiveTestScenario,
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import {
  createPopulatedHubIModel,
  registerHubTestContext,
} from "../TestUtils/HubTestContext";

const { count } = IModelTestUtils;

describe("IModelTransformerHub - core", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  const outputDir = registerHubTestContext(
    "IModelTransformerHubCore",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
    }
  );

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
      await withTransformerLifecycle(
        transformer1,
        [targetEditTxn1],
        async () => {
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
          const jsonProps1 = JSON.parse(
            scopeEsaResult1?.jsonProperties ?? "{}"
          );
          assert.isEmpty(jsonProps1.reverseSyncVersion ?? "");
        }
      );
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
      await withTransformerLifecycle(
        transformer2,
        [targetEditTxn2],
        async () => {
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
          const jsonProps2 = JSON.parse(
            scopeEsaResult2?.jsonProperties ?? "{}"
          );
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
        }
      );
      await targetBriefcase.pushChanges({
        description: "target changes for transformation 2",
        retainLocks: true,
      });
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

  it("should handle sequential deletes after processAll with default processChanges options", async () => {
    const sourceIModelId = await createPopulatedHubIModel(
      outputDir,
      iTwinId,
      IModelTransformerTestUtils.generateUniqueName(
        "ProcessChangesDeletesSource"
      )
    );
    const targetIModelId = await createPopulatedHubIModel(
      outputDir,
      iTwinId,
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

      const sourceDbChangesetIndex = sourceDb.changeset.index;
      const processAllEditTxn = createStartedEditTxn(targetDb);
      const processAllTransformer = new IModelTransformer({
        source: sourceDb,
        target: processAllEditTxn,
      });
      await withTransformerLifecycle(
        processAllTransformer,
        [processAllEditTxn],
        async () => {
          await processAllTransformer.process();
          const syncVersionAfterProcessAll =
            await processAllTransformer[
              "_provenanceManager"
            ].getSynchronizationVersion();
          expect(syncVersionAfterProcessAll.index).to.equal(
            sourceDbChangesetIndex,
            "processAll should persist the source synchronization version"
          );
        }
      );
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
        await withTransformerLifecycle(transformer, [editTxn]);
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
      await transformerTestHub.deleteIModel({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      await transformerTestHub.deleteIModel({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
    }
  });

  it("Transform source iModel to target iModel", async () => {
    const sourceIModelId = await createPopulatedHubIModel(
      outputDir,
      iTwinId,
      "TransformerSource",
      async (sourceSeedDb) => {
        await TestUtils.ExtensiveTestScenario.prepareDb(sourceSeedDb);
      }
    );

    const targetIModelId = await createPopulatedHubIModel(
      outputDir,
      iTwinId,
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

      {
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
        await withTransformerLifecycle(
          transformer,
          [importEditTxn1],
          async () => {
            await transformer.process();
            // Verify processAll wrote the sync version so subsequent processChanges starts from correct index
            const syncVersionAfterProcessAll =
              await transformer[
                "_provenanceManager"
              ].getSynchronizationVersion();
            assert.equal(
              syncVersionAfterProcessAll.index,
              sourceDb.changeset.index,
              "processAll should write sync version matching source changeset index"
            );
          }
        );
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

      {
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
        await withTransformerLifecycle(transformer, [hubEditTxn], async () => {
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
          assert.isFalse(targetDb.txns.hasPendingTxns);
        });
        await targetDb.pushChanges({
          accessToken,
          description: "Should not actually push because there are no changes",
        });
      }

      {
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
        await withTransformerLifecycle(transformer, [importEditTxn2]);
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
});
