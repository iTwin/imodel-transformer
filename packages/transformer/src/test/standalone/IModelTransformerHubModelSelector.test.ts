/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "vitest";
import * as path from "node:path";

import {
  BisCoreSchema,
  BriefcaseDb,
  BriefcaseManager,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSourceAspect,
  GenericSchema,
  IModelDb,
  IModelJsFs,
  ModelSelector,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, Guid, GuidString, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ElementProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";

import {
  ChangedInstanceIds,
  IModelImporter,
  IModelTransformer,
} from "../../imodel-transformer";
import { ProvenanceManager } from "../../ProvenanceManager";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { registerHubTestContext } from "../TestUtils/HubTestContext";

const { count } = IModelTestUtils;

describe("IModelTransformerHub - model selector", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  const outputDir = registerHubTestContext(
    "IModelTransformerHubModelSelector",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
    }
  );

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
      await withTransformerLifecycle(provenanceInitializer, [
        provenanceInitEditTxn,
      ]);

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
      const sourceDbChangesets = await transformerTestHub.downloadChangesets({
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
      await withTransformerLifecycle(
        synchronizer,
        [injectedEditTxn],
        async () => {
          await synchronizer.process();
          expect(didExportModelSelector).to.be.true;
          expect(didImportModelSelector).to.be.true;
        }
      );
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
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await transformerTestHub.deleteIModel({
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
      await withTransformerLifecycle(transformer, [initialTargetEditTxn]);

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
      await withTransformerLifecycle(
        transformer,
        [changeTargetEditTxn],
        async () => {
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
              const sourceElement =
                sourceDb.elements.getElement(sourceElementId);
              expect(sourceElement.code.value).to.equal(codeValue);
            }
          );
        }
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

    const sourceIModelId = await transformerTestHub.createNewIModel({
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
    const targetIModelId = await transformerTestHub.createNewIModel({
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
    const { catIdInTarget, modelIdInTarget } = await withTransformerLifecycle(
      transformer,
      [forkInitEditTxn],
      async () => {
        await transformer.process();
        return {
          catIdInTarget: transformer.context.findTargetElementId(categoryId1),
          modelIdInTarget: transformer.context.findTargetElementId(modelId1),
        };
      }
    );
    await targetDb.pushChanges({ description: "fork init" });

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
    try {
      await transformer.process();
    } finally {
      transformer.dispose();
      reverseSyncEditTxn.end("save", "reverse sync");
      reverseSyncSourceEditTxn.end("save", "reverse sync provenance");
    }
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
    await withTransformerLifecycle(transformer, [forwardSyncEditTxn]);
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
  });
});
