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
  deleteElementTree,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  ElementOwnsChildElements,
  ExternalSourceAspect,
  GenericSchema,
  IModelDb,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SnapshotDb,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";

import {
  AccessToken,
  DbResult,
  Guid,
  GuidString,
  Id64,
  Id64String,
} from "@itwin/core-bentley";
import {
  Code,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import {
  ChangedInstanceIds,
  IModelTransformer,
} from "../../imodel-transformer";

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

describe("IModelTransformerHub - branch synchronization", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let saveAndPushChanges: (db: BriefcaseDb, desc: string) => Promise<void>;

  const outputDir = registerHubTestContext(
    "IModelTransformerHubBranch",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
      saveAndPushChanges = context.saveAndPushChanges;
    }
  );

  it("should properly delete element in master when element in branch is deleted alongside all of its ESAs.", async () => {
    // This test exercises elemIdToScopeESAs map in IModelTransformer.
    const masterIModelName = "MasterMultipleESAsDifferentKinds";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    const { modelId, categoryId } = withEditTxn(
      masterSeedDb,
      "populate master seed",
      (txn) => ({
        modelId: PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "PhysicalModel"
        ),
        categoryId: SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "SpatialCategory",
          new SubCategoryAppearance()
        ),
      })
    );
    withEditTxn(masterSeedDb, "maintain master objects", (txn) => {
      for (const name of ["1", "2"]) {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: name,
          }),
          userLabel: name,
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(0, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
          jsonProperties: { updateState: 1 },
        } as PhysicalElementProps);
      }
    });
    masterSeedDb.performCheckpoint();
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

    let masterIModelId: GuidString | undefined;
    let branchIModelId: GuidString | undefined;
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let aspectIdForRelationship: Id64String | undefined;

    try {
      masterIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: masterIModelName,
        noLocks: true,
        version0: masterSeedFileName,
      });
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      await saveAndPushChanges(masterDb, "seeded master");
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: "BranchMultipleESAsDifferentKinds",
        noLocks: true,
        version0: masterDb.pathName,
      });
      branchDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: branchIModelId,
      });
      const branchInitEditTxn = createStartedEditTxn(branchDb);
      const provenanceInitializer = new IModelTransformer(
        { source: masterDb, target: branchInitEditTxn },
        {
          forceExternalSourceAspectProvenance: true,
          wasSourceIModelCopiedToTarget: true,
        }
      );
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(
        masterDb,
        "insert master relationship provenance data",
        (txn) => {
          for (const name of ["3", "4", "5"]) {
            const elementProps: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category: categoryId,
              code: new Code({
                spec: IModelDb.rootSubjectId,
                scope: IModelDb.rootSubjectId,
                value: name,
              }),
              userLabel: name,
              geom: IModelTransformerTestUtils.createBox(
                Point3d.create(1, 1, 1)
              ),
              placement: {
                origin: Point3d.create(0, 0, 0),
                angles: YawPitchRollAngles.createDegrees(0, 0, 0),
              },
              jsonProperties: { updateState: Number(name) },
            };
            txn.insertElement(elementProps);
          }
        }
      );
      await masterDb.pushChanges({
        accessToken,
        description: "insert master relationship provenance data",
      });
      withEditTxn(
        masterDb,
        "insert relationship provenance test data",
        (txn) => {
          const sourceId = IModelTestUtils.queryByUserLabel(masterDb!, "3");
          const targetIds = ["2", "1", "4", "5"].map((name) =>
            IModelTestUtils.queryByUserLabel(masterDb!, name)
          );
          for (const targetId of targetIds)
            ElementGroupsMembers.insert(txn, sourceId, targetId);
        }
      );
      await masterDb.pushChanges({
        accessToken,
        description: "insert relationship provenance test data",
      });

      const forwardSyncEditTxn = createStartedEditTxn(branchDb);
      const forwardSyncer = new IModelTransformer(
        { source: masterDb, target: forwardSyncEditTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      await withTransformerLifecycle(forwardSyncer, [forwardSyncEditTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "forward sync relationship provenance",
      });

      const elemId = IModelTestUtils.queryByUserLabel(branchDb, "3");
      const aspects = branchDb.elements.getAspects(
        elemId,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      expect(aspects.length).to.be.equal(5); // 4 relationships + 1 element.
      let foundElementEsa = false;
      aspects.forEach((a) => {
        if (a.kind === ExternalSourceAspect.Kind.Element)
          foundElementEsa = true;
        else if (a.kind === ExternalSourceAspect.Kind.Relationship)
          aspectIdForRelationship = a.id;
      });
      expect(aspectIdForRelationship).to.not.be.undefined;
      expect(foundElementEsa).to.be.true;

      withEditTxn(
        branchDb,
        "delete relationship provenance test data",
        (txn) => {
          const branchElemId = IModelTestUtils.queryByUserLabel(branchDb!, "3");
          const branchAspects = branchDb!.elements.getAspects(
            branchElemId
          ) as ExternalSourceAspect[];
          branchAspects.forEach((a) => txn.deleteAspect(a.id));
          txn.deleteElement(branchElemId);
        }
      );
      await branchDb.pushChanges({
        accessToken,
        description: "delete relationship provenance test data",
      });

      const reverseSyncEditTxn = createStartedEditTxn(masterDb);
      const reverseSyncSourceEditTxn = createStartedEditTxn(branchDb);
      const reverseSyncer = new IModelTransformer(
        { source: branchDb, target: reverseSyncEditTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: reverseSyncSourceEditTxn,
        }
      );
      await withTransformerLifecycle(reverseSyncer, [
        reverseSyncEditTxn,
        reverseSyncSourceEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "reverse sync deleted relationship provenance data",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "reverse sync deleted relationship provenance data",
      });

      expect(IModelTestUtils.queryByUserLabel(branchDb, "3")).to.be.equal(
        Id64.invalid
      );
      expect(IModelTestUtils.queryByUserLabel(masterDb, "3")).to.be.equal(
        Id64.invalid
      );
      expect(() =>
        branchDb!.elements.getAspect(aspectIdForRelationship!)
      ).to.throw("not found");
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branchDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
      if (masterIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: masterIModelId,
        });
      if (branchIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
      masterSeedDb.close();
    }
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
      sourceIModelId = await transformerTestHub.createNewIModel({
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
      targetIModelId = await transformerTestHub.createNewIModel({
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
      await withTransformerLifecycle(transformer, [forkInitEditTxn]);
      await targetDb.pushChanges({ description: "fork init" });

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

      await withTransformerLifecycle(transformer, [
        reverseSyncEditTxn,
        reverseSyncSourceEditTxn,
      ]);

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
          await transformerTestHub.deleteIModel({
            iTwinId,
            iModelId: sourceIModelId,
          });
        if (targetIModelId)
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
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
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
      const branchDbChangesets = await transformerTestHub.downloadChangesets({
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
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
      if (branchIModelId) {
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
      }
    }
  });
});
