/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "vitest";
import * as path from "node:path";

import {
  BriefcaseDb,
  BriefcaseManager,
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  ExternalSourceAspect,
  IModelDb,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";

import {
  AccessToken,
  DbResult,
  GuidString,
  Id64,
  Id64String,
} from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
  IModel,
  IModelVersion,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import {
  ChangedInstanceIds,
  IModelTransformer,
  ProcessChangesOptions,
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
import {
  assertHubTestIModelState,
  getHubTestIModelState,
} from "../TestUtils/HubTestState";

const { count } = IModelTestUtils;

describe("IModelTransformerHub - merge", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let saveAndPushChanges: (db: BriefcaseDb, desc: string) => Promise<void>;

  const outputDir = registerHubTestContext(
    "IModelTransformerHubMerge",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
      saveAndPushChanges = context.saveAndPushChanges;
    }
  );

  it("should merge changes made on a branch back to master", async () => {
    const masterIModelName =
      IModelTransformerTestUtils.generateUniqueName("MergeMaster");
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    const masterSeedState = {
      1: 1,
      2: 1,
      20: 1,
      21: 1,
      40: 1,
      41: 2,
      42: 3,
    };
    const { modelId, categoryId } = withEditTxn(
      masterSeedDb,
      "populate master seed",
      (txn) => ({
        categoryId: SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "SpatialCategory",
          new SubCategoryAppearance()
        ),
        modelId: PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "PhysicalModel"
        ),
      })
    );
    withEditTxn(masterSeedDb, "maintain master seed objects", (txn) => {
      for (const [name, updateState] of Object.entries(masterSeedState)) {
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
          jsonProperties: { updateState },
        } as PhysicalElementProps);
      }
    });

    // 20 will be deleted, so it's important to know remapping deleted elements still works if there is no fedguid
    const noFedGuidElemIds = masterSeedDb.queryEntityIds({
      from: "Bis.Element",
      where: "UserLabel IN ('1','20','41','42')",
    });
    withEditTxn(masterSeedDb, "null out selected federation guids", () => {
      for (const elemId of noFedGuidElemIds)
        masterSeedDb.withSqliteStatement(
          `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
          (s) => {
            expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
          }
        );
    });
    masterSeedDb.performCheckpoint();

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

    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: masterIModelName,
      noLocks: true,
      version0: masterSeedFileName,
    });
    let branch1IModelId: GuidString | undefined;
    let branch2IModelId: GuidString | undefined;
    let masterDb: BriefcaseDb | undefined;
    let branch1Db: BriefcaseDb | undefined;
    let branch2Db: BriefcaseDb | undefined;

    const insertPhysicalObject = (
      txn: EditTxn,
      name: string,
      updateState: number
    ) =>
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
        jsonProperties: { updateState },
      } as PhysicalElementProps);
    const updatePhysicalObject = (
      db: IModelDb,
      txn: EditTxn,
      name: string,
      updateState: number
    ) => {
      const element = db.elements.getElement(
        IModelTestUtils.queryByUserLabel(db, name)
      );
      txn.updateElement({
        ...element.toJSON(),
        jsonProperties: { ...element.jsonProperties, updateState },
      });
    };

    try {
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      await saveAndPushChanges(masterDb, "seeded master");
      masterDb.performCheckpoint();

      const createBranch = async (
        iModelName: string
      ): Promise<{ id: GuidString; db: BriefcaseDb }> => {
        const id = await HubWrappers.recreateIModel({
          accessToken,
          iTwinId,
          iModelName,
          noLocks: true,
          version0: masterDb!.pathName,
        });
        const db = await HubWrappers.downloadAndOpenBriefcase({
          accessToken,
          iTwinId,
          iModelId: id,
        });
        return { id, db };
      };
      const branch1 = await createBranch(
        IModelTransformerTestUtils.generateUniqueName("MergeBranch1")
      );
      branch1IModelId = branch1.id;
      branch1Db = branch1.db;
      const branch1InitEditTxn = createStartedEditTxn(branch1Db);
      const branch1Initializer = new IModelTransformer(
        { source: masterDb, target: branch1InitEditTxn },
        { wasSourceIModelCopiedToTarget: true }
      );
      await withTransformerLifecycle(branch1Initializer, [branch1InitEditTxn]);
      await branch1Db.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(masterDb, "update master object 40", (txn) => {
        updatePhysicalObject(masterDb!, txn, "40", 5);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "update master object 40",
      });
      masterDb.performCheckpoint();

      const branch2 = await createBranch(
        IModelTransformerTestUtils.generateUniqueName("MergeBranch2")
      );
      branch2IModelId = branch2.id;
      branch2Db = branch2.db;
      const branch2InitEditTxn = createStartedEditTxn(branch2Db);
      const branch2Initializer = new IModelTransformer(
        { source: masterDb, target: branch2InitEditTxn },
        { wasSourceIModelCopiedToTarget: true }
      );
      await withTransformerLifecycle(branch2Initializer, [branch2InitEditTxn]);
      await branch2Db.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(branch1Db, "update branch1 objects", (txn) => {
        updatePhysicalObject(branch1Db!, txn, "2", 2);
        insertPhysicalObject(txn, "3", 1);
        insertPhysicalObject(txn, "4", 1);
      });
      await branch1Db.pushChanges({
        accessToken,
        description: "update branch1 objects",
      });
      withEditTxn(branch1Db, "insert expected relationships", (txn) => {
        expectedRelationships.forEach(({ sourceLabel, targetLabel }, i) => {
          const sourceId = IModelTestUtils.queryByUserLabel(
            branch1Db!,
            sourceLabel
          );
          const targetId = IModelTestUtils.queryByUserLabel(
            branch1Db!,
            targetLabel
          );
          assert(sourceId && targetId);
          const rel = ElementGroupsMembers.create(
            branch1Db!,
            sourceId,
            targetId,
            0
          );
          expectedRelationships[i].idInBranch1 = txn.insertRelationship(
            rel.toJSON()
          );
        });
      });
      await branch1Db.pushChanges({
        accessToken,
        description: "insert expected relationships",
      });
      withEditTxn(branch1Db, "update expected relationship", (txn) => {
        const rel = branch1Db!.relationships.getInstance<ElementGroupsMembers>(
          ElementGroupsMembers.classFullName,
          expectedRelationships[0].idInBranch1
        );
        rel.memberPriority = 1;
        txn.updateRelationship(rel.toJSON());
      });
      await branch1Db.pushChanges({
        accessToken,
        description: "update expected relationship",
      });
      withEditTxn(branch1Db, "update and delete branch1 objects", (txn) => {
        updatePhysicalObject(branch1Db!, txn, "1", 2);
        txn.deleteElement(IModelTestUtils.queryByUserLabel(branch1Db!, "3"));
        insertPhysicalObject(txn, "5", 1);
        insertPhysicalObject(txn, "6", 1);
        txn.deleteElement(IModelTestUtils.queryByUserLabel(branch1Db!, "20"));
        updatePhysicalObject(branch1Db!, txn, "21", 2);
      });
      await branch1Db.pushChanges({
        accessToken,
        description: "update and delete branch1 objects",
      });
      withEditTxn(
        branch1Db,
        "delete branch1 object 21 and insert 30",
        (txn) => {
          txn.deleteElement(IModelTestUtils.queryByUserLabel(branch1Db!, "21"));
          insertPhysicalObject(txn, "30", 1);
        }
      );
      await branch1Db.pushChanges({
        accessToken,
        description: "delete branch1 object 21 and insert 30",
      });

      {
        const reverseTargetEditTxn = createStartedEditTxn(masterDb);
        const reverseSourceEditTxn = createStartedEditTxn(branch1Db);
        const reverseSyncer = new IModelTransformer(
          { source: branch1Db, target: reverseTargetEditTxn },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
            sourceEditTxn: reverseSourceEditTxn,
          }
        );
        await withTransformerLifecycle(reverseSyncer, [
          reverseTargetEditTxn,
          reverseSourceEditTxn,
        ]);
        await branch1Db.pushChanges({
          accessToken,
          description: "reverse sync branch1",
        });
        await masterDb.pushChanges({
          accessToken,
          description: "reverse sync branch1",
        });
      }
      const branch1State = await getHubTestIModelState(branch1Db);
      expect(await getHubTestIModelState(masterDb)).to.deep.equal({
        ...branch1State,
        40: 5,
      });

      {
        const forwardTargetEditTxn = createStartedEditTxn(branch2Db);
        const forwardSyncer = new IModelTransformer(
          { source: masterDb, target: forwardTargetEditTxn },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
          }
        );
        await withTransformerLifecycle(forwardSyncer, [forwardTargetEditTxn]);
        await branch2Db.pushChanges({
          accessToken,
          description: "forward sync branch2",
        });
      }
      expect(await getHubTestIModelState(branch2Db)).to.deep.equal(
        await getHubTestIModelState(masterDb)
      );

      withEditTxn(branch2Db, "insert branch2 objects", (txn) => {
        insertPhysicalObject(txn, "7", 1);
        insertPhysicalObject(txn, "8", 1);
      });
      await branch2Db.pushChanges({
        accessToken,
        description: "insert branch2 objects",
      });
      withEditTxn(masterDb, "update master conflict objects", (txn) => {
        const master7Id = IModelTestUtils.queryByUserLabel(masterDb!, "7");
        if (Id64.isValid(master7Id))
          updatePhysicalObject(masterDb!, txn, "7", 2);
        else insertPhysicalObject(txn, "7", 2);
        insertPhysicalObject(txn, "9", 1);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "update master conflict objects",
      });
      {
        const reverseTargetEditTxn = createStartedEditTxn(masterDb);
        const reverseSourceEditTxn = createStartedEditTxn(branch2Db);
        const reverseSyncer = new IModelTransformer(
          { source: branch2Db, target: reverseTargetEditTxn },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
            sourceEditTxn: reverseSourceEditTxn,
          }
        );
        await withTransformerLifecycle(reverseSyncer, [
          reverseTargetEditTxn,
          reverseSourceEditTxn,
        ]);
        await branch2Db.pushChanges({
          accessToken,
          description: "reverse sync branch2",
        });
        await masterDb.pushChanges({
          accessToken,
          description: "reverse sync branch2",
        });
      }

      for (const db of [masterDb, branch1Db, branch2Db]) {
        const elem1Id = IModelTestUtils.queryByUserLabel(db, "1");
        expect(db.elements.getElement(elem1Id).federationGuid).to.be.undefined;
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
      expect(count(masterDb, ExternalSourceAspect.classFullName)).to.equal(0);
      for (const db of [branch1Db, branch2Db]) {
        const elem1Id = IModelTestUtils.queryByUserLabel(db, "1");
        expect(db.elements.getElement(elem1Id).federationGuid).to.be.undefined;
        const aspects = [
          ...db.queryEntityIds({ from: "BisCore.ExternalSourceAspect" }),
        ].map((aspectId) =>
          db.elements.getAspect(aspectId).toJSON()
        ) as ExternalSourceAspectProps[];
        expect(aspects).to.deep.subsetEqual([
          {
            element: { id: IModelDb.rootSubjectId },
            identifier: masterDb.iModelId,
          },
          { element: { id: elem1Id }, identifier: elem1Id },
        ]);
        expect(Date.parse(aspects[3].version!)).not.to.be.NaN;
      }
      await assertHubTestIModelState(masterDb, { 7: 1 }, { subset: true });

      withEditTxn(masterDb, "update master object 6", (txn) => {
        updatePhysicalObject(masterDb!, txn, "6", 2);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "update master object 6",
      });
      withEditTxn(masterDb, "delete expected relationships", (txn) => {
        expectedRelationships.forEach(({ sourceLabel, targetLabel }) => {
          const sourceId = IModelTestUtils.queryByUserLabel(
            masterDb!,
            sourceLabel
          );
          const targetId = IModelTestUtils.queryByUserLabel(
            masterDb!,
            targetLabel
          );
          assert(sourceId && targetId);
          const rel = masterDb!.relationships.getInstance(
            ElementGroupsMembers.classFullName,
            { sourceId, targetId }
          );
          txn.deleteRelationship(rel.toJSON());
        });
      });
      await masterDb.pushChanges({
        accessToken,
        description: "delete expected relationships",
      });
      {
        const forwardTargetEditTxn = createStartedEditTxn(branch1Db);
        const forwardSyncer = new IModelTransformer(
          { source: masterDb, target: forwardTargetEditTxn },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
          }
        );
        await withTransformerLifecycle(forwardSyncer, [forwardTargetEditTxn]);
        await branch1Db.pushChanges({
          accessToken,
          description: "forward sync branch1",
        });
      }
      for (const rel of expectedRelationships) {
        expect(
          branch1Db.relationships.tryGetInstance(
            ElementGroupsMembers.classFullName,
            rel.idInBranch1
          ),
          `had ${rel.sourceLabel}->${rel.targetLabel}`
        ).to.be.undefined;
        const sourceId = IModelTestUtils.queryByUserLabel(
          branch1Db,
          rel.sourceLabel
        );
        const targetId = IModelTestUtils.queryByUserLabel(
          branch1Db,
          rel.targetLabel
        );
        assert(sourceId && targetId);
        expect(
          branch1Db.relationships.tryGetInstance(
            ElementGroupsMembers.classFullName,
            { sourceId, targetId }
          ),
          `had ${rel.sourceLabel}->${rel.targetLabel}`
        ).to.be.undefined;
        const srcElemAspects = branch1Db.elements.getAspects(
          sourceId,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[];
        expect(!srcElemAspects.some((a) => a.identifier === rel.idInBranch1)).to
          .be.true;
        expect(srcElemAspects.length).to.lessThanOrEqual(1);
      }
      await assertHubTestIModelState(branch1Db, { 7: 1 }, { subset: true });

      withEditTxn(branch1Db, "update branch1 conflict object", (txn) => {
        updatePhysicalObject(branch1Db!, txn, "7", 10);
      });
      await branch1Db.pushChanges({
        accessToken,
        description: "update branch1 conflict object",
      });
      {
        const reverseTargetEditTxn = createStartedEditTxn(masterDb);
        const reverseSourceEditTxn = createStartedEditTxn(branch1Db);
        const reverseSyncer = new IModelTransformer(
          { source: branch1Db, target: reverseTargetEditTxn },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
            sourceEditTxn: reverseSourceEditTxn,
          }
        );
        await withTransformerLifecycle(reverseSyncer, [
          reverseTargetEditTxn,
          reverseSourceEditTxn,
        ]);
        await branch1Db.pushChanges({
          accessToken,
          description: "reverse sync final branch1 change",
        });
        await masterDb.pushChanges({
          accessToken,
          description: "reverse sync final branch1 change",
        });
      }
      {
        const forwardTargetEditTxn = createStartedEditTxn(branch2Db);
        const forwardSyncer = new IModelTransformer(
          { source: masterDb, target: forwardTargetEditTxn },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
          }
        );
        await withTransformerLifecycle(forwardSyncer, [forwardTargetEditTxn]);
        await branch2Db.pushChanges({
          accessToken,
          description: "forward sync final branch2 change",
        });
      }
      for (const db of [masterDb, branch1Db, branch2Db])
        await assertHubTestIModelState(db, { 7: 10 }, { subset: true });

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
      let sourceDb: BriefcaseDb | undefined;

      try {
        const master = {
          id: masterIModelId,
          db: masterDb,
          state: await getHubTestIModelState(masterDb),
        };

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
        sourceDb = await HubWrappers.downloadAndOpenBriefcase({
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
            { source: sourceDb!, target: editTxn },
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
        await withTransformerLifecycle(replayInitTransformer.transformer, [
          replayInitTransformer.editTxn,
        ]); // process any elements that were part of the "seed"

        await saveAndPushChanges(replayedDb, "changes from source seed");
        for (const masterDbChangeset of masterDbChangesets) {
          await sourceDb.pullChanges({
            accessToken,
            toIndex: masterDbChangeset.index,
          });
          const replayTransformer = makeReplayTransformer({
            startChangeset: sourceDb.changeset,
          });
          await withTransformerLifecycle(replayTransformer.transformer, [
            replayTransformer.editTxn,
          ]);
          await saveAndPushChanges(
            replayedDb,
            masterDbChangeset.description ?? ""
          );
        }
        sourceDb?.close();
        sourceDb = undefined;
        await assertHubTestIModelState(replayedDb, master.state); // should have same ending state as masterDb

        // make sure there are no deletes in the replay history (all elements that were eventually deleted from masterDb were excluded)
        const replayedDbChangesets =
          await transformerTestHub.downloadChangesets({
            accessToken,
            iModelId: replayedIModelId,
            targetDir: BriefcaseManager.getChangeSetsPath(replayedIModelId),
          });
        assert.isAtLeast(
          replayedDbChangesets.length,
          masterDbChangesets.length
        ); // replayedDb will have more changesets when seed contains elements
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
        sourceDb?.close();
        replayedDb.close();
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: replayedIModelId,
        });
      }
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branch1Db)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branch1Db);
      if (branch2Db)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branch2Db);
      masterSeedDb.close();
      if (branch1IModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branch1IModelId,
        });
      if (branch2IModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branch2IModelId,
        });
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
    }
  });
});
