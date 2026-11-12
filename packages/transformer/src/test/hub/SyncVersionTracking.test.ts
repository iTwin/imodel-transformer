/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect, vi } from "vitest";
import * as path from "node:path";
import {
  BriefcaseDb,
  BriefcaseManager,
  ExternalSourceAspect,
  IModelDb,
  IModelHost,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import * as TestUtils from "../TestUtils/index.js";
import {
  AccessToken,
  Guid,
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
} from "@itwin/core-common";
import {
  IModelTransformer,
  IModelTransformerError,
} from "../../imodel-transformer.js";
import { ProvenanceManager } from "../../ProvenanceManager.js";
import {
  assertTransformerError,
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import {
  assertElemState,
  runTimeline,
  Timeline,
} from "../TestUtils/TimelineTestUtil.js";
import {
  recordChangesetDownloads,
  useTransformerHub,
} from "../TestUtils/TransformerHubFixture.js";

const { count } = IModelTestUtils;

/** Which source changesets a synchronization processes, and the sync versions and pending changeset indices it records. */
describe("Sync version tracking", () => {
  const hub = useTransformerHub("SyncVersionTracking");
  const { outputDir, createPopulatedIModelHubIModel } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
  });

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

  it("should not include 'initialized branch provenance' changeset in a reverse sync", async () => {
    let downloads: ReturnType<typeof recordChangesetDownloads> | undefined;
    const validateCsFileProps = () => {
      downloads!.restore();
      expect(downloads!.files).not.toHaveLength(0);
      expect(
        downloads!.files.some((csFileProp) =>
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
            {
              initTransformer: () => {
                downloads = recordChangesetDownloads();
              },
              assert: { afterProcessChanges: validateCsFileProps },
            },
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
    const queryChangeset = vi.spyOn(BriefcaseManager, "queryChangeset");
    await syncer.process();
    expect(queryChangeset.mock.calls).to.have.length.greaterThan(0);
    for (const [args] of queryChangeset.mock.calls) {
      expect(args).to.deep.equal({
        iModelId: branch.id,
        changeset: {
          id: branchAt2Changeset.id,
        },
      });
    }

    syncer.dispose();
    syncEditTxn.end();
    reverseSyncSourceEditTxn.end();
    await tearDown();
    vi.restoreAllMocks();
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
          });
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
          });
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
              });
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
    let synchronizationVersionErrorAsserted = false;
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
          });
          targetScopeProvenanceProps = targetScopeProvenance;
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
          sync: [
            "master",
            {
              expectThrow: true,
              assert: {
                onError(error) {
                  assertTransformerError(
                    error,
                    IModelTransformerError.SynchronizationVersionMissing,
                    "Could not find synchronization version in scope aspect. This may be due to the last successful run of the transformer being done with an older version.\n         Consider running the transformer with branchRelationshipDataBehavior set to 'unsafe-migrate'"
                  );
                  synchronizationVersionErrorAsserted = true;
                },
              },
            },
          ],
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
            count(master.db, "bis.Element")
          );
          expect(count(branch.db, "bis.Element")).to.be.equal(
            count(master.db, "bis.Element")
          );

          // The root Subject owns scope provenance only because root elements are not transformed.
          (await externalAspectCounts(branch.db)).forEach((value) => {
            expect(value.aspectCount).to.equal(1);
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

    expect(synchronizationVersionErrorAsserted).to.be.true;
    await tearDown();
  });

  it("should skip provenance changesets made to branch during reverse sync", async () => {
    const timeline: Timeline = [
      { master: { 1: 1 } },
      { master: { 2: 2 } },
      { master: { 3: 1 } },
      { branch: { branch: "master" } },
      { branch: { 1: 2, 4: 1 } },
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
          });
        },
      },
      { master: { sync: ["branch"] } },
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
});
