/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect, vi } from "vitest";
import * as path from "node:path";
import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSourceAspect,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
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
  PhysicalElementProps,
} from "@itwin/core-common";
import { IModelTransformer } from "../../imodel-transformer.js";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import {
  deleted,
  runTimeline,
  Timeline,
} from "../TestUtils/TimelineTestUtil.js";
import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

const { count } = IModelTestUtils;

/** How provenance is initialized, versioned, and matched, including for iModels forked by copying. */
describe("Provenance", () => {
  const hub = useTransformerHub("Provenance");
  const { outputDir } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
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
    vi.restoreAllMocks();
  });
});
