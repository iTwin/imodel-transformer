/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, assert, beforeEach, expect, vi } from "vitest";
import {
  BriefcaseDb,
  DisplayStyle3d,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  SubjectOwnsSubjects,
  withEditTxn,
} from "@itwin/core-backend";
import { AccessToken, Guid, GuidString, Id64String } from "@itwin/core-bentley";
import {
  Code,
  IModel,
  PhysicalElementProps,
  Placement3d,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import { IModelTransformer } from "../../imodel-transformer.js";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import { runTimeline, Timeline } from "../TestUtils/TimelineTestUtil.js";
import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

const { count } = IModelTestUtils;

/** Elements deleted and recreated, and codes reused or swapped, across changesets. */
describe("Element recreation and codes", () => {
  const hub = useTransformerHub("ElementRecreationAndCodes");
  const { prepareBriefcase, closeAndDeleteBriefcase } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
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
    vi.restoreAllMocks();
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

    it("preserves a remapped guidless target when its source is recreated across changesets", async () => {
      const sourceSubjectId = withEditTxn(
        sourceDb,
        "insert original guidless subject",
        (txn) => {
          const subject = Subject.create(
            sourceDb,
            IModel.rootSubjectId,
            "Guidless recreation"
          );
          subject.federationGuid = Guid.empty;
          return subject.insert(txn);
        }
      );
      await sourceDb.pushChanges({
        description: "Insert original guidless subject",
        retainLocks: true,
      });

      const initialEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialEditTxn,
      });
      await transformer.process();
      const targetSubjectId =
        transformer.context.findTargetElementId(sourceSubjectId);
      transformer.dispose();
      initialEditTxn.end();
      await targetDb.pushChanges({
        description: "Initial guidless subject transformation",
        retainLocks: true,
      });

      withEditTxn(sourceDb, "delete original guidless subject", (txn) => {
        txn.deleteElement(sourceSubjectId);
      });
      await sourceDb.pushChanges({
        description: "Delete original guidless subject",
        retainLocks: true,
      });
      const startChangeset = sourceDb.changeset;

      const recreatedSourceSubjectId = withEditTxn(
        sourceDb,
        "recreate guidless subject",
        (txn) => {
          const subject = Subject.create(
            sourceDb,
            IModel.rootSubjectId,
            "Guidless recreation"
          );
          subject.federationGuid = Guid.empty;
          subject.userLabel = "Recreated guidless subject";
          return subject.insert(txn);
        }
      );
      await sourceDb.pushChanges({
        description: "Recreate guidless subject",
        retainLocks: true,
      });

      class GuidlessRecreationTransformer extends IModelTransformer {
        public override async addCustomChanges(): Promise<void> {
          expect(this.context.findTargetElementId(sourceSubjectId)).to.equal(
            targetSubjectId
          );
          this.context.remapElement(recreatedSourceSubjectId, targetSubjectId);
        }
      }

      const changesEditTxn = createStartedEditTxn(targetDb);
      transformer = new GuidlessRecreationTransformer(
        { source: sourceDb, target: changesEditTxn },
        { argsForProcessChanges: { startChangeset } }
      );
      await transformer.process();
      transformer.dispose();
      changesEditTxn.end();

      expect(
        targetDb.elements.getElement<Subject>(targetSubjectId).userLabel
      ).to.equal("Recreated guidless subject");
      expect(
        count(
          targetDb,
          Subject.classFullName,
          `Parent.Id = ${IModel.rootSubjectId}`
        )
      ).to.equal(1);
    });
  });
});
