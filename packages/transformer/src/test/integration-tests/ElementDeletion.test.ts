/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, assert, beforeEach, expect, vi } from "vitest";
import * as path from "node:path";
import {
  BisCoreSchema,
  BriefcaseDb,
  BriefcaseManager,
  CategorySelector,
  ChangesetReader,
  DefinitionContainer,
  DefinitionModel,
  DefinitionPartition,
  deleteElementTree,
  DisplayStyle3d,
  EditTxn,
  ElementGroupsMembers,
  ElementOwnsChildElements,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  GenericSchema,
  GeometryPart,
  IModelDb,
  IModelJsFs,
  ModelSelector,
  PhysicalModel,
  PhysicalObject,
  PhysicalType,
  PropertyFilter,
  SnapshotDb,
  SpatialCategory,
  SpatialViewDefinition,
  Subject,
  SubjectOwnsPartitionElements,
  withEditTxn,
} from "@itwin/core-backend";
import {
  AccessToken,
  Guid,
  GuidString,
  Id64,
  Id64String,
  Logger,
} from "@itwin/core-bentley";
import {
  Code,
  DefinitionElementProps,
  ExternalSourceAspectProps,
  GeometricElementProps,
  GeometryPartProps,
  IModel,
  InformationPartitionElementProps,
  ModelProps,
  PhysicalElementProps,
  SpatialViewDefinitionProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import {
  ChangedInstanceIds,
  IModelImporter,
  IModelTransformer,
} from "../../imodel-transformer.js";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import {
  populateTimelineSeed,
  runTimeline,
  Timeline,
  TimelineIModelState,
} from "../TestUtils/TimelineTestUtil.js";
import {
  recordChangesetDownloads,
  useTransformerHub,
} from "../TestUtils/TransformerHubFixture.js";

/** How source element and model deletions reach the target, including elements that target-only data still uses. */
describe("Element deletion", () => {
  const hub = useTransformerHub("ElementDeletion");
  const {
    outputDir,
    createPopulatedIModelHubIModel,
    prepareBriefcase,
    closeAndDeleteBriefcase,
  } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
  });

  it("keeps a category that target-only elements still use when the source deletes it, and does not retry the deletion", async () => {
    const sourceIModelId = await createPopulatedIModelHubIModel(
      IModelTransformerTestUtils.generateUniqueName("KeptCategorySource")
    );
    const targetIModelId = await createPopulatedIModelHubIModel(
      IModelTransformerTestUtils.generateUniqueName("KeptCategoryTarget")
    );
    let sourceDb: BriefcaseDb | undefined;
    let targetDb: BriefcaseDb | undefined;
    const warningSpy = vi.spyOn(Logger, "logWarning");
    const deleteSpy = vi.spyOn(IModelImporter.prototype, "deleteElements");
    const keptWarnings = () =>
      warningSpy.mock.calls.filter(([, message]) =>
        String(message).startsWith("Kept ")
      );

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
      await sourceDb.locks.acquireLocks({ shared: "0x10", exclusive: "0x1" });
      await targetDb.locks.acquireLocks({ shared: "0x10", exclusive: "0x1" });

      const insertPhysicalObject = (
        txn: EditTxn,
        modelId: Id64String,
        categoryId: Id64String,
        name: string
      ) =>
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({ scope: "0x1", spec: "0x1", value: name }),
          userLabel: name,
        } as PhysicalElementProps);
      const source = withEditTxn(sourceDb, "insert source data", (txn) => {
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
        const elementId = insertPhysicalObject(
          txn,
          modelId,
          categoryId,
          "SourceElement"
        );
        return { modelId, categoryId, elementId };
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Initial source data",
        retainLocks: true,
      });
      const sourceCategoryFederationGuid = sourceDb.elements.getElement(
        source.categoryId
      ).federationGuid;
      expect(sourceCategoryFederationGuid).to.not.be.undefined;

      const processAllEditTxn = createStartedEditTxn(targetDb);
      const processAllTransformer = new IModelTransformer({
        source: sourceDb,
        target: processAllEditTxn,
      });
      await processAllTransformer.process();
      processAllTransformer.dispose();
      processAllEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Initial processAll transformation",
        retainLocks: true,
      });

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

      const targetCategoryId = targetDb.elements.queryElementIdByCode(
        SpatialCategory.createCode(
          targetDb,
          IModel.dictionaryId,
          "SourceCategory"
        )
      )!;
      const targetModelId = IModelTestUtils.queryByCodeValue(
        targetDb,
        "SourceModel"
      );
      const targetOnlyId = withEditTxn(targetDb, "insert target-only", (txn) =>
        insertPhysicalObject(txn, targetModelId, targetCategoryId, "TargetOnly")
      );
      await targetDb.pushChanges({
        accessToken,
        description: "Insert target-only element",
        retainLocks: true,
      });

      withEditTxn(sourceDb, "delete element and category", (txn) => {
        txn.deleteElement(source.elementId);
        txn.deleteDefinitionElements([source.categoryId]);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Delete source element and category",
        retainLocks: true,
      });
      await processChanges("Process source category deletion");

      const expectCategoryKeptWithProvenance = () => {
        // Provenance is the FederationGuid shared with the source category.
        expect(
          targetDb!.elements.tryGetElement(targetCategoryId)?.federationGuid
        ).to.equal(sourceCategoryFederationGuid);
      };
      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "SourceElement")
      ).to.equal(Id64.invalid);
      expectCategoryKeptWithProvenance();
      expect(keptWarnings()).to.have.length(1);
      expect(keptWarnings()[0][1]).to.contain(targetCategoryId);
      expect(keptWarnings()[0][1]).to.contain(targetOnlyId);

      // Once nothing uses the category, a later sync still doesn't delete it: its source deletion was already processed.
      withEditTxn(targetDb, "delete target-only", (txn) =>
        txn.deleteElement(targetOnlyId)
      );
      await targetDb.pushChanges({
        accessToken,
        description: "Delete target-only element",
        retainLocks: true,
      });
      withEditTxn(sourceDb, "insert later source data", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "LaterCategory",
          {}
        );
        insertPhysicalObject(txn, source.modelId, categoryId, "LaterElement");
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Insert later source element",
        retainLocks: true,
      });
      warningSpy.mockClear();
      deleteSpy.mockClear();
      await processChanges("Process later source change");

      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "LaterElement")
      ).to.not.equal(Id64.invalid);
      expectCategoryKeptWithProvenance();
      expect(keptWarnings()).to.have.length(0);
      for (const [elementIds] of deleteSpy.mock.calls)
        expect(elementIds.has(targetCategoryId)).to.be.false;
    } finally {
      warningSpy.mockRestore();
      deleteSpy.mockRestore();
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

  it("keeps a geometry part that a target-only element places when the source deletes it", async () => {
    const sourceIModelId = await createPopulatedIModelHubIModel(
      IModelTransformerTestUtils.generateUniqueName("KeptPartSource")
    );
    const targetIModelId = await createPopulatedIModelHubIModel(
      IModelTransformerTestUtils.generateUniqueName("KeptPartTarget")
    );
    let sourceDb: BriefcaseDb | undefined;
    let targetDb: BriefcaseDb | undefined;
    const warningSpy = vi.spyOn(Logger, "logWarning");

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
      await sourceDb.locks.acquireLocks({ shared: "0x10", exclusive: "0x1" });
      await targetDb.locks.acquireLocks({ shared: "0x10", exclusive: "0x1" });

      const size = Point3d.create(1, 1, 1);
      const insertPartUser = (
        txn: EditTxn,
        modelId: Id64String,
        categoryId: Id64String,
        partId: Id64String,
        name: string
      ) =>
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({ scope: "0x1", spec: "0x1", value: name }),
          placement: { origin: [0, 0, 0], angles: {} },
          geom: IModelTestUtils.createBox(
            size,
            categoryId,
            IModelDb.getDefaultSubCategoryId(categoryId),
            undefined,
            partId
          ),
        } as PhysicalElementProps);
      const source = withEditTxn(sourceDb, "insert source data", (txn) => {
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
        const partId = txn.insertElement({
          classFullName: GeometryPart.classFullName,
          model: IModel.dictionaryId,
          code: GeometryPart.createCode(
            sourceDb!,
            IModel.dictionaryId,
            "SourcePart"
          ),
          geom: IModelTestUtils.createBox(size),
        } as GeometryPartProps);
        const elementId = insertPartUser(
          txn,
          modelId,
          categoryId,
          partId,
          "SourceElement"
        );
        return { partId, elementId };
      });
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
      processAllTransformer.dispose();
      processAllEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Initial processAll transformation",
        retainLocks: true,
      });

      const targetPartId = targetDb.elements.queryElementIdByCode(
        GeometryPart.createCode(targetDb, IModel.dictionaryId, "SourcePart")
      )!;
      expect(targetPartId).to.not.be.undefined;
      const targetCategoryId = targetDb.elements.queryElementIdByCode(
        SpatialCategory.createCode(
          targetDb,
          IModel.dictionaryId,
          "SourceCategory"
        )
      )!;
      const targetModelId = IModelTestUtils.queryByCodeValue(
        targetDb,
        "SourceModel"
      );
      const targetOnlyId = withEditTxn(targetDb, "insert target-only", (txn) =>
        insertPartUser(
          txn,
          targetModelId,
          targetCategoryId,
          targetPartId,
          "TargetOnly"
        )
      );
      await targetDb.pushChanges({
        accessToken,
        description: "Insert target-only part user",
        retainLocks: true,
      });

      withEditTxn(sourceDb, "delete element and part", (txn) => {
        txn.deleteElement(source.elementId);
        txn.deleteDefinitionElements([source.partId]);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Delete source element and part",
        retainLocks: true,
      });

      // Only native code reads geometry, so native deletion refuses the part instead of the transformer skipping it.
      const editTxn = createStartedEditTxn(targetDb);
      const transformer = new IModelTransformer(
        { source: sourceDb, target: editTxn },
        { argsForProcessChanges: {} }
      );
      await transformer.process();
      transformer.dispose();
      editTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Process source part deletion",
        retainLocks: true,
      });

      expect(
        IModelTestUtils.queryByCodeValue(targetDb, "SourceElement")
      ).to.equal(Id64.invalid);
      expect(targetDb.elements.tryGetElement(targetPartId)).to.not.be.undefined;
      expect(targetDb.elements.tryGetElement(targetOnlyId)).to.not.be.undefined;
      const refusedWarnings = warningSpy.mock.calls.filter(([, message]) =>
        String(message).includes("native deletion refused")
      );
      expect(refusedWarnings).to.have.length(1);
      expect(refusedWarnings[0][1]).to.contain(
        `${targetPartId} (BisCore:GeometryPart "SourcePart", DefinitionInUse)`
      );
    } finally {
      warningSpy.mockRestore();
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
    vi.restoreAllMocks();
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
    vi.restoreAllMocks();
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
    await transformer.process();
    changeTargetEditTxn.end();

    const queryReader = targetDb.createQueryReader(
      `SELECT COUNT(*) FROM ${Subject.classFullName}`
    );
    await queryReader.step();
    const subjectCount = queryReader.current.toArray()[0];
    expect(subjectCount).to.equal(3); // RootSubject + 2 created subjects
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

    it("should process changes successfully when element is deleted after existing elements were expanded into overflow table", async () => {
      // Import initial schema with property count that does not require overflow table
      const initialSchema = generateSchema(1, "SourceProperty", 5);
      await sourceDb.importSchemaStrings([initialSchema]);
      const elementId = createPhysicalElement(
        sourceDb,
        "DynamicTestSchema:DynamicPhysicalElement"
      );
      const aspectId = withEditTxn(
        sourceDb,
        "insert aspect excluded from element provenance lookup",
        (txn) =>
          txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(elementId),
            scope: { id: IModel.rootSubjectId },
            kind: "Document",
            identifier: "deleted-aspect",
          } as ExternalSourceAspectProps)
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
      const openFileSpy = vi.spyOn(ChangesetReader, "openFile");
      const downloads = recordChangesetDownloads();
      const processedDeletionIds: Id64String[] = [];
      const processDeletedElement =
        transformer["processDeletedElement"].bind(transformer);
      transformer["processDeletedElement"] = async (...args) => {
        processedDeletionIds.push(args[0]);
        return processDeletedElement(...args);
      };
      const findTargetElementIdSpy = vi.spyOn(
        transformer.context,
        "findTargetElementId"
      );
      try {
        await transformer.process();
        downloads.restore();
        secondTransformEditTxn.end();
        await targetDb.pushChanges({
          description: "Transformation 2: Process Changes with deletion",
          retainLocks: true,
        });

        const selectedChangesetPaths = downloads.files.map(
          (csFile) => csFile.pathname
        );
        expect(openFileSpy).toHaveBeenCalledTimes(
          selectedChangesetPaths.length
        );
        expect(
          openFileSpy.mock.calls.map(([args]) => args.fileName)
        ).to.deep.equal(selectedChangesetPaths);
        expect(
          openFileSpy.mock.calls.map(([args]) => args.propFilter)
        ).to.deep.equal(
          selectedChangesetPaths.map(
            () => PropertyFilter.InstanceKeyAndIdentifiers
          )
        );
        expect(findTargetElementIdSpy).toHaveBeenCalledWith(elementId);
        expect(processedDeletionIds).toContain(elementId);
        expect(processedDeletionIds).not.toContain(aspectId);
      } finally {
        downloads.restore();
        openFileSpy.mockRestore();
      }

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

    it("honors a guidless deletion remap and ignores a missing mapping", async () => {
      const sourceSubjectId = withEditTxn(
        sourceDb,
        "insert guidless source subject",
        (txn) => {
          const subject = Subject.create(
            sourceDb,
            IModel.rootSubjectId,
            "Context mapped source"
          );
          subject.federationGuid = Guid.empty;
          return subject.insert(txn);
        }
      );
      await sourceDb.pushChanges({
        description: "Insert guidless source subject",
        retainLocks: true,
      });

      const initialEditTxn = createStartedEditTxn(targetDb);
      const transformer = new IModelTransformer({
        source: sourceDb,
        target: initialEditTxn,
      });
      await transformer.process();
      const provenanceTargetId =
        transformer.context.findTargetElementId(sourceSubjectId);
      transformer.dispose();
      initialEditTxn.end();
      await targetDb.pushChanges({
        description: "Initial guidless subject transformation",
        retainLocks: true,
      });

      const customTargetId = withEditTxn(
        targetDb,
        "insert custom deletion target",
        (txn) =>
          Subject.insert(txn, IModel.rootSubjectId, "Custom deletion target")
      );
      await targetDb.pushChanges({
        description: "Insert custom deletion target",
        retainLocks: true,
      });

      const unmappedSourceId = withEditTxn(
        sourceDb,
        "insert never-synchronized guidless subject",
        (txn) => {
          const subject = Subject.create(
            sourceDb,
            IModel.rootSubjectId,
            "Never synchronized"
          );
          subject.federationGuid = Guid.empty;
          return subject.insert(txn);
        }
      );
      await sourceDb.pushChanges({
        description: "Insert never-synchronized guidless subject",
        retainLocks: true,
      });

      withEditTxn(sourceDb, "delete guidless source subjects", (txn) => {
        txn.deleteElement(sourceSubjectId);
        txn.deleteElement(unmappedSourceId);
      });
      await sourceDb.pushChanges({
        description: "Delete guidless source subjects",
        retainLocks: true,
      });

      class CustomDeletionRemapTransformer extends IModelTransformer {
        public override async addCustomChanges(): Promise<void> {
          expect(this.context.findTargetElementId(sourceSubjectId)).to.equal(
            provenanceTargetId
          );
          this.context.remapElement(sourceSubjectId, customTargetId);
        }
      }

      const changesEditTxn = createStartedEditTxn(targetDb);
      const changesTransformer = new CustomDeletionRemapTransformer(
        { source: sourceDb, target: changesEditTxn },
        { argsForProcessChanges: {} }
      );
      const addCustomChangesSpy = vi.spyOn(
        changesTransformer,
        "addCustomChanges"
      );
      await changesTransformer.process();
      expect(
        changesTransformer.context.findTargetElementId(unmappedSourceId)
      ).to.equal(Id64.invalid);
      changesTransformer.dispose();
      changesEditTxn.end();

      expect(addCustomChangesSpy).toHaveBeenCalledOnce();
      expect(targetDb.elements.tryGetElement(customTargetId)).toBeUndefined();
      expect(targetDb.elements.tryGetElement(provenanceTargetId)).toBeDefined();
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
          federationGuid: Guid.empty,
          userLabel: "TestClassElement",
          SourceProperty1: "value1",
        } as GeometricElementProps);
      });
    }
  });
});
