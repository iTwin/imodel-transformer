/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect, vi } from "vitest";

import {
  BriefcaseDb,
  DefinitionContainer,
  DefinitionModel,
  DefinitionPartition,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSourceAspect,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, GuidString } from "@itwin/core-bentley";
import {
  Code,
  IModel,
  InformationPartitionElementProps,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelTransformer } from "../../imodel-transformer";

import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { registerHubTestContext } from "../TestUtils/HubTestContext";

describe("IModelTransformerHub - definition processing", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext(
    "IModelTransformerHubDefinitionProcessing",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
    }
  );

  it("should delete definition elements and models when processing changes", async () => {
    let definitionPartitionId1: string;
    let definitionPartitionModelId1: string;
    let definitionPartitionId2: string;
    let definitionPartitionModelId2: string;
    let definitionContainerId1: string;
    let definitionContainerModelId1: string;

    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "DefinitionHierarchyMaster"
      ),
      noLocks: true,
    });
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let branchIModelId: GuidString | undefined;

    try {
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      withEditTxn(masterDb, "create definition hierarchy", (txn) => {
        const definitionPartitionProps: InformationPartitionElementProps = {
          classFullName: DefinitionPartition.classFullName,
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
          code: Code.createEmpty(),
        };
        definitionPartitionId1 = txn.insertElement(definitionPartitionProps);
        definitionPartitionId2 = txn.insertElement(definitionPartitionProps);
        definitionPartitionModelId1 = txn.insertModel({
          classFullName: DefinitionModel.classFullName,
          modeledElement: { id: definitionPartitionId1 },
          parentModel: IModel.repositoryModelId,
        });
        definitionPartitionModelId2 = txn.insertModel({
          classFullName: DefinitionModel.classFullName,
          modeledElement: { id: definitionPartitionId2 },
          parentModel: IModel.repositoryModelId,
        });
        definitionContainerId1 = txn.insertElement({
          classFullName: DefinitionContainer.classFullName,
          model: definitionPartitionModelId1,
          code: Code.createEmpty(),
        });
        definitionContainerModelId1 = txn.insertModel({
          classFullName: DefinitionModel.classFullName,
          modeledElement: { id: definitionContainerId1 },
          parentModel: definitionPartitionModelId1,
        });
      });
      await masterDb.pushChanges({
        accessToken,
        description: "create definition hierarchy",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "DefinitionHierarchyBranch"
        ),
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
        { wasSourceIModelCopiedToTarget: true }
      );
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(masterDb, "delete definition models", (txn) => {
        txn.deleteModel(definitionContainerModelId1);
        txn.deleteModel(definitionPartitionModelId1);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "delete definition models",
      });
      const firstSyncTxn = createStartedEditTxn(branchDb);
      const firstSync = new IModelTransformer(
        { source: masterDb, target: firstSyncTxn },
        { argsForProcessChanges: { startChangeset: { index: undefined } } }
      );
      await withTransformerLifecycle(firstSync, [firstSyncTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "synchronize first definition model deletion",
      });

      withEditTxn(masterDb, "delete second definition model", (txn) => {
        txn.deleteModel(definitionPartitionModelId2);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "delete second definition model",
      });
      const secondSyncTxn = createStartedEditTxn(branchDb);
      const secondSync = new IModelTransformer(
        { source: masterDb, target: secondSyncTxn },
        { argsForProcessChanges: { startChangeset: { index: undefined } } }
      );
      await withTransformerLifecycle(secondSync, [secondSyncTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "synchronize second definition model deletion",
      });

      expect(masterDb.models.tryGetModel(definitionContainerModelId1!)).to.be
        .undefined;
      expect(masterDb.elements.tryGetElement(definitionContainerId1!)).to.be
        .undefined;
      expect(masterDb.models.tryGetModel(definitionPartitionModelId1!)).to.be
        .undefined;
      expect(masterDb.elements.tryGetElement(definitionPartitionId2!)).to.not.be
        .undefined;
      expect(masterDb.models.tryGetModel(definitionPartitionModelId2!)).to.be
        .undefined;
      expect(branchDb.models.tryGetModel(definitionContainerModelId1!)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(definitionContainerId1!)).to.be
        .undefined;
      expect(branchDb.models.tryGetModel(definitionPartitionModelId1!)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(definitionPartitionId2!)).to.not.be
        .undefined;
      expect(branchDb.models.tryGetModel(definitionPartitionModelId2!)).to.be
        .undefined;
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branchDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
      if (branchIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
      vi.restoreAllMocks();
    }
  });

  it("should use the lastMod of provenanceDb's element as the provenance aspect version", async () => {
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "LastModProvenanceMaster"
      ),
      noLocks: true,
    });
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let branchIModelId: GuidString | undefined;

    try {
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      const { modelId, categoryId } = withEditTxn(
        masterDb,
        "populate master object",
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
      withEditTxn(masterDb, "insert master object", (txn) => {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "1",
          }),
          userLabel: "1",
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(0, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
          jsonProperties: { updateState: 1 },
        } as PhysicalElementProps);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "populate master object",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "LastModProvenanceBranch"
        ),
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

      withEditTxn(branchDb, "update branch object", (txn) => {
        const element = branchDb!.elements.getElement(
          IModelTestUtils.queryByUserLabel(branchDb!, "1")
        );
        txn.updateElement({
          ...element.toJSON(),
          jsonProperties: { ...element.jsonProperties, updateState: 2 },
        });
      });
      await branchDb.pushChanges({
        accessToken,
        description: "update branch object",
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
        description: "reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "reverse sync",
      });

      const elem1InMaster = IModelTestUtils.queryByUserLabel(masterDb, "1");
      expect(elem1InMaster).not.to.be.undefined;
      const elem1InBranch = IModelTestUtils.queryByUserLabel(branchDb, "1");
      expect(elem1InBranch).not.to.be.undefined;
      const lastModInMaster =
        masterDb.elements.queryLastModifiedTime(elem1InMaster);
      const physElem1Esas = branchDb.elements.getAspects(
        elem1InBranch,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      expect(physElem1Esas).to.have.lengthOf(1);
      expect(physElem1Esas[0].version).to.equal(lastModInMaster);
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branchDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
      if (branchIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
      vi.restoreAllMocks();
    }
  });

  it("should successfully process changes when codeValues are switched around between elements", async () => {
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "SwitchedCodeValuesMaster"
      ),
      noLocks: true,
    });
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let branchIModelId: GuidString | undefined;

    try {
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      const { modelId, categoryId } = withEditTxn(
        masterDb,
        "populate master objects",
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
      withEditTxn(masterDb, "insert master objects", (txn) => {
        for (const [name, updateState] of Object.entries({
          1: 1,
          2: 2,
          3: 3,
        })) {
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
      await masterDb.pushChanges({
        accessToken,
        description: "populate master objects",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "SwitchedCodeValuesBranch"
        ),
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
        { wasSourceIModelCopiedToTarget: true }
      );
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(masterDb, "swap element code values", (txn) => {
        const elem1 = masterDb!.elements.getElement(
          IModelTransformerTestUtils.queryByCodeValue(masterDb!, "1")
        );
        const elem2 = masterDb!.elements.getElement(
          IModelTransformerTestUtils.queryByCodeValue(masterDb!, "2")
        );
        const elem3 = masterDb!.elements.getElement(
          IModelTransformerTestUtils.queryByCodeValue(masterDb!, "3")
        );
        elem1.code.value = "tempValue";
        txn.updateElement(elem1.toJSON());
        elem2.code.value = "1";
        txn.updateElement(elem2.toJSON());
        elem3.code.value = "2";
        txn.updateElement(elem3.toJSON());
        elem1.code.value = "3";
        txn.updateElement(elem1.toJSON());
      });
      await masterDb.pushChanges({
        accessToken,
        description: "swap element code values",
      });

      const branchSyncEditTxn = createStartedEditTxn(branchDb);
      const branchSyncer = new IModelTransformer(
        { source: masterDb, target: branchSyncEditTxn },
        { argsForProcessChanges: { startChangeset: { index: undefined } } }
      );
      await withTransformerLifecycle(branchSyncer, [branchSyncEditTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "synchronize swapped element code values",
      });

      for (const db of [branchDb, masterDb]) {
        const elem1Id = IModelTestUtils.queryByCodeValue(db, "1");
        const elem2Id = IModelTestUtils.queryByCodeValue(db, "2");
        const elem3Id = IModelTestUtils.queryByCodeValue(db, "3");
        const elem1 = db.elements.getElement(elem1Id);
        const elem2 = db.elements.getElement(elem2Id);
        const elem3 = db.elements.getElement(elem3Id);
        expect(elem1.userLabel).to.equal("2");
        expect(elem2.userLabel).to.equal("3");
        expect(elem3.userLabel).to.equal("1");
      }
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branchDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
      if (branchIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
    }
  });

  it("should successfully process changes when Definition Elements' codeValues are switched around", async () => {
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "SwitchedDefinitionCodesMaster"
      ),
      noLocks: true,
    });
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let branchIModelId: GuidString | undefined;

    try {
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      withEditTxn(masterDb, "insert definition categories", (txn) => {
        const categoryA = SpatialCategory.create(
          masterDb!,
          IModel.dictionaryId,
          "A"
        );
        const categoryB = SpatialCategory.create(
          masterDb!,
          IModel.dictionaryId,
          "B"
        );
        categoryA.userLabel = "A";
        categoryB.userLabel = "B";
        txn.insertElement(categoryA.toJSON());
        txn.insertElement(categoryB.toJSON());
      });
      await masterDb.pushChanges({
        accessToken,
        description: "insert definition categories",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "SwitchedDefinitionCodesBranch"
        ),
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
        { wasSourceIModelCopiedToTarget: true }
      );
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(masterDb, "swap definition category codes", (txn) => {
        const categoryA = masterDb!.elements.getElement(
          SpatialCategory.createCode(masterDb!, IModel.dictionaryId, "A")
        );
        const categoryB = masterDb!.elements.getElement(
          SpatialCategory.createCode(masterDb!, IModel.dictionaryId, "B")
        );
        categoryA.code.value = "temp";
        txn.updateElement(categoryA.toJSON());
        categoryB.code.value = "A";
        txn.updateElement(categoryB.toJSON());
        categoryA.code.value = "B";
        txn.updateElement(categoryA.toJSON());
      });
      await masterDb.pushChanges({
        accessToken,
        description: "swap definition category codes",
      });

      const branchSyncEditTxn = createStartedEditTxn(branchDb);
      const branchSyncer = new IModelTransformer(
        { source: masterDb, target: branchSyncEditTxn },
        { argsForProcessChanges: { startChangeset: { index: undefined } } }
      );
      await withTransformerLifecycle(branchSyncer, [branchSyncEditTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "synchronize swapped definition category codes",
      });

      for (const db of [branchDb, masterDb]) {
        const categoryA = db.elements.getElement(
          SpatialCategory.createCode(db, IModel.dictionaryId, "A")
        );
        const categoryB = db.elements.getElement(
          SpatialCategory.createCode(db, IModel.dictionaryId, "B")
        );
        expect(categoryA.userLabel).to.equal(
          "B",
          `categoryA.userlabel mismatch in ${db.name}`
        );
        expect(categoryB.userLabel).to.equal(
          "A",
          `categoryB.userlabel mismatch in ${db.name}`
        );
      }
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branchDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
      if (branchIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
    }
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
    await withTransformerLifecycle(transformer, [initialTargetEditTxn]);

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
    await withTransformerLifecycle(transformer, [changeTargetEditTxn]);

    const queryReader = targetDb.createQueryReader(
      `SELECT COUNT(*) FROM ${Subject.classFullName}`
    );
    await queryReader.step();
    const subjectCount = queryReader.current.toArray()[0];
    expect(subjectCount).to.equal(3); // RootSubject + 2 created subjects
  });
});
