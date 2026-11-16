/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, assert, beforeEach, expect } from "vitest";
import {
  BisCoreSchema,
  BriefcaseDb,
  BriefcaseManager,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSourceAspect,
  GenericSchema,
  IModelDb,
  ModelSelector,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  withEditTxn,
} from "@itwin/core-backend";
import {
  AccessToken,
  Guid,
  GuidString,
  Id64,
  Id64Array,
  Id64String,
} from "@itwin/core-bentley";
import {
  BisCodeSpec,
  Code,
  ColorDef,
  ElementProps,
  IModel,
  InformationPartitionElementProps,
  PhysicalElementProps,
  Placement3d,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import {
  ChangedInstanceIds,
  IModelImporter,
  IModelTransformer,
} from "../../imodel-transformer.js";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
  PhysicalModelConsolidator,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

const { count } = IModelTestUtils;

/** Models, partitions, model consolidation, and model selectors. */
describe("Model sync", () => {
  const hub = useTransformerHub("ModelSync");
  const { prepareBriefcase, closeAndDeleteBriefcase } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
  });

  it("should consolidate PhysicalModels", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("ConsolidateModelsSource");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("ConsolidateModelsTarget");
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

      const sourceModelIds: Id64Array = [];
      // Helper functions that take EditTxn
      const insertPhysicalObject = (
        txn: Parameters<Parameters<typeof withEditTxn>[2]>[0],
        catId: Id64String,
        physicalModelId: Id64String,
        modelIndex: number,
        originX: number,
        originY: number,
        undefinedFederationGuid: boolean = false
      ) => {
        const physicalObjectProps1: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: physicalModelId,
          category: catId,
          code: Code.createEmpty(),
          userLabel: `M${modelIndex}-PhysicalObject(${originX},${originY})`,
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: Placement3d.fromJSON({
            origin: { x: originX, y: originY },
            angles: {},
          }),
        };
        if (undefinedFederationGuid)
          physicalObjectProps1.federationGuid = Guid.empty;
        txn.insertElement(physicalObjectProps1);
      };

      const insertModelWithElements = (
        txn: Parameters<Parameters<typeof withEditTxn>[2]>[0],
        catId: Id64String,
        modelIndex: number
      ): Id64String => {
        const sourceModelId: Id64String = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          `PhysicalModel${modelIndex}`
        );
        const xArray: number[] = [
          20 * modelIndex + 1,
          20 * modelIndex + 3,
          20 * modelIndex + 5,
          20 * modelIndex + 7,
          20 * modelIndex + 9,
        ];
        const yArray: number[] = [0, 2, 4, 6, 8];
        let undefinedFederationGuid = false;
        for (const x of xArray) {
          for (const y of yArray) {
            insertPhysicalObject(
              txn,
              catId,
              sourceModelId,
              modelIndex,
              x,
              y,
              undefinedFederationGuid
            );
            undefinedFederationGuid = !undefinedFederationGuid;
          }
        }
        return sourceModelId;
      };

      // Wrap all source inserts in a single EditTxn
      const categoryId = withEditTxn(
        sourceDb,
        "insert category and models",
        (txn) => {
          const catId: Id64String = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "SpatialCategory",
            { color: ColorDef.green.toJSON() }
          );

          // insert models 0-4 with 25 elements each (5*25).
          for (let i = 0; i < 5; i++) {
            sourceModelIds.push(insertModelWithElements(txn, catId, i));
          }

          return catId;
        }
      );
      assert.equal(5, count(sourceDb, PhysicalModel.classFullName));
      assert.equal(125, count(sourceDb, PhysicalObject.classFullName));
      await sourceDb.pushChanges({
        accessToken,
        description: "5 physical models",
      });

      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
      const targetModelId = withEditTxn(
        targetDb,
        "insert target model",
        (txn) =>
          PhysicalModel.insert(txn, IModel.rootSubjectId, "PhysicalModel")
      );
      assert.isTrue(Id64.isValidId64(targetModelId));

      const consolidateEditTxn1 = createStartedEditTxn(targetDb);
      let transformer = new PhysicalModelConsolidator(
        sourceDb,
        targetDb,
        consolidateEditTxn1,
        targetModelId
      );
      await transformer.process();
      consolidateEditTxn1.end();

      assert.equal(1, count(targetDb, PhysicalModel.classFullName));
      const targetPartition =
        targetDb.elements.getElement<PhysicalPartition>(targetModelId);
      assert.equal(
        targetPartition.code.value,
        "PhysicalModel",
        "Target PhysicalModel name should not be overwritten during consolidation"
      );
      assert.equal(125, count(targetDb, PhysicalObject.classFullName));
      const aspects = targetDb.elements.getAspects(
        targetPartition.id,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      expect(aspects.map((aspect) => aspect.identifier)).to.have.members(
        sourceModelIds
      );
      expect(aspects.length).to.equal(
        5,
        "Provenance should be recorded for each source PhysicalModel"
      );

      // Insert 10 objects under model-1, update model2/partition2, insert models 5 & 6
      withEditTxn(sourceDb, "additional inserts and updates", (txn) => {
        const xArr: number[] = [101, 105];
        const yArr: number[] = [0, 2, 4, 6, 8];
        let undefinedFedGuid = false;
        for (const x of xArr) {
          for (const y of yArr) {
            insertPhysicalObject(
              txn,
              categoryId,
              sourceModelIds[1],
              1,
              x,
              y,
              undefinedFedGuid
            );
            undefinedFedGuid = !undefinedFedGuid;
          }
        }

        // Update model2 and partition2
        const model2 = sourceDb.models.getModel(sourceModelIds[2]);
        model2.isPrivate = true;
        model2.update(txn);

        const partition2 = sourceDb.elements.getElement(sourceModelIds[2]);
        partition2.userLabel = "Element-Updated";
        partition2.update(txn);

        // insert model 5 & 6 and 50 physical objects
        for (let i = 5; i < 7; i++) {
          sourceModelIds.push(insertModelWithElements(txn, categoryId, i));
        }
      });
      await sourceDb.pushChanges({ description: "additional PhysicalModels" });
      // 2 models added
      assert.equal(7, count(sourceDb, PhysicalModel.classFullName));
      // 60 elements added
      assert.equal(185, count(sourceDb, PhysicalObject.classFullName));
      const consolidateEditTxn2 = createStartedEditTxn(targetDb);
      transformer = new PhysicalModelConsolidator(
        sourceDb,
        targetDb,
        consolidateEditTxn2,
        targetModelId,
        {
          startChangeset: sourceDb.changeset,
        }
      );
      await transformer.process();
      transformer.dispose();
      consolidateEditTxn2.end();

      const sql = `SELECT ECInstanceId, Model.Id AS modelId FROM ${PhysicalObject.classFullName}`;
      let objectCounter = 0;
      for await (const row of targetDb.createQueryReader(sql)) {
        const targetElementId = row.id;
        const targetElement = targetDb.elements.getElement<PhysicalObject>({
          id: targetElementId,
          wantGeometry: true,
        });
        assert.exists(targetElement.geom);
        assert.isFalse(targetElement.calculateRange3d().isNull);
        const targetElementModelId = row.modelId;
        assert.equal(targetModelId, targetElementModelId);
        ++objectCounter;
      }
      assert.equal(185, objectCounter);

      assert.equal(1, count(targetDb, PhysicalModel.classFullName));
      let modelId = Id64.invalid;
      const modelReader = targetDb.createQueryReader(
        `SELECT ECInstanceId, isPrivate FROM ${PhysicalModel.classFullName}`
      );
      if (await modelReader.step()) {
        const isPrivate = modelReader.current.isPrivate;
        assert.isFalse(isPrivate);
        modelId = modelReader.current.id;
      }
      assert.isTrue(Id64.isValidId64(modelId));

      const physicalPartition =
        targetDb.elements.getElement<PhysicalPartition>(modelId);
      assert.equal("PhysicalModel", physicalPartition.code.value);

      const sourceAspects = targetDb.elements.getAspects(
        modelId,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      expect(sourceAspects.map((aspect) => aspect.identifier)).to.have.members(
        sourceModelIds
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
      await provenanceInitializer.process();
      provenanceInitializer.dispose();
      provenanceInitEditTxn.end();

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
      await synchronizer.process();
      expect(didExportModelSelector).to.be.true;
      expect(didImportModelSelector).to.be.true;
      synchronizer.dispose();
      injectedEditTxn.end();
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

  it("should delete model when its partition was recreated, but model was left deleted", async () => {
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

      const constPartitionFedGuid = Guid.createValue();
      const { originalPartitionId, modelId } = withEditTxn(
        sourceDb,
        "insert elements & models",
        (txn) => {
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
          return { originalPartitionId: partId, modelId: modId };
        }
      );
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

      withEditTxn(sourceDb, "recreate elements & models", (txn) => {
        txn.deleteModel(modelId);
        txn.deleteElement(originalPartitionId);
        txn.insertElement({
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
      });
      await sourceDb.pushChanges({
        description: "recreated elements & models",
      });

      const changeTargetEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changeTargetEditTxn },
        { argsForProcessChanges: { startChangeset: sourceDb.changeset } }
      );
      await transformer.process();
      changeTargetEditTxn.end();
      await targetDb.pushChanges({
        description: "change processing transformation",
      });

      const targetPartition = targetDb.elements.getElement<PhysicalPartition>(
        { federationGuid: constPartitionFedGuid },
        PhysicalPartition
      );
      expect(targetPartition.code.value).to.be.equal("recreated partition");

      expect(count(sourceDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(targetDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(sourceDb, PhysicalModel.classFullName)).to.equal(0);
      expect(count(targetDb, PhysicalModel.classFullName)).to.equal(0);
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

    it("should leave model contents correct when model partition was recreated with different federation guid and the same code value", async () => {
      // Arrange
      const specId = sourceDb.codeSpecs.getByName(
        BisCodeSpec.physicalMaterial
      ).id;
      const { subjectId, physicalModelId, categoryId, physicalObjectId } =
        withEditTxn(sourceDb, "recreate elements & models", (txn) => {
          // prepare source - create initial subject, model, and element
          const subjId = Subject.insert(txn, IModel.rootSubjectId, "Subject1");
          const physModId = PhysicalModel.insert(txn, subjId, "PhysicalModel");
          const catId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: physModId,
            category: catId,
            code: new Code({
              value: "PO1",
              scope: IModel.rootSubjectId,
              spec: specId,
            }),
          };
          const physicalObjId = txn.insertElement(physicalObjectProps);
          return {
            subjectId: subjId,
            physicalModelId: physModId,
            categoryId: catId,
            physicalObjectId: physicalObjId,
          };
        });
      await sourceDb.pushChanges({
        accessToken,
        description: "First changes",
        retainLocks: true,
      });

      // Run first transform
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      firstTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "First transformation",
        retainLocks: true,
      });

      // Recreate source model partition with different federation guid
      withEditTxn(sourceDb, "delete and recreate model", (txn) => {
        txn.deleteElement(physicalObjectId);
        txn.deleteModel(physicalModelId);
        txn.deleteElement(physicalModelId);
        const physicalModel2Id = PhysicalModel.insert(
          txn,
          subjectId,
          "PhysicalModel"
        );
        const physicalObject2Props: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: physicalModel2Id,
          category: categoryId,
          code: new Code({
            value: "PO2",
            scope: IModel.rootSubjectId,
            spec: specId,
          }),
        };
        txn.insertElement(physicalObject2Props);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Second changes",
      });

      // Act - run second transform with change processing
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        {
          argsForProcessChanges: {},
        }
      );
      await transformer.process();
      transformer.dispose();
      secondTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Second transformation",
      });

      // Assert - verify that new elements and models exist with correct values
      expect(
        IModelTransformerTestUtils.queryByCodeValue(targetDb, "PO2")
      ).to.not.be.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryModelIddByModeledElementCodeValue(
          targetDb,
          "PhysicalModel"
        )
      ).to.not.be.equal(Id64.invalid);
    });

    it("should delete model when model partition was recreated with different federation guid and the same code value but model was left deleted", async () => {
      // Arrange
      const specId = sourceDb.codeSpecs.getByName(
        BisCodeSpec.physicalMaterial
      ).id;
      const { subjectId, physicalModelId, physicalObjectId } = withEditTxn(
        sourceDb,
        "recreate elements & models",
        (txn) => {
          // prepare source - create initial subject, model, and element
          const subjId = Subject.insert(txn, IModel.rootSubjectId, "Subject1");
          const physModId = PhysicalModel.insert(txn, subjId, "PhysicalModel");
          const catId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: physModId,
            category: catId,
            code: new Code({
              value: "PO1",
              scope: IModel.rootSubjectId,
              spec: specId,
            }),
          };
          const physicalObjId = txn.insertElement(physicalObjectProps);
          return {
            subjectId: subjId,
            physicalModelId: physModId,
            physicalObjectId: physicalObjId,
          };
        }
      );
      await sourceDb.pushChanges({
        accessToken,
        description: "First changes",
        retainLocks: true,
      });

      // Run first transform
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      firstTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "First transformation",
        retainLocks: true,
      });

      // Recreate source model partition with different federation guid
      withEditTxn(sourceDb, "delete and recreate model", (txn) => {
        txn.deleteElement(physicalObjectId);
        txn.deleteModel(physicalModelId);
        txn.deleteElement(physicalModelId);
        const partitionProps: InformationPartitionElementProps = {
          classFullName: PhysicalPartition.classFullName,
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsPartitionElements(subjectId),
          code: PhysicalPartition.createCode(
            txn.iModel,
            subjectId,
            "PhysicalModel"
          ),
        };
        txn.insertElement(partitionProps);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Second changes",
      });

      // Act - run second transform with change processing
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        {
          argsForProcessChanges: {},
        }
      );
      await transformer.process();
      transformer.dispose();
      secondTransformEditTxn.end();
      await targetDb.pushChanges({
        accessToken,
        description: "Second transformation",
      });

      // Assert - verify that new elements and models exist with correct values
      expect(
        IModelTransformerTestUtils.queryByCodeValue(targetDb, "PhysicalModel")
      ).to.not.be.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryModelIddByModeledElementCodeValue(
          targetDb,
          "PhysicalModel"
        )
      ).to.be.equal(Id64.invalid);
    });
  });
});
