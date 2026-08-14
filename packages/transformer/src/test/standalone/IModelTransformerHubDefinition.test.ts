/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect, vi } from "vitest";

import {
  BriefcaseDb,
  CategorySelector,
  DisplayStyle3d,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  IModelDb,
  ModelSelector,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SpatialCategory,
  SpatialViewDefinition,
  Subject,
  SubjectOwnsPartitionElements,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, Guid, GuidString, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
  Placement3d,
  SpatialViewDefinitionProps,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import { IModelExporter, IModelTransformer } from "../../imodel-transformer";

import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import {
  createPopulatedHubIModel,
  registerHubTestContext,
} from "../TestUtils/HubTestContext";

const { count } = IModelTestUtils;

describe("IModelTransformerHub - definitions", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let saveAndPushChanges: (db: BriefcaseDb, desc: string) => Promise<void>;

  const outputDir = registerHubTestContext(
    "IModelTransformerHubDefinition",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
      saveAndPushChanges = context.saveAndPushChanges;
    }
  );

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
      await withTransformerLifecycle(transformer, [initialTargetEditTxn]);
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

  it("should update aspects when processing changes", async () => {
    let elementIds: Id64String[] = [];
    const aspectIds: Id64String[] = [];
    const sourceIModelId = await createPopulatedHubIModel(
      outputDir,
      iTwinId,
      "TransformerSource",
      (sourceSeedDb) => {
        elementIds = withEditTxn(
          sourceSeedDb,
          "seed source subjects and aspects",
          (txn) => {
            const createdElementIds = [
              Subject.insert(txn, IModel.rootSubjectId, "Subject1"),
              Subject.insert(txn, IModel.rootSubjectId, "Subject2"),
            ];

            // 10 aspects in total (5 per element)
            createdElementIds.forEach((element) => {
              for (let i = 0; i < 5; ++i) {
                const aspectProps: ExternalSourceAspectProps = {
                  classFullName: ExternalSourceAspect.classFullName,
                  element: new ElementOwnsExternalSourceAspects(element),
                  identifier: `${i}`,
                  kind: "Document",
                  scope: {
                    id: IModel.rootSubjectId,
                    relClassName:
                      "BisCore:ElementScopesExternalSourceIdentifier",
                  },
                };

                const aspectId = txn.insertAspect(aspectProps);
                aspectIds.push(aspectId); // saving for later deletion
              }
            });

            return createdElementIds;
          }
        );
      }
    );

    const targetIModelId = await createPopulatedHubIModel(
      outputDir,
      iTwinId,
      "TransformerTarget"
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

      const exporter = new IModelExporter(sourceDb);
      // First transformation uses processAll (no argsForProcessChanges) to establish provenance
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      const transformer = new IModelTransformer(
        { source: exporter, target: firstTransformEditTxn },
        { includeSourceProvenance: true }
      );

      // run first transformation
      await transformer.process();
      firstTransformEditTxn.end();
      await saveAndPushChanges(targetDb, "First transformation");

      const addedAspectProps: ExternalSourceAspectProps = {
        classFullName: ExternalSourceAspect.classFullName,
        element: new ElementOwnsExternalSourceAspects(elementIds[0]),
        identifier: "aspectAddedAfterFirstTransformation",
        kind: "Document",
        scope: {
          id: IModel.rootSubjectId,
          relClassName: "BisCore:ElementScopesExternalSourceIdentifier",
        },
      };
      withEditTxn(sourceDb, "insert aspect", (txn) => {
        txn.insertAspect(addedAspectProps);
      });
      withEditTxn(sourceDb, "delete aspects", (txn) => {
        aspectIds.slice(5).forEach((aspectId) => txn.deleteAspect(aspectId));
      });

      await saveAndPushChanges(sourceDb, "Update source");

      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      const transformer2 = new IModelTransformer(
        { source: exporter, target: secondTransformEditTxn },
        {
          includeSourceProvenance: true,
          argsForProcessChanges: {
            startChangeset: sourceDb.changeset,
          },
        }
      );
      await transformer2.process();
      secondTransformEditTxn.end();
      await saveAndPushChanges(targetDb, "Second transformation");

      const targetElementIds = targetDb.queryEntityIds({
        from: Subject.classFullName,
        where: "Parent.Id != ?",
        bindings: [IModel.rootSubjectId],
      });
      targetElementIds.forEach((elementId) => {
        const targetAspects = targetDb.elements.getAspects(
          elementId,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[];
        const sourceAspects = sourceDb.elements.getAspects(
          elementId,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[];
        expect(targetAspects.length).to.be.equal(sourceAspects.length + 1); // +1 because provenance aspect was added
        const aspectAddedAfterFirstTransformation = targetAspects.find(
          (aspect) =>
            aspect.identifier === "aspectAddedAfterFirstTransformation"
        );
        expect(aspectAddedAfterFirstTransformation).to.not.be.undefined;
      });
    } finally {
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: sourceIModelId,
      });
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: targetIModelId,
      });
    }
  });

  // will fix in separate PR, tracked here: https://github.com/iTwin/imodel-transformer/issues/27
  it.skip("should delete definition elements when processing changes", async () => {
    let spatialViewDefId: Id64String;
    let displayStyleId: Id64String;
    let spatialViewDef: SpatialViewDefinition;
    let displayStyle: DisplayStyle3d;
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "DefinitionElementsMaster"
      ),
      noLocks: true,
    });
    const branchIModelName = IModelTransformerTestUtils.generateUniqueName(
      "DefinitionElementsBranch"
    );
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let branchIModelId: GuidString | undefined;

    try {
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      withEditTxn(masterDb, "create view definition test data", (txn) => {
        const modelSelector = ModelSelector.create(
          masterDb!,
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
          masterDb!,
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
          masterDb!.elements.getElement<SpatialViewDefinition>(
            spatialViewDefId
          );
      });
      await masterDb.pushChanges({
        accessToken,
        description: "create view definition test data",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: branchIModelName,
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

      withEditTxn(masterDb, "delete view definition test data", (txn) => {
        const notDeleted = txn.deleteDefinitionElements([
          spatialViewDefId,
          displayStyleId,
        ]);
        assert(notDeleted.size === 0);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "delete view definition test data",
      });

      const branchSyncEditTxn = createStartedEditTxn(branchDb);
      const branchSyncer = new IModelTransformer(
        { source: masterDb, target: branchSyncEditTxn },
        { argsForProcessChanges: { startChangeset: { index: 2 } } }
      );
      await withTransformerLifecycle(branchSyncer, [branchSyncEditTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "synchronize deleted view definition data",
      });

      expect(masterDb.elements.tryGetElement(spatialViewDef!.code)).to.be
        .undefined;
      expect(masterDb.elements.tryGetElement(displayStyle!.code)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(spatialViewDef!.code)).to.be
        .undefined;
      expect(branchDb.elements.tryGetElement(displayStyle!.code)).to.be
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

  // Regression test for https://github.com/iTwin/imodel-transformer/issues/28
  it("should succeed when element is deleted and element with the same code is re-added in the next changeset", async () => {
    let categoryId: Id64String;
    let modelId: Id64String;
    let elementId: Id64String;
    let displayStyleId: Id64String;
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "DisplayStyleRegressionMaster"
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
      withEditTxn(masterDb, "create display style regression data", (txn) => {
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
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
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
      await masterDb.pushChanges({
        accessToken,
        description: "create display style regression data",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "DisplayStyleRegressionBranch"
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

      withEditTxn(masterDb, "replace display style", (txn) => {
        txn.deleteDefinitionElements([displayStyleId]);
        DisplayStyle3d.insert(txn, IModel.dictionaryId, "TestDisplayStyle", {
          excludedElements: [elementId],
        });
      });
      await masterDb.pushChanges({
        accessToken,
        description: "replace display style",
      });

      const branchSyncEditTxn = createStartedEditTxn(branchDb);
      const branchSyncer = new IModelTransformer(
        { source: masterDb, target: branchSyncEditTxn },
        { argsForProcessChanges: { startChangeset: { index: 2 } } }
      );
      await withTransformerLifecycle(branchSyncer, [branchSyncEditTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "synchronize replaced display style",
      });

      expect(
        count(branchDb, DisplayStyle3d.classFullName),
        "target should contain one DisplayStyle3d element"
      ).to.equal(1);
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
});
