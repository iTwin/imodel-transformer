/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "vitest";

import {
  BriefcaseDb,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSourceAspect,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, GuidString, Id64String } from "@itwin/core-bentley";
import {
  Code,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
  SubjectProps,
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

const countElementExternalSourceAspects = (
  db: IModelDb,
  elementId: Id64String
) =>
  db.elements
    .getAspects(elementId, ExternalSourceAspect.classFullName)
    .filter(
      (aspect) =>
        (aspect as ExternalSourceAspect).kind ===
        ExternalSourceAspect.Kind.Element
    ).length;

describe("IModelTransformerHub - root elements", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext("IModelTransformerHubRootElements", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
  });

  for (const propagateRootElems of [true, false]) {
    it(`${
      propagateRootElems ? "should" : "shouldn't"
    } propagate changes to rootSubject, repositoryModel, realityDataSourcesModel when skipPropagateChangesToRootElements is set to ${!propagateRootElems}`, async () => {
      const masterIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "RootPropagationMaster"
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
            "RootPropagationBranch"
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

        withEditTxn(branchDb, "update branch objects", (txn) => {
          const element1 = branchDb!.elements.getElement(
            IModelTestUtils.queryByUserLabel(branchDb!, "1")
          );
          txn.updateElement({
            ...element1.toJSON(),
            jsonProperties: { ...element1.jsonProperties, updateState: 2 },
          });
          txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: new Code({
              spec: IModelDb.rootSubjectId,
              scope: IModelDb.rootSubjectId,
              value: "4",
            }),
            userLabel: "4",
            geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
            placement: {
              origin: Point3d.create(0, 0, 0),
              angles: YawPitchRollAngles.createDegrees(0, 0, 0),
            },
            jsonProperties: { updateState: 1 },
          } as PhysicalElementProps);
        });
        await branchDb.pushChanges({
          accessToken,
          description: "update branch objects",
        });

        withEditTxn(branchDb, "update root model and element props", (txn) => {
          const dict = branchDb!.models.getModelProps(IModelDb.dictionaryId);
          txn.updateModel({ ...dict, jsonProperties: { test: 1 } });
          const repositoryModel = branchDb!.models.getModelProps(
            IModelDb.repositoryModelId
          );
          txn.updateModel({ ...repositoryModel, jsonProperties: { test: 2 } });
          const realityDataSourcesModel = branchDb!.models.getModelProps("0xe");
          txn.updateModel({
            ...realityDataSourcesModel,
            jsonProperties: { test: 3 },
          });
          const rootSubjectFromBranch =
            branchDb!.elements.getElementProps<SubjectProps>("0x1");
          txn.updateElement({
            ...rootSubjectFromBranch,
            description: "test description",
            jsonProperties: { test: 4 },
          });
          const realityDataSourcesElement =
            branchDb!.elements.getElementProps("0xe");
          txn.updateElement({
            ...realityDataSourcesElement,
            jsonProperties: { test: 5 },
          });
          const dictionaryElement = branchDb!.elements.getElementProps(
            IModelDb.dictionaryId
          );
          txn.updateElement({
            ...dictionaryElement,
            jsonProperties: { test: 6 },
          });
        });
        await branchDb.pushChanges({
          accessToken,
          description: "update root model and element props",
        });

        const reverseTargetEditTxn = createStartedEditTxn(masterDb);
        const reverseSourceEditTxn = createStartedEditTxn(branchDb);
        const reverseSyncer = new IModelTransformer(
          { source: branchDb, target: reverseTargetEditTxn },
          {
            skipPropagateChangesToRootElements: !propagateRootElems,
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
            sourceEditTxn: reverseSourceEditTxn,
          }
        );
        let reverseSyncSucceeded = false;
        try {
          await reverseSyncer.process();
          reverseSyncSucceeded = true;
        } finally {
          reverseSyncer.dispose();
          reverseTargetEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
          reverseSourceEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
        }
        await branchDb.pushChanges({
          accessToken,
          description: "reverse sync root changes",
        });
        await masterDb.pushChanges({
          accessToken,
          description: "reverse sync root changes",
        });

        const dictionaryModelMaster = masterDb.models.getModel(
          IModelDb.dictionaryId
        );
        const dictionaryModelBranch = branchDb.models.getModel(
          IModelDb.dictionaryId
        );
        expect(dictionaryModelMaster.jsonProperties.test).to.equal(
          propagateRootElems ? 1 : undefined
        );
        expect(dictionaryModelBranch.jsonProperties.test).to.equal(1);

        const repositoryModelMaster = masterDb.models.getModel(
          IModelDb.repositoryModelId
        );
        const repositoryModelBranch = branchDb.models.getModel(
          IModelDb.repositoryModelId
        );
        expect(repositoryModelMaster.jsonProperties.test).to.equal(
          propagateRootElems ? 2 : undefined
        );
        expect(repositoryModelBranch.jsonProperties.test).to.equal(2);

        const realityDataSourcesModelMaster = masterDb.models.getModel("0xe");
        const realityDataSourcesModelBranch = branchDb.models.getModel("0xe");
        expect(realityDataSourcesModelMaster.jsonProperties.test).to.equal(
          propagateRootElems ? 3 : undefined
        );
        expect(realityDataSourcesModelBranch.jsonProperties.test).to.equal(3);

        const rootSubjectMaster = masterDb.elements.getRootSubject();
        const rootSubjectBranch = branchDb.elements.getRootSubject();
        expect(rootSubjectMaster.description).to.equal(
          propagateRootElems ? "test description" : ""
        );
        expect(rootSubjectBranch.description).to.equal("test description");
        expect(rootSubjectMaster.jsonProperties.test).to.equal(
          propagateRootElems ? 4 : undefined
        );
        expect(rootSubjectBranch.jsonProperties.test).to.equal(4);

        const realityDataSourcesElementMaster =
          masterDb.elements.getElementProps("0xe");
        const realityDataSourcesElementBranch =
          branchDb.elements.getElementProps("0xe");
        expect(realityDataSourcesElementMaster.jsonProperties?.test).to.equal(
          propagateRootElems ? 5 : undefined
        );
        expect(realityDataSourcesElementBranch.jsonProperties.test).to.equal(5);

        const dictionaryElementMaster = masterDb.elements.getElementProps(
          IModelDb.dictionaryId
        );
        const dictionaryElementBranch = branchDb.elements.getElementProps(
          IModelDb.dictionaryId
        );
        expect(dictionaryElementMaster.jsonProperties?.test).to.equal(
          propagateRootElems ? 6 : undefined
        );
        expect(dictionaryElementBranch.jsonProperties.test).to.equal(6);
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
  }

  for (const skipPropagateChangesToRootElements of [true, false]) {
    it(`should ${
      skipPropagateChangesToRootElements ? "skip" : "propagate"
    } a remapped root Subject update during processChanges and synchronize its children`, async () => {
      const sourceIModelId = await HubWrappers.createIModel(
        accessToken,
        iTwinId,
        IModelTransformerTestUtils.generateUniqueName(
          "RemappedRootProcessChangesSource"
        )
      );
      const targetIModelId = await HubWrappers.createIModel(
        accessToken,
        iTwinId,
        IModelTransformerTestUtils.generateUniqueName(
          "RemappedRootProcessChangesTarget"
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

        const sourceChildSubjectId = withEditTxn(
          sourceDb,
          "insert source child Subject and update root",
          (txn) => {
            const childSubjectId = Subject.insert(
              txn,
              IModel.rootSubjectId,
              "Source child"
            );
            const rootSubjectProps =
              sourceDb!.elements.getElementProps<SubjectProps>(
                IModel.rootSubjectId
              );
            rootSubjectProps.code = Subject.createCode(
              sourceDb!,
              IModel.rootSubjectId,
              "Source root"
            );
            rootSubjectProps.userLabel = "Source root";
            txn.updateElement(rootSubjectProps);
            return childSubjectId;
          }
        );
        const remappedTargetRootSubjectId = withEditTxn(
          targetDb,
          "insert remapped target Subject",
          (txn) => Subject.insert(txn, IModel.rootSubjectId, "Mapped root")
        );
        await sourceDb.pushChanges({
          accessToken,
          description: "insert source child Subject and update root",
          retainLocks: true,
        });
        await targetDb.pushChanges({
          accessToken,
          description: "insert remapped target Subject",
          retainLocks: true,
        });

        const initialTargetEditTxn = createStartedEditTxn(targetDb);
        let transformer = new IModelTransformer(
          { source: sourceDb, target: initialTargetEditTxn },
          {
            targetScopeElementId: remappedTargetRootSubjectId,
            skipPropagateChangesToRootElements: true,
          }
        );
        transformer.context.remapElement(
          IModel.rootSubjectId,
          remappedTargetRootSubjectId
        );
        let targetChildSubjectId!: Id64String;
        await withTransformerLifecycle(
          transformer,
          [initialTargetEditTxn],
          async () => {
            await transformer.process();
            await transformer.updateSynchronizationVersion({
              initializeReverseSyncVersion: true,
            });
            targetChildSubjectId =
              transformer.context.findTargetElementId(sourceChildSubjectId);
          }
        );
        await targetDb.pushChanges({
          accessToken,
          description: "initial transformation",
          retainLocks: true,
        });

        const targetRootBeforeChanges = targetDb.elements.getElement<Subject>(
          remappedTargetRootSubjectId,
          Subject
        );
        const targetRootLabelBeforeChanges = targetRootBeforeChanges.userLabel;
        const targetRootElementAspectCountBeforeChanges =
          countElementExternalSourceAspects(
            targetDb,
            remappedTargetRootSubjectId
          );

        withEditTxn(sourceDb, "update source root and child Subject", (txn) => {
          const rootSubjectProps =
            sourceDb!.elements.getElementProps<SubjectProps>(
              IModel.rootSubjectId
            );
          rootSubjectProps.userLabel = "Updated source root";
          txn.updateElement(rootSubjectProps);

          const childSubjectProps =
            sourceDb!.elements.getElementProps<SubjectProps>(
              sourceChildSubjectId
            );
          childSubjectProps.userLabel = "Updated source child";
          txn.updateElement(childSubjectProps);
        });
        await sourceDb.pushChanges({
          accessToken,
          description: "update source root and child Subject",
          retainLocks: true,
        });

        const processChangesTargetEditTxn = createStartedEditTxn(targetDb);
        transformer = new IModelTransformer(
          { source: sourceDb, target: processChangesTargetEditTxn },
          {
            argsForProcessChanges: {},
            targetScopeElementId: remappedTargetRootSubjectId,
            skipPropagateChangesToRootElements,
          }
        );
        transformer.context.remapElement(
          IModel.rootSubjectId,
          remappedTargetRootSubjectId
        );
        await withTransformerLifecycle(transformer, [
          processChangesTargetEditTxn,
        ]);

        const targetRootAfterChanges = targetDb.elements.getElement<Subject>(
          remappedTargetRootSubjectId,
          Subject
        );
        expect(targetRootAfterChanges.userLabel).to.equal(
          skipPropagateChangesToRootElements
            ? targetRootLabelBeforeChanges
            : "Updated source root"
        );
        expect(
          targetDb.elements.getElement<Subject>(targetChildSubjectId, Subject)
            .userLabel
        ).to.equal("Updated source child");
        if (skipPropagateChangesToRootElements) {
          const targetRootElementAspectCountAfterChanges =
            countElementExternalSourceAspects(
              targetDb,
              remappedTargetRootSubjectId
            );
          expect(targetRootElementAspectCountAfterChanges).to.equal(
            targetRootElementAspectCountBeforeChanges
          );
        }
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
  }
});
