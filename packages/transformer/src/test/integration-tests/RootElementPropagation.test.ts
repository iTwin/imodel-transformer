/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "vitest";
import {
  BriefcaseDb,
  ExternalSourceAspect,
  IModelDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { AccessToken, GuidString, Id64String } from "@itwin/core-bentley";
import { IModel, SubjectProps } from "@itwin/core-common";
import { IModelTransformer } from "../../imodel-transformer.js";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import { runTimeline, Timeline } from "../TestUtils/TimelineTestUtil.js";
import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

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

/** Propagating changes to the root Subject and other root elements (skipPropagateChangesToRootElements). */
describe("Root element propagation", () => {
  const hub = useTransformerHub("RootElementPropagation");
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
  });

  for (const propagateRootElems of [true, false]) {
    it(`${
      propagateRootElems ? "should" : "shouldn't"
    } propagate changes to rootSubject, repositoryModel, realityDataSourcesModel when skipPropagateChangesToRootElements is set to ${!propagateRootElems}`, async () => {
      const timeline: Timeline = [
        { master: { 1: 1 } },
        { branch: { branch: "master" } },
        { branch: { 1: 2, 4: 1 } },
        {
          branch: {
            manualUpdate(branch) {
              withEditTxn(
                branch,
                "update root model and element props",
                (txn) => {
                  // Update models
                  const dictionaryId = IModelDb.dictionaryId;
                  const dict = branch.models.getModelProps(dictionaryId);
                  txn.updateModel({
                    ...dict,
                    jsonProperties: { test: 1 },
                  });

                  const repositoryModel = branch.models.getModelProps(
                    IModelDb.repositoryModelId
                  );
                  txn.updateModel({
                    ...repositoryModel,
                    jsonProperties: { test: 2 },
                  });

                  const realityDataSourcesModel =
                    branch.models.getModelProps("0xe");
                  txn.updateModel({
                    ...realityDataSourcesModel,
                    jsonProperties: { test: 3 },
                  });

                  // Update Elements now.
                  const rootSubjectFromBranch =
                    branch.elements.getElementProps<SubjectProps>("0x1");
                  txn.updateElement({
                    ...rootSubjectFromBranch,
                    description: "test description",
                    jsonProperties: { test: 4 },
                  });

                  const realityDataSourcesElement =
                    branch.elements.getElementProps("0xe");
                  txn.updateElement({
                    ...realityDataSourcesElement,
                    jsonProperties: { test: 5 },
                  });

                  const dictionaryElement = branch.elements.getElementProps(
                    IModelDb.dictionaryId
                  );
                  txn.updateElement({
                    ...dictionaryElement,
                    jsonProperties: { test: 6 },
                  });
                }
              );
            },
          },
        },
        { master: { sync: ["branch"] } },
        {
          assert({ master, branch }) {
            const dictionaryModelMaster = master.db.models.getModel(
              IModelDb.dictionaryId
            );
            const dictionaryModelBranch = branch.db.models.getModel(
              IModelDb.dictionaryId
            );
            expect(dictionaryModelMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 1 : undefined
            );
            expect(dictionaryModelBranch.jsonProperties.test).to.equal(1);

            const repositoryModelMaster = master.db.models.getModel(
              IModelDb.repositoryModelId
            );
            const repositoryModelBranch = branch.db.models.getModel(
              IModelDb.repositoryModelId
            );
            expect(repositoryModelMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 2 : undefined
            );
            expect(repositoryModelBranch.jsonProperties.test).to.equal(2);

            const realityDataSourcesModelMaster =
              master.db.models.getModel("0xe");
            const realityDataSourcesModelBranch =
              branch.db.models.getModel("0xe");
            expect(realityDataSourcesModelMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 3 : undefined
            );
            expect(realityDataSourcesModelBranch.jsonProperties.test).to.equal(
              3
            );

            const rootSubjectMaster = master.db.elements.getRootSubject();
            const rootSubjectBranch = branch.db.elements.getRootSubject();
            expect(rootSubjectMaster.description).to.equal(
              propagateRootElems ? "test description" : ""
            );
            expect(rootSubjectBranch.description).to.equal("test description");
            expect(rootSubjectMaster.jsonProperties.test).to.equal(
              propagateRootElems ? 4 : undefined
            );
            expect(rootSubjectBranch.jsonProperties.test).to.equal(4);

            const realityDataSourcesElementMaster =
              master.db.elements.getElementProps("0xe");
            const realityDataSourcesElementBranch =
              branch.db.elements.getElementProps("0xe");
            expect(
              realityDataSourcesElementMaster.jsonProperties?.test
            ).to.equal(propagateRootElems ? 5 : undefined);
            expect(
              realityDataSourcesElementBranch.jsonProperties.test
            ).to.equal(5);

            const dictionaryElementMaster = master.db.elements.getElementProps(
              IModelDb.dictionaryId
            );
            const dictionaryElementBranch = branch.db.elements.getElementProps(
              IModelDb.dictionaryId
            );
            expect(dictionaryElementMaster.jsonProperties?.test).to.equal(
              propagateRootElems ? 6 : undefined
            );
            expect(dictionaryElementBranch.jsonProperties.test).to.equal(6);
          },
        },
      ];

      const { tearDown } = await runTimeline(timeline, {
        iTwinId,
        accessToken,
        transformerOpts: {
          skipPropagateChangesToRootElements: !propagateRootElems,
        },
      });

      await tearDown();
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
        await transformer.process();
        await transformer.updateSynchronizationVersion({
          initializeReverseSyncVersion: true,
        });
        const targetChildSubjectId =
          transformer.context.findTargetElementId(sourceChildSubjectId);
        transformer.dispose();
        initialTargetEditTxn.end();
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
        await transformer.process();
        transformer.dispose();
        processChangesTargetEditTxn.end();

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
