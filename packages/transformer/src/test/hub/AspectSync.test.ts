/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect } from "vitest";
import {
  BriefcaseDb,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { AccessToken, GuidString, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
  GeometricElementProps,
  IModel,
} from "@itwin/core-common";
import { IModelExporter, IModelTransformer } from "../../imodel-transformer.js";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

/** ElementAspect updates during change processing. */
describe("Aspect sync", () => {
  const hub = useTransformerHub("AspectSync");
  const {
    createPopulatedIModelHubIModel,
    prepareBriefcase,
    closeAndDeleteBriefcase,
    saveAndPushChanges,
  } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
  });

  it("should update aspects when processing changes", async () => {
    let elementIds: Id64String[] = [];
    const aspectIds: Id64String[] = [];
    const sourceIModelId = await createPopulatedIModelHubIModel(
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

    const targetIModelId =
      await createPopulatedIModelHubIModel("TransformerTarget");

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

    it("should still export updated aspects when the owning element is unchanged during processChanges", async () => {
      // Import a schema with a custom UniqueAspect so we can test aspect-only updates
      // without interference from the provenance system
      const testSchemaPath =
        IModelTransformerTestUtils.getPathToSchemaWithUniqueAspect();
      await sourceDb.importSchemas([testSchemaPath]);
      await targetDb.importSchemas([testSchemaPath]);
      await sourceDb.pushChanges({
        description: "Import test schema",
        retainLocks: true,
      });
      await targetDb.pushChanges({
        description: "Import test schema",
        retainLocks: true,
      });

      // Create an element with a unique aspect
      const elementId = withEditTxn(
        sourceDb,
        "create element with unique aspect",
        (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "TestPhysicalModelForAspect"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "TestCategoryForAspect",
            {}
          );
          const elemId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "ElementWithUniqueAspect",
          } as GeometricElementProps);
          txn.insertAspect({
            classFullName: "TestSchema1:MyUniqueAspect",
            element: { id: elemId },
            myProp1: "original-value",
          } as any);
          return elemId;
        }
      );
      await sourceDb.pushChanges({
        description: "Initial element with unique aspect",
        retainLocks: true,
      });

      // Run initial processAll transformation
      const firstEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstEditTxn,
      });
      await transformer.process();
      transformer.dispose();
      firstEditTxn.end();
      await targetDb.pushChanges({
        description: "Initial transformation",
        retainLocks: true,
      });

      // Verify initial aspect value on target
      const targetElementId = IModelTestUtils.queryByUserLabel(
        targetDb,
        "ElementWithUniqueAspect"
      );
      const targetAspectsBefore = targetDb.elements.getAspects(
        targetElementId,
        "TestSchema1:MyUniqueAspect"
      );
      expect(targetAspectsBefore).to.have.lengthOf(1);
      expect((targetAspectsBefore[0] as any).myProp1).to.equal(
        "original-value"
      );

      // Update only the aspect (not the element directly)
      withEditTxn(sourceDb, "update unique aspect only", (txn) => {
        const aspects = sourceDb.elements.getAspects(
          elementId,
          "TestSchema1:MyUniqueAspect"
        );
        txn.updateAspect({
          ...aspects[0].toJSON(),
          myProp1: "updated-value",
        } as any);
      });
      await sourceDb.pushChanges({
        description: "Aspect-only update",
        retainLocks: true,
      });

      // Run processChanges — the aspect change should propagate to the target
      const secondEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondEditTxn },
        { argsForProcessChanges: {} }
      );
      await transformer.process();
      transformer.dispose();
      secondEditTxn.end();

      // Verify: the aspect on the target element was updated
      const targetAspectsAfter = targetDb.elements.getAspects(
        targetElementId,
        "TestSchema1:MyUniqueAspect"
      );
      expect(targetAspectsAfter).to.have.lengthOf(1);
      expect(
        (targetAspectsAfter[0] as any).myProp1,
        "target aspect should have been updated to 'updated-value' by processChanges"
      ).to.equal("updated-value");
    });
  });
});
