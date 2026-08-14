/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "vitest";
import * as path from "node:path";

import {
  BriefcaseDb,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  ExternalSourceAspect,
  IModelDb,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";

import {
  AccessToken,
  DbResult,
  GuidString,
  Id64,
  Id64String,
} from "@itwin/core-bentley";
import {
  Code,
  IModel,
  IModelError,
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

describe("IModelTransformerHub - relationship provenance", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let saveAndPushChanges: (db: BriefcaseDb, desc: string) => Promise<void>;

  const outputDir = registerHubTestContext(
    "IModelTransformerHubRelationship",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
      saveAndPushChanges = context.saveAndPushChanges;
    }
  );

  interface RelationshipDeleteCase {
    name: string;
    masterIModelName: string;
    federationGuidMode: "preserved" | "null";
    provenanceMode: "none" | "new" | "old";
  }
  const relationshipDeleteCases: readonly RelationshipDeleteCase[] = [
    {
      name: "should be able to handle relationship delete using fedguids",
      masterIModelName: "MasterNewRelProvenanceFedGuids",
      federationGuidMode: "preserved",
      provenanceMode: "none",
    },
    {
      name: "should be able to handle relationship delete using new relationship provenance method with no fedguids",
      masterIModelName: "MasterNewRelProvenanceNoFedGuids",
      federationGuidMode: "null",
      provenanceMode: "new",
    },
    {
      name: "should be able to handle relationship delete using old relationship provenance method with no fedguids",
      masterIModelName: "MasterOldRelProvenanceNoFedGuids",
      federationGuidMode: "null",
      provenanceMode: "old",
    },
  ];

  for (const testCase of relationshipDeleteCases) {
    it(testCase.name, async () => {
      // SEE: https://github.com/iTwin/imodel-transformer/issues/54 for the scenario this test exercises.
      // Each case syncs a relationship from a branch to its master, deletes it in the master, and syncs the deletion back.
      const masterSeedFileName = path.join(
        outputDir,
        `${testCase.masterIModelName}.bim`
      );
      if (IModelJsFs.existsSync(masterSeedFileName))
        IModelJsFs.removeSync(masterSeedFileName);

      let masterSeedDb: SnapshotDb | undefined;
      let masterIModelId: GuidString | undefined;
      let branchIModelId: GuidString | undefined;
      let masterDb: BriefcaseDb | undefined;
      let branchDb: BriefcaseDb | undefined;
      let relIdInBranch: Id64String | undefined;

      try {
        const seedDb = SnapshotDb.createEmpty(masterSeedFileName, {
          rootSubject: { name: testCase.masterIModelName },
        });
        masterSeedDb = seedDb;
        const { modelId, categoryId } = withEditTxn(
          seedDb,
          "insert master seed model and category",
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
        withEditTxn(seedDb, "insert master seed elements", (txn) => {
          for (const name of ["1", "2"]) {
            const elementProps: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category: categoryId,
              code: new Code({
                spec: IModelDb.rootSubjectId,
                scope: IModelDb.rootSubjectId,
                value: name,
              }),
              userLabel: name,
              geom: IModelTransformerTestUtils.createBox(
                Point3d.create(1, 1, 1)
              ),
              placement: {
                origin: Point3d.create(0, 0, 0),
                angles: YawPitchRollAngles.createDegrees(0, 0, 0),
              },
              jsonProperties: { updateState: 1 },
            };
            txn.insertElement(elementProps);
          }
        });

        if (testCase.federationGuidMode === "null") {
          const noFedGuidElemIds = seedDb.queryEntityIds({
            from: "Bis.Element",
            where: "UserLabel IN ('1','2')",
          });
          withEditTxn(seedDb, "null out fedguids", () => {
            for (const elemId of noFedGuidElemIds)
              seedDb.withSqliteStatement(
                `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
                (s) => {
                  expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
                }
              );
          });
        }
        seedDb.performCheckpoint();

        masterIModelId = await HubWrappers.recreateIModel({
          accessToken,
          iTwinId,
          iModelName: testCase.masterIModelName,
          noLocks: true,
          version0: masterSeedFileName,
        });
        masterDb = await HubWrappers.downloadAndOpenBriefcase({
          accessToken,
          iTwinId,
          iModelId: masterIModelId,
        });
        await saveAndPushChanges(
          masterDb,
          "seeded from 'master-seed' at point 0"
        );

        branchIModelId = await HubWrappers.recreateIModel({
          accessToken,
          iTwinId,
          iModelName: `${testCase.masterIModelName}-branch1`,
          noLocks: true,
          version0: masterDb.pathName,
        });
        branchDb = await HubWrappers.downloadAndOpenBriefcase({
          accessToken,
          iTwinId,
          iModelId: branchIModelId,
        });

        const branchProvenanceEditTxn = createStartedEditTxn(branchDb);
        const branchProvenanceTransformer = new IModelTransformer(
          { source: masterDb, target: branchProvenanceEditTxn },
          { wasSourceIModelCopiedToTarget: true }
        );
        let branchProvenanceSucceeded = false;
        try {
          await branchProvenanceTransformer.process();
          branchProvenanceSucceeded = true;
        } finally {
          branchProvenanceTransformer.dispose();
          branchProvenanceEditTxn.end(
            branchProvenanceSucceeded ? "save" : "abandon"
          );
        }
        await branchDb.pushChanges({
          accessToken,
          description: "initialized branch provenance",
        });

        withEditTxn(branchDb, "insert branch relationship", (txn) => {
          const sourceId = IModelTestUtils.queryByUserLabel(branchDb!, "1");
          const targetId = IModelTestUtils.queryByUserLabel(branchDb!, "2");
          const rel = ElementGroupsMembers.create(
            branchDb!,
            sourceId,
            targetId
          );
          relIdInBranch = txn.insertRelationship(rel.toJSON());
        });
        await branchDb.pushChanges({
          accessToken,
          description: "insert branch relationship",
        });

        const reverseSyncEditTxn = createStartedEditTxn(masterDb);
        const reverseSyncSourceEditTxn = createStartedEditTxn(branchDb);
        const reverseSyncer = new IModelTransformer(
          { source: branchDb, target: reverseSyncEditTxn },
          {
            sourceEditTxn: reverseSyncSourceEditTxn,
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
          }
        );
        let reverseSyncSucceeded = false;
        try {
          if (testCase.provenanceMode === "old")
            reverseSyncer["_forceOldRelationshipProvenanceMethod"] = true;
          await reverseSyncer.process();
          reverseSyncSucceeded = true;
        } finally {
          reverseSyncer.dispose();
          reverseSyncEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
          reverseSyncSourceEditTxn.end(
            reverseSyncSucceeded ? "save" : "abandon"
          );
        }
        await branchDb.pushChanges({
          accessToken,
          description: "reverse sync relationship",
        });
        await masterDb.pushChanges({
          accessToken,
          description: "reverse sync relationship",
        });

        const aspects = branchDb.elements.getAspects(
          IModelTestUtils.queryByUserLabel(branchDb, "1"),
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[];
        if (testCase.provenanceMode === "none") {
          expect(aspects.length).to.be.equal(0);
        } else if (testCase.provenanceMode === "new") {
          expect(aspects.length).to.be.equal(2);
          for (const aspect of aspects) {
            if (aspect.kind === "Relationship") {
              // When forceOldRelationshipProvenanceMethod is not set to true, provenanceRelInstanceId is defined on jsonProperties.
              expect(aspect.jsonProperties).to.not.be.undefined;
              expect(JSON.parse(aspect.jsonProperties!).provenanceRelInstanceId)
                .to.not.be.undefined;
            }
          }
        } else {
          // Lets make sure that forceOldRelationshipProvenance worked by reading the json properties of the ESA for the relationship.
          expect(aspects.length).to.be.equal(2);
          let foundRelationshipAspect = false;
          for (const aspect of aspects) {
            if (aspect.kind === "Relationship") {
              foundRelationshipAspect = true;
              // When forceOldRelationshipProvenanceMethod is true, targetRelInstanceId is defined on jsonProperties.
              expect(aspect.jsonProperties).to.not.be.undefined;
              expect(JSON.parse(aspect.jsonProperties!).targetRelInstanceId).to
                .not.be.undefined;
            }
          }
          expect(foundRelationshipAspect).to.be.true;
        }

        withEditTxn(masterDb, "delete master relationship", (txn) => {
          const rel = masterDb!.relationships.getInstance<ElementGroupsMembers>(
            ElementGroupsMembers.classFullName,
            {
              sourceId: IModelTestUtils.queryByUserLabel(masterDb!, "1"),
              targetId: IModelTestUtils.queryByUserLabel(masterDb!, "2"),
            }
          );
          txn.deleteRelationship(rel.toJSON());
        });
        await masterDb.pushChanges({
          accessToken,
          description: "delete master relationship",
        });

        const forwardSyncEditTxn = createStartedEditTxn(branchDb);
        const forwardSyncer = new IModelTransformer(
          {
            source: masterDb,
            target: forwardSyncEditTxn,
          },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
          }
        );
        let forwardSyncSucceeded = false;
        try {
          if (testCase.provenanceMode === "old")
            forwardSyncer["_forceOldRelationshipProvenanceMethod"] = true;
          await forwardSyncer.process();
          forwardSyncSucceeded = true;
        } finally {
          forwardSyncer.dispose();
          forwardSyncEditTxn.end(forwardSyncSucceeded ? "save" : "abandon");
        }
        await branchDb.pushChanges({
          accessToken,
          description: "forward sync relationship deletion",
        });

        expect(relIdInBranch, "expected relationship id in branch to be set").to
          .not.be.undefined;
        expect(() =>
          branchDb!.relationships.getInstance<ElementGroupsMembers>(
            ElementGroupsMembers.classFullName,
            relIdInBranch!
          )
        ).to.throw(IModelError);
      } finally {
        const cleanup = async (
          description: string,
          action: () => void | Promise<void>
        ) => {
          try {
            await action();
          } catch (error) {
            // eslint-disable-next-line no-console
            console.error(`Failed to clean up ${description}`, error);
          }
        };

        if (masterDb)
          await cleanup("master briefcase", async () => {
            await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb!);
          });
        if (branchDb)
          await cleanup("branch briefcase", async () => {
            await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb!);
          });
        if (masterIModelId)
          await cleanup("master iModel", async () => {
            await transformerTestHub.deleteIModel({
              iTwinId,
              iModelId: masterIModelId!,
            });
          });
        if (branchIModelId)
          await cleanup("branch iModel", async () => {
            await transformerTestHub.deleteIModel({
              iTwinId,
              iModelId: branchIModelId!,
            });
          });
        if (masterSeedDb)
          await cleanup("master seed", () => masterSeedDb!.close());
      }
    });
  }

  it("should be able to handle a transformation which deletes a relationship and then elements of that relationship", async () => {
    const masterIModelName = "MasterDeleteRelAndEnds";
    const masterSeedFileName = path.join(outputDir, `${masterIModelName}.bim`);
    if (IModelJsFs.existsSync(masterSeedFileName))
      IModelJsFs.removeSync(masterSeedFileName);
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    const { modelId, categoryId } = withEditTxn(
      masterSeedDb,
      "populate master seed",
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
    withEditTxn(masterSeedDb, "maintain master objects", (txn) => {
      for (const [name, updateState] of Object.entries({
        40: 1,
        2: 2,
        41: 3,
        42: 4,
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
    const noFedGuidElemIds = masterSeedDb.queryEntityIds({
      from: "Bis.Element",
      where: "UserLabel IN ('41', '42')",
    });
    withEditTxn(masterSeedDb, "null out fedguids", () => {
      for (const elemId of noFedGuidElemIds)
        masterSeedDb.withSqliteStatement(
          `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
          (s) => {
            expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
          }
        );
    });
    masterSeedDb.performCheckpoint();

    const seedSecondConn = SnapshotDb.openFile(masterSeedDb.pathName);
    for (const elemId of noFedGuidElemIds)
      expect(seedSecondConn.elements.getElement(elemId).federationGuid).to.be
        .undefined;
    seedSecondConn.close();

    const expectedRelationships = [
      {
        sourceLabel: "40",
        targetLabel: "2",
        idInBranch: "not inserted yet",
        sourceFedGuid: true,
        targetFedGuid: true,
      },
      {
        sourceLabel: "41",
        targetLabel: "42",
        idInBranch: "not inserted yet",
        sourceFedGuid: false,
        targetFedGuid: false,
      },
    ];
    let aspectIdForRelationship: Id64String | undefined;
    let masterIModelId: GuidString | undefined;
    let branchIModelId: GuidString | undefined;
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;

    try {
      masterIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: masterIModelName,
        noLocks: true,
        version0: masterSeedFileName,
      });
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      await saveAndPushChanges(masterDb, "seeded master");
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: "BranchDeleteRelAndEnds",
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

      withEditTxn(branchDb, "insert branch relationships", (txn) => {
        expectedRelationships.forEach(({ sourceLabel, targetLabel }, i) => {
          const relationshipSourceId = IModelTestUtils.queryByUserLabel(
            branchDb!,
            sourceLabel
          );
          const targetId = IModelTestUtils.queryByUserLabel(
            branchDb!,
            targetLabel
          );
          assert(relationshipSourceId && targetId);
          const rel = ElementGroupsMembers.create(
            branchDb!,
            relationshipSourceId,
            targetId,
            0
          );
          expectedRelationships[i].idInBranch = txn.insertRelationship(
            rel.toJSON()
          );
        });
      });
      await branchDb.pushChanges({
        accessToken,
        description: "insert branch relationships",
      });

      const reverseSyncEditTxn = createStartedEditTxn(masterDb);
      const reverseSyncSourceEditTxn = createStartedEditTxn(branchDb);
      const reverseSyncer = new IModelTransformer(
        { source: branchDb, target: reverseSyncEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: reverseSyncSourceEditTxn,
        }
      );
      let reverseSyncSucceeded = false;
      try {
        await reverseSyncer.process();
        reverseSyncSucceeded = true;
      } finally {
        reverseSyncer.dispose();
        reverseSyncEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
        reverseSyncSourceEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
      }
      await branchDb.pushChanges({
        accessToken,
        description: "reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "reverse sync",
      });

      const sourceId = IModelTestUtils.queryByUserLabel(
        branchDb,
        expectedRelationships[1].sourceLabel
      );
      const aspects = branchDb.elements.getAspects(
        sourceId,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      assert(aspects.length === 2);
      let foundElementEsa = false;
      for (const aspect of aspects) {
        if (aspect.kind === "Element") foundElementEsa = true;
        else if (aspect.kind === "Relationship")
          aspectIdForRelationship = aspect.id;
      }
      assert(
        aspectIdForRelationship &&
          Id64.isValid(aspectIdForRelationship) &&
          foundElementEsa
      );

      withEditTxn(masterDb, "delete relationships and elements", (txn) => {
        expectedRelationships.forEach(({ sourceLabel, targetLabel }) => {
          const sourceElementId = IModelTestUtils.queryByUserLabel(
            masterDb!,
            sourceLabel
          );
          const targetElementId = IModelTestUtils.queryByUserLabel(
            masterDb!,
            targetLabel
          );
          assert(sourceElementId && targetElementId);
          const rel = masterDb!.relationships.getInstance(
            ElementGroupsMembers.classFullName,
            { sourceId: sourceElementId, targetId: targetElementId }
          );
          txn.deleteRelationship(rel.toJSON());
          txn.deleteElement(sourceElementId);
          txn.deleteElement(targetElementId);
        });
      });
      await masterDb.pushChanges({
        accessToken,
        description: "delete relationships and elements",
      });

      const forwardSyncEditTxn = createStartedEditTxn(branchDb);
      const forwardSyncer = new IModelTransformer(
        { source: masterDb, target: forwardSyncEditTxn },
        { argsForProcessChanges: { startChangeset: { index: undefined } } }
      );
      let forwardSyncSucceeded = false;
      try {
        await forwardSyncer.process();
        forwardSyncSucceeded = true;
      } finally {
        forwardSyncer.dispose();
        forwardSyncEditTxn.end(forwardSyncSucceeded ? "save" : "abandon");
      }
      await branchDb.pushChanges({
        accessToken,
        description: "forward sync deleted relationships and elements",
      });

      for (const rel of expectedRelationships) {
        expect(
          branchDb.relationships.tryGetInstance(
            ElementGroupsMembers.classFullName,
            rel.idInBranch
          ),
          `had ${rel.sourceLabel}->${rel.targetLabel}`
        ).to.be.undefined;
        const branchSourceId = IModelTestUtils.queryByUserLabel(
          branchDb,
          rel.sourceLabel
        );
        const branchTargetId = IModelTestUtils.queryByUserLabel(
          branchDb,
          rel.targetLabel
        );
        assert(
          Id64.isInvalid(branchSourceId) && Id64.isInvalid(branchTargetId),
          `SourceId is ${branchSourceId}, TargetId is ${branchTargetId}. Expected both to be ${Id64.invalid}.`
        );
        expect(
          () =>
            branchDb!.relationships.tryGetInstance(
              ElementGroupsMembers.classFullName,
              { sourceId: branchSourceId, targetId: branchTargetId }
            ),
          `had ${rel.sourceLabel}->${rel.targetLabel}`
        ).to.throw; // TODO: This shouldn't throw but it does in core due to failing to bind ids of 0.

        expect(() =>
          branchDb!.elements.getAspect(aspectIdForRelationship!)
        ).to.throw(
          "not found",
          `Expected aspectId: ${aspectIdForRelationship} to no longer be present in branch imodel.`
        );
      }
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branchDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
      if (masterIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: masterIModelId,
        });
      if (branchIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
      masterSeedDb.close();
    }
  });
});
