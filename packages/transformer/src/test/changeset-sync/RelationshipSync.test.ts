/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, assert, beforeEach, expect } from "vitest";
import * as path from "node:path";
import {
  BriefcaseDb,
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
  Guid,
  GuidString,
  Id64,
  Id64String,
} from "@itwin/core-bentley";
import {
  Code,
  GeometricElementProps,
  IModel,
  IModelError,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import {
  IModelTransformer,
  IModelTransformOptions,
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
  TimelineIModelElemState,
  TimelineIModelState,
} from "../TestUtils/TimelineTestUtil.js";
import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

const { count } = IModelTestUtils;

/** Relationship inserts, replacements, and deletes across provenance modes. */
describe("Relationship sync", () => {
  const hub = useTransformerHub("RelationshipSync");
  const {
    outputDir,
    prepareBriefcase,
    closeAndDeleteBriefcase,
    saveAndPushChanges,
  } = hub;
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  beforeAll(() => {
    ({ iTwinId, accessToken } = hub);
  });

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
    const masterSeedState = {
      40: 1,
      2: 2,
      41: 3,
      42: 4,
    } as TimelineIModelElemState;
    const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
      rootSubject: { name: masterIModelName },
    });
    // masterSeedDb.nativeDb.setITwinId(iTwinId); // workaround for "ContextId was not properly setup in the checkpoint" issue
    populateTimelineSeed(masterSeedDb, masterSeedState);

    const noFedGuidElemIds = masterSeedDb.queryEntityIds({
      from: "Bis.Element",
      where: "UserLabel IN ('41', '42')",
    });
    for (const elemId of noFedGuidElemIds)
      masterSeedDb.withSqliteStatement(
        `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
        (s) => {
          expect(s.step()).to.equal(DbResult.BE_SQLITE_DONE);
        }
      );
    masterSeedDb.performCheckpoint();

    // hard to check this without closing the db...
    const seedSecondConn = SnapshotDb.openFile(masterSeedDb.pathName);
    for (const elemId of noFedGuidElemIds)
      expect(seedSecondConn.elements.getElement(elemId).federationGuid).to.be
        .undefined;
    seedSecondConn.close();

    const masterSeed: TimelineIModelState = {
      // HACK: we know this will only be used for seeding via its path and performCheckpoint
      db: masterSeedDb as any as BriefcaseDb,
      id: "master-seed",
      state: masterSeedState,
    };

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
    const timeline: Timeline = [
      { master: { seed: masterSeed } },
      { branch: { branch: "master" } },
      {
        branch: {
          manualUpdate(db) {
            withEditTxn(db, "insert branch relationships", (txn) => {
              expectedRelationships.map(({ sourceLabel, targetLabel }, i) => {
                const sourceId = IModelTestUtils.queryByUserLabel(
                  db,
                  sourceLabel
                );
                const targetId = IModelTestUtils.queryByUserLabel(
                  db,
                  targetLabel
                );
                assert(sourceId && targetId);
                const rel = ElementGroupsMembers.create(
                  db,
                  sourceId,
                  targetId,
                  0
                );
                expectedRelationships[i].idInBranch = txn.insertRelationship(
                  rel.toJSON()
                );
              });
            });
          },
        },
      },
      { master: { sync: ["branch"] } }, // first master<-branch reverse sync
      {
        assert({ branch }) {
          // expectedRelationships[1] has no fedguids, so expect to find 2 esas. One for the relationship and one for the element's own provenance.
          const sourceId = IModelTestUtils.queryByUserLabel(
            branch.db,
            expectedRelationships[1].sourceLabel
          );
          const aspects = branch.db.elements.getAspects(
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
        },
      },
      {
        master: {
          manualUpdate(db) {
            withEditTxn(db, "delete relationships and elements", (txn) => {
              expectedRelationships.forEach(({ sourceLabel, targetLabel }) => {
                const sourceId = IModelTestUtils.queryByUserLabel(
                  db,
                  sourceLabel
                );
                const targetId = IModelTestUtils.queryByUserLabel(
                  db,
                  targetLabel
                );
                assert(sourceId && targetId);
                const rel = db.relationships.getInstance(
                  ElementGroupsMembers.classFullName,
                  { sourceId, targetId }
                );
                txn.deleteRelationship(rel.toJSON());
                txn.deleteElement(sourceId);
                txn.deleteElement(targetId);
              });
            });
          },
        },
      },
      { branch: { sync: ["master"] } }, // master->branch forward sync
      {
        assert({ branch }) {
          for (const rel of expectedRelationships) {
            expect(
              branch.db.relationships.tryGetInstance(
                ElementGroupsMembers.classFullName,
                rel.idInBranch
              ),
              `had ${rel.sourceLabel}->${rel.targetLabel}`
            ).to.be.undefined;
            const sourceId = IModelTestUtils.queryByUserLabel(
              branch.db,
              rel.sourceLabel
            );
            const targetId = IModelTestUtils.queryByUserLabel(
              branch.db,
              rel.targetLabel
            );
            // Since we deleted both elements in the previous manualUpdate
            assert(
              Id64.isInvalid(sourceId) && Id64.isInvalid(targetId),
              `SourceId is ${sourceId}, TargetId is ${targetId}. Expected both to be ${Id64.invalid}.`
            );
            expect(
              () =>
                branch.db.relationships.tryGetInstance(
                  ElementGroupsMembers.classFullName,
                  { sourceId, targetId }
                ),
              `had ${rel.sourceLabel}->${rel.targetLabel}`
            ).to.throw; // TODO: This shouldn't throw but it does in core due to failing to bind ids of 0.

            expect(() =>
              branch.db.elements.getAspect(aspectIdForRelationship!)
            ).to.throw(
              "not found",
              `Expected aspectId: ${aspectIdForRelationship} to no longer be present in branch imodel.`
            );
          }
        },
      },
    ];
    const { tearDown } = await runTimeline(timeline, {
      iTwinId,
      accessToken,
    });

    await tearDown();
    masterSeedDb.close();
  });

  for (const forceExternalSourceAspectProvenance of [true, false]) {
    it(`should keep the master relationship when a branch relationship is replaced over the same endpoints during reverse sync (forceExternalSourceAspectProvenance=${forceExternalSourceAspectProvenance})`, async () => {
      const masterSeedFileName = path.join(
        outputDir,
        `MasterReplacedRelationship${forceExternalSourceAspectProvenance}.bim`
      );
      if (IModelJsFs.existsSync(masterSeedFileName))
        IModelJsFs.removeSync(masterSeedFileName);
      const masterSeedState = { 1: 1, 2: 1 };
      const masterSeedDb = SnapshotDb.createEmpty(masterSeedFileName, {
        rootSubject: { name: "MasterReplacedRelationship" },
      });
      populateTimelineSeed(masterSeedDb, masterSeedState);
      const masterSeed: TimelineIModelState = {
        // HACK: we know this will only be used for seeding via its path and performCheckpoint
        db: masterSeedDb as any as BriefcaseDb,
        id: "master-seed",
        state: masterSeedState,
      };
      const queryMember = (db: IModelDb) => {
        const sourceId = IModelTestUtils.queryByUserLabel(db, "1");
        const targetId = IModelTestUtils.queryByUserLabel(db, "2");
        return {
          sourceId,
          targetId,
          rel: db.relationships.tryGetInstance<ElementGroupsMembers>(
            ElementGroupsMembers.classFullName,
            { sourceId, targetId }
          ),
        };
      };
      let masterRelId: Id64String | undefined;

      const timeline: Timeline = [
        { master: { seed: masterSeed } },
        {
          master: {
            manualUpdate(db) {
              const { sourceId, targetId } = queryMember(db);
              masterRelId = withEditTxn(db, "insert relationship", (txn) =>
                txn.insertRelationship(
                  ElementGroupsMembers.create(
                    db,
                    sourceId,
                    targetId,
                    0
                  ).toJSON()
                )
              );
            },
          },
        },
        { branch1: { branch: "master" } },
        {
          branch1: {
            manualUpdate(db) {
              const { sourceId, targetId, rel } = queryMember(db);
              assert(rel !== undefined);
              withEditTxn(db, "replace relationship", (txn) => {
                txn.deleteRelationship(rel.toJSON());
                txn.insertRelationship(
                  ElementGroupsMembers.create(
                    db,
                    sourceId,
                    targetId,
                    1
                  ).toJSON()
                );
              });
            },
          },
        },
        { master: { sync: ["branch1"] } },
        {
          assert({ master }) {
            expect(
              count(master.db, ElementGroupsMembers.classFullName)
            ).to.equal(1);
            const { rel } = queryMember(master.db);
            expect(rel?.id).to.equal(masterRelId);
            expect(rel?.memberPriority).to.equal(1);
          },
        },
      ];

      const { tearDown } = await runTimeline(timeline, {
        iTwinId,
        accessToken,
        transformerOpts: { forceExternalSourceAspectProvenance },
      });
      await tearDown();
    });
  }

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

    for (const forceExternalSourceAspectProvenance of [true, false]) {
      for (const withFederationGuids of [true, false]) {
        for (const sourceChange of [
          "replaced in one changeset",
          "replaced across changesets",
          "replaced by a different relationship class",
          "deleted",
        ] as const) {
          const replacedByOtherClass =
            sourceChange === "replaced by a different relationship class";
          // without federation guids, deletes match the target relationship by provenance id rather than by endpoints
          if (replacedByOtherClass && !withFederationGuids) continue;
          it(`should sync a source relationship that was ${sourceChange} (forceExternalSourceAspectProvenance=${forceExternalSourceAspectProvenance}, federationGuids=${withFederationGuids})`, async () => {
            const otherRelClassFullName =
              "TestOtherRelationships:OtherRefersToElements";
            if (replacedByOtherClass) {
              // a sibling of ElementGroupsMembers, so looking up ElementGroupsMembers by endpoints never finds it
              const otherRelSchema = `<?xml version="1.0" encoding="UTF-8"?>
                <ECSchema schemaName="TestOtherRelationships" alias="tor" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
                  <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
                  <ECRelationshipClass typeName="OtherRefersToElements" strength="referencing" modifier="None">
                    <BaseClass>bis:ElementRefersToElements</BaseClass>
                    <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true">
                      <Class class="bis:Element"/>
                    </Source>
                    <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true">
                      <Class class="bis:Element"/>
                    </Target>
                  </ECRelationshipClass>
                </ECSchema>`;
              for (const db of [sourceDb, targetDb]) {
                await db.importSchemaStrings([otherRelSchema]);
                await db.pushChanges({
                  description: "import other relationship schema",
                  retainLocks: true,
                });
              }
            }
            const [groupId, memberId] = withEditTxn(
              sourceDb,
              "insert elements",
              (txn) => {
                const modelId = PhysicalModel.insert(
                  txn,
                  IModel.rootSubjectId,
                  "M"
                );
                const categoryId = SpatialCategory.insert(
                  txn,
                  IModel.dictionaryId,
                  "C",
                  {}
                );
                return ["Group", "Member"].map((label) =>
                  txn.insertElement({
                    classFullName: PhysicalObject.classFullName,
                    model: modelId,
                    category: categoryId,
                    code: Code.createEmpty(),
                    userLabel: label,
                    ...(withFederationGuids
                      ? {}
                      : { federationGuid: Guid.empty }),
                  } as GeometricElementProps)
                );
              }
            );
            const insertMember = (memberPriority: number) =>
              withEditTxn(sourceDb, "insert relationship", (txn) =>
                txn.insertRelationship(
                  ElementGroupsMembers.create(
                    sourceDb,
                    groupId,
                    memberId,
                    memberPriority
                  ).toJSON()
                )
              );
            const deleteMember = (relId: Id64String) =>
              withEditTxn(sourceDb, "delete relationship", (txn) =>
                txn.deleteRelationship(
                  sourceDb.relationships
                    .getInstance(ElementGroupsMembers.classFullName, relId)
                    .toJSON()
                )
              );
            const transform = async (
              options: IModelTransformOptions,
              description: string
            ) => {
              const editTxn = createStartedEditTxn(targetDb);
              const transformer = new IModelTransformer(
                { source: sourceDb, target: editTxn },
                { forceExternalSourceAspectProvenance, ...options }
              );
              await transformer.process();
              transformer.dispose();
              editTxn.end();
              await targetDb.pushChanges({ description, retainLocks: true });
            };
            const targetMembers = () => [
              ...targetDb.queryEntityIds({
                from: ElementGroupsMembers.classFullName,
              }),
            ];
            const usesRelationshipAspects =
              forceExternalSourceAspectProvenance || !withFederationGuids;
            const targetRelationshipAspects = () =>
              targetDb.elements
                .getAspects(
                  IModelTestUtils.queryByUserLabel(targetDb, "Group"),
                  ExternalSourceAspect.classFullName
                )
                .filter(
                  (aspect) =>
                    (aspect as ExternalSourceAspect).kind ===
                    ExternalSourceAspect.Kind.Relationship
                );

            const originalRelId = insertMember(0);
            await sourceDb.pushChanges({
              description: "initial source data",
              retainLocks: true,
            });
            await transform({}, "initial transform");
            const [originalTargetRelId] = targetMembers();
            assert(originalTargetRelId !== undefined);

            deleteMember(originalRelId);
            if (replacedByOtherClass) {
              withEditTxn(sourceDb, "insert other relationship", (txn) =>
                txn.insertRelationship({
                  classFullName: otherRelClassFullName,
                  sourceId: groupId,
                  targetId: memberId,
                })
              );
              await sourceDb.pushChanges({
                description: `relationship ${sourceChange}`,
                retainLocks: true,
              });
              await transform({ argsForProcessChanges: {} }, "process changes");

              expect(
                targetMembers(),
                "the deleted relationship has a different class than the inserted one, so it should still be deleted"
              ).to.be.empty;
              expect([
                ...targetDb.queryEntityIds({ from: otherRelClassFullName }),
              ]).to.have.lengthOf(1);
              expect(
                targetRelationshipAspects(),
                "only the inserted relationship should have provenance"
              ).to.have.lengthOf(usesRelationshipAspects ? 1 : 0);
              return;
            }
            if (sourceChange === "replaced across changesets")
              await sourceDb.pushChanges({
                description: "delete relationship",
                retainLocks: true,
              });
            const replacementRelId =
              sourceChange === "deleted" ? undefined : insertMember(1);
            await sourceDb.pushChanges({
              description: `relationship ${sourceChange}`,
              retainLocks: true,
            });
            await transform({ argsForProcessChanges: {} }, "process changes");

            if (replacementRelId === undefined) {
              expect(targetMembers()).to.be.empty;
              expect(targetRelationshipAspects()).to.be.empty;
              return;
            }
            expect(
              targetMembers(),
              "the relationship still exists in the source, so it should exist in the target"
            ).to.deep.equal([originalTargetRelId]);
            expect(
              targetDb.relationships.getInstance<ElementGroupsMembers>(
                ElementGroupsMembers.classFullName,
                originalTargetRelId
              ).memberPriority
            ).to.equal(1);
            expect(
              targetRelationshipAspects(),
              "the deleted relationship's provenance should be removed, leaving only the replacement's"
            ).to.have.lengthOf(usesRelationshipAspects ? 1 : 0);

            // the replacement relationship's provenance must still let a later delete reach the target
            deleteMember(replacementRelId);
            await sourceDb.pushChanges({
              description: "delete replacement relationship",
              retainLocks: true,
            });
            await transform(
              { argsForProcessChanges: {} },
              "process replacement delete"
            );
            expect(targetMembers()).to.be.empty;
            expect(targetRelationshipAspects()).to.be.empty;
          });
        }
      }
    }
  });
});
