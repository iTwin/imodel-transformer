/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";
import * as path from "node:path";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
  BriefcaseManager,
  DefinitionContainer,
  DefinitionModel,
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  ElementOwnsChildElements,
  ElementOwnsExternalSourceAspects,
  ElementOwnsMultiAspects,
  ExternalSourceAspect,
  IModelDb,
  IModelHost,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import * as TestUtils from "../TestUtils";
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
  ElementAspectProps,
  ExternalSourceAspectProps,
  IModel,
  IModelVersion,
  PhysicalElementProps,
  QueryBinder,
} from "@itwin/core-common";
import {
  ChangedInstanceIds,
  ChangeScanResult,
  IModelTransformer,
  IModelTransformOptions,
} from "../../imodel-transformer";
import { ChangesetScanner, DeletionBatch } from "../../ChangesetScanner";
import {
  CachedChangesTransformer,
  ChangeCache,
} from "./ChangeScanningExamples";
import { createStartedEditTxn, HubWrappers } from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { populateTimelineSeed } from "../TestUtils/TimelineTestUtil";

/** Hub tests for reading source changes: deletion batches, the scanChanges hook, and changeset reader filtering. */
describe("IModelTransformerHub change scanning", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "IModelTransformerHubChangeScanning"
  );
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  beforeAll(async () => {
    transformerTestHub.start(
      "IModelTransformerHubChangeScanning",
      KnownTestLocations.outputDir
    );
    iTwinId = transformerTestHub.iTwinId;
    IModelJsFs.recursiveMkDirSync(outputDir);
    accessToken = await HubWrappers.getAccessToken(
      TestUtils.TestUserType.Regular
    );
  });

  let restoreCheckpointDownload: (() => void) | undefined;
  beforeEach(() => {
    restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
  });

  afterEach(() => {
    restoreCheckpointDownload?.();
    restoreCheckpointDownload = undefined;
  });

  afterAll(() => transformerTestHub.stop());

  describe("deletion batches in reverse sync", () => {
    interface BranchFixture {
      masterDb: BriefcaseDb;
      branchDb: BriefcaseDb;
    }

    /**
     * Runs a test on a master whose elements "1" and "2" have no FederationGuid and are related by an
     * ElementGroupsMembers, and a branch of it that tracks them with ExternalSourceAspects.
     */
    async function withBranch(
      name: string,
      test: (fixture: BranchFixture) => Promise<void>
    ): Promise<void> {
      const seedFileName = path.join(outputDir, `${name}.bim`);
      if (IModelJsFs.existsSync(seedFileName))
        IModelJsFs.removeSync(seedFileName);
      let seedDb: SnapshotDb | undefined;
      let masterIModelId: GuidString | undefined;
      let branchIModelId: GuidString | undefined;
      let masterDb: BriefcaseDb | undefined;
      let branchDb: BriefcaseDb | undefined;
      try {
        const db = SnapshotDb.createEmpty(seedFileName, {
          rootSubject: { name },
        });
        seedDb = db;
        populateTimelineSeed(db, { 1: 1, 2: 1 });
        withEditTxn(db, "remove federation GUIDs and relate", (txn) => {
          for (const elemId of db.queryEntityIds({
            from: "Bis.Element",
            where: "UserLabel IN ('1','2')",
          }))
            db.withSqliteStatement(
              `UPDATE bis_Element SET FederationGuid=NULL WHERE Id=${elemId}`,
              (stmt) => {
                expect(stmt.step()).to.equal(DbResult.BE_SQLITE_DONE);
              }
            );
          txn.insertRelationship(
            ElementGroupsMembers.create(
              db,
              IModelTestUtils.queryByUserLabel(db, "1"),
              IModelTestUtils.queryByUserLabel(db, "2")
            ).toJSON()
          );
        });
        db.performCheckpoint();

        masterIModelId = await HubWrappers.recreateIModel({
          accessToken,
          iTwinId,
          iModelName: name,
          noLocks: true,
          version0: seedFileName,
        });
        masterDb = await HubWrappers.downloadAndOpenBriefcase({
          accessToken,
          iTwinId,
          iModelId: masterIModelId,
        });
        branchIModelId = await HubWrappers.recreateIModel({
          accessToken,
          iTwinId,
          iModelName: `${name}-branch`,
          noLocks: true,
          version0: masterDb.pathName,
        });
        branchDb = await HubWrappers.downloadAndOpenBriefcase({
          accessToken,
          iTwinId,
          iModelId: branchIModelId,
        });
        await synchronize(masterDb, branchDb, {
          options: { wasSourceIModelCopiedToTarget: true },
        });

        await test({ masterDb, branchDb });
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
          await cleanup("master briefcase", async () =>
            HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb!)
          );
        if (branchDb)
          await cleanup("branch briefcase", async () =>
            HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb!)
          );
        if (masterIModelId)
          await cleanup("master iModel", async () =>
            transformerTestHub.deleteIModel({
              iTwinId,
              iModelId: masterIModelId!,
            })
          );
        if (branchIModelId)
          await cleanup("branch iModel", async () =>
            transformerTestHub.deleteIModel({
              iTwinId,
              iModelId: branchIModelId!,
            })
          );
        if (seedDb) await cleanup("master seed", () => seedDb!.close());
      }
    }

    /** Synchronizes the source's changes to the target, then pushes the target, and for a reverse synchronization, the source. */
    async function synchronize(
      source: BriefcaseDb,
      target: BriefcaseDb,
      {
        reverse = false,
        options = { argsForProcessChanges: {} },
        process = async (syncer) => syncer.process(),
      }: {
        reverse?: boolean;
        options?: IModelTransformOptions;
        process?: (transformer: IModelTransformer) => Promise<void>;
      }
    ): Promise<void> {
      const targetEditTxn = createStartedEditTxn(target);
      const sourceEditTxn = reverse ? createStartedEditTxn(source) : undefined;
      const transformer = new IModelTransformer(
        { source, target: targetEditTxn },
        { ...options, sourceEditTxn }
      );
      let succeeded = false;
      try {
        await process(transformer);
        succeeded = true;
      } finally {
        transformer.dispose();
        targetEditTxn.end(succeeded ? "save" : "abandon");
        sourceEditTxn?.end(succeeded ? "save" : "abandon");
      }
      if (reverse)
        await source.pushChanges({ accessToken, description: "synchronize" });
      await target.pushChanges({ accessToken, description: "synchronize" });
    }

    /** Pushes one edit and returns its changeset index. */
    async function editAndPush(
      db: BriefcaseDb,
      description: string,
      edit: (txn: EditTxn) => void
    ): Promise<number> {
      withEditTxn(db, description, edit);
      await db.pushChanges({ accessToken, description });
      return db.changeset.index!;
    }

    function elementProvenanceAspectIds(
      db: IModelDb,
      elementId: Id64String
    ): Id64String[] {
      return (
        db.elements.getAspects(
          elementId,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[]
      )
        .filter((aspect) => aspect.kind === ExternalSourceAspect.Kind.Element)
        .map((aspect) => aspect.id);
    }

    function elementIds(db: IModelDb): [Id64String, Id64String] {
      return [
        IModelTestUtils.queryByUserLabel(db, "1"),
        IModelTestUtils.queryByUserLabel(db, "2"),
      ];
    }

    it("finds a deleted relationship's endpoint through an aspect deleted in a later changeset", async () =>
      withBranch("RelThenEndpointDelete", async ({ masterDb, branchDb }) => {
        const [masterElement1Id, masterElement2Id] = elementIds(masterDb);
        const [branchElement1Id, branchElement2Id] = elementIds(branchDb);
        expect(branchDb.elements.getElement(branchElement2Id).federationGuid).to
          .be.undefined;
        expect(
          elementProvenanceAspectIds(branchDb, branchElement2Id)
        ).to.have.lengthOf(1);
        const branchRel =
          branchDb.relationships.getInstance<ElementGroupsMembers>(
            ElementGroupsMembers.classFullName,
            { sourceId: branchElement1Id, targetId: branchElement2Id }
          );

        await editAndPush(branchDb, "delete relationship", (txn) =>
          txn.deleteRelationship(branchRel.toJSON())
        );
        await editAndPush(branchDb, "delete endpoint element", (txn) =>
          txn.deleteElement(branchElement2Id)
        );
        let relationshipInTarget: unknown;
        await synchronize(branchDb, masterDb, {
          reverse: true,
          process: async (transformer) => {
            await transformer.process();
            relationshipInTarget = transformer[
              "_deletedSourceRelationshipData"
            ]?.get(branchRel.id);
          },
        });

        // The endpoint's aspect is only in a later changeset of the same batch.
        expect(relationshipInTarget).to.deep.include({
          sourceIdInTarget: masterElement1Id,
          targetIdInTarget: masterElement2Id,
        });
        expect(masterDb.elements.tryGetElementProps(masterElement2Id)).to.be
          .undefined;
      }));

    it("deletes an element whose provenance aspect was deleted in an earlier changeset", async () =>
      withBranch("AspectThenElementDelete", async ({ masterDb, branchDb }) => {
        const [masterElement1Id] = elementIds(masterDb);
        const [branchElement1Id] = elementIds(branchDb);
        const aspectIds = elementProvenanceAspectIds(
          branchDb,
          branchElement1Id
        );
        expect(aspectIds).to.have.lengthOf(1);

        await editAndPush(branchDb, "delete provenance aspect", (txn) =>
          txn.deleteAspect(aspectIds)
        );
        await editAndPush(branchDb, "delete element", (txn) =>
          txn.deleteElement(branchElement1Id)
        );
        await synchronize(branchDb, masterDb, { reverse: true });

        // Best effort: the batch spanning both changesets still has the deleted aspect's identifier.
        expect(masterDb.elements.tryGetElementProps(masterElement1Id)).to.be
          .undefined;
      }));

    it("scans one deletion batch per changeset range", async () =>
      withBranch("DeletionBatchPerRange", async ({ masterDb, branchDb }) => {
        const [masterElement1Id, masterElement2Id] = elementIds(masterDb);
        const [branchElement1Id, branchElement2Id] = elementIds(branchDb);

        const firstIndex = await editAndPush(
          branchDb,
          "delete element 1",
          (txn) => txn.deleteElement(branchElement1Id)
        );
        await editAndPush(masterDb, "update element 2", (txn) =>
          txn.updateElement({
            ...masterDb.elements.getElementProps(masterElement2Id),
            jsonProperties: { updateState: 2 },
          })
        );
        // The reverse synchronization skips the changeset this pushes to the branch.
        await synchronize(masterDb, branchDb, {});
        const secondIndex = await editAndPush(
          branchDb,
          "delete element 2",
          (txn) => txn.deleteElement(branchElement2Id)
        );

        const scans: {
          ranges: readonly (readonly [number, number])[];
          result: ChangeScanResult;
        }[] = [];
        await synchronize(branchDb, masterDb, {
          reverse: true,
          process: async (transformer) => {
            const scanChanges = transformer["scanChanges"].bind(transformer);
            transformer["scanChanges"] = async (ranges) => {
              const result = await scanChanges(ranges);
              scans.push({ ranges, result });
              return result;
            };
            await transformer.process();
          },
        });

        expect(scans).to.have.lengthOf(1);
        expect(scans[0].ranges).to.deep.equal([
          [firstIndex, firstIndex],
          [secondIndex, secondIndex],
        ]);
        expect(
          scans[0].result.deletionBatches.map((batch) =>
            batch.elements.map((record) => record.ecInstanceId)
          )
        ).to.deep.equal([[branchElement1Id], [branchElement2Id]]);
        expect(masterDb.elements.tryGetElementProps(masterElement1Id)).to.be
          .undefined;
        expect(masterDb.elements.tryGetElementProps(masterElement2Id)).to.be
          .undefined;
      }));
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

    // itwinjs-core#9738: a changeset that changes only a navigation property's Id omits its unchanged
    // RelECClassId. Once a later changeset deletes the row, reading that change must not complete the
    // navigation value from the iModel.
    const navigationChangeSchema = `<?xml version="1.0" encoding="UTF-8"?>
      <ECSchema schemaName="TestNavChange" alias="tnc" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
        <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
        <ECEntityClass typeName="Note">
          <BaseClass>bis:ElementMultiAspect</BaseClass>
          <ECProperty propertyName="text" typeName="string"/>
        </ECEntityClass>
      </ECSchema>`;

    async function setUpNavigationChange() {
      await sourceDb.importSchemaStrings([navigationChangeSchema]);
      await sourceDb.pushChanges({
        description: "Import navigation change schema",
        retainLocks: true,
      });
      const ids = withEditTxn(sourceDb, "create navigation targets", (txn) => {
        const subjectId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "NavigationChangeSubject"
        );
        const modelId = DefinitionModel.insert(
          txn,
          subjectId,
          "NavigationChangeModel"
        );
        const insert = (userLabel: string, parentId?: Id64String) =>
          txn.insertElement({
            classFullName: DefinitionContainer.classFullName,
            model: modelId,
            code: Code.createEmpty(),
            userLabel,
            parent:
              parentId === undefined
                ? undefined
                : new ElementOwnsChildElements(parentId),
          });
        const parentAId = insert("NavigationParentA");
        const parentBId = insert("NavigationParentB");
        const childId = insert("NavigationChild", parentAId);
        const noteId = txn.insertAspect({
          classFullName: "TestNavChange:Note",
          element: { id: parentAId },
          text: "original",
        } as ElementAspectProps);
        return { parentBId, childId, noteId };
      });
      await sourceDb.pushChanges({
        description: "Create navigation targets",
        retainLocks: true,
      });

      const editTxn = createStartedEditTxn(targetDb);
      const transformer = new IModelTransformer({
        source: sourceDb,
        target: editTxn,
      });
      try {
        await transformer.processSchemas();
        await transformer.process();
      } finally {
        transformer.dispose();
        editTxn.end();
      }
      await targetDb.pushChanges({
        description: "Transform navigation targets",
        retainLocks: true,
      });
      return ids;
    }

    async function editSourceAndPush(
      description: string,
      edit: (txn: EditTxn) => void
    ) {
      withEditTxn(sourceDb, description, edit);
      await sourceDb.pushChanges({ description, retainLocks: true });
    }

    const targetIdOf = (userLabel: string) =>
      IModelTestUtils.queryByUserLabel(targetDb, userLabel);

    async function reparentChild(deleteChild: boolean) {
      const { parentBId, childId } = await setUpNavigationChange();
      await editSourceAndPush("reparent child", (txn) =>
        txn.updateElement({
          ...sourceDb.elements.getElementProps(childId),
          parent: new ElementOwnsChildElements(parentBId),
        })
      );
      if (deleteChild) {
        await editSourceAndPush("delete reparented child", (txn) =>
          txn.deleteElement(childId)
        );
      }
      await processChangesWith();
    }

    it("should delete an element after a reparenting change", async () => {
      await reparentChild(true);
      expect(targetIdOf("NavigationChild")).to.equal(Id64.invalid);
    });

    it("should reparent a surviving element", async () => {
      await reparentChild(false);
      expect(
        targetDb.elements.getElementProps(targetIdOf("NavigationChild")).parent
          ?.id
      ).to.equal(targetIdOf("NavigationParentB"));
    });

    async function changeNoteThenDelete(
      change: (
        txn: EditTxn,
        noteProps: ElementAspectProps,
        parentBId: Id64String
      ) => void
    ) {
      const { parentBId, noteId } = await setUpNavigationChange();
      const targetNotes = () =>
        ["NavigationParentA", "NavigationParentB"].map(
          (label) =>
            targetDb.elements.getAspects(
              targetIdOf(label),
              "TestNavChange:Note"
            ).length
        );
      expect(targetNotes()).to.deep.equal([1, 0]);

      await editSourceAndPush("change note", (txn) =>
        change(txn, sourceDb.elements.getAspect(noteId).toJSON(), parentBId)
      );
      await editSourceAndPush("delete note", (txn) => txn.deleteAspect(noteId));
      await processChangesWith();
      expect(targetNotes()).to.deep.equal([0, 0]);
    }

    it("should delete an aspect after its owner changed", async () => {
      await changeNoteThenDelete((txn, noteProps, parentBId) =>
        txn.updateAspect({
          ...noteProps,
          element: new ElementOwnsMultiAspects(parentBId),
        })
      );
    });

    it("should delete an aspect after its properties changed", async () => {
      await changeNoteThenDelete((txn, noteProps) =>
        txn.updateAspect({
          ...noteProps,
          text: "updated",
        } as ElementAspectProps)
      );
    });

    it("classifies aspect deletions by class", async () => {
      const { subjectId, aspectId } = withEditTxn(
        sourceDb,
        "insert subject with aspect",
        (txn) => {
          const insertedSubjectId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "Aspect classification"
          );
          const insertedAspectId = txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(insertedSubjectId),
            scope: { id: IModel.rootSubjectId },
            kind: "Document",
            identifier: "classified-aspect",
          } as ExternalSourceAspectProps);
          return { subjectId: insertedSubjectId, aspectId: insertedAspectId };
        }
      );
      await sourceDb.pushChanges({
        description: "Insert subject with aspect",
        retainLocks: true,
      });

      const initialEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialEditTxn,
      });
      await transformer.process();
      const targetSubjectId =
        transformer.context.findTargetElementId(subjectId);
      transformer.dispose();
      initialEditTxn.end();
      await targetDb.pushChanges({
        description: "Initial aspect classification transformation",
        retainLocks: true,
      });
      expect(Id64.isValid(targetSubjectId)).to.be.true;

      withEditTxn(sourceDb, "delete subject with aspect", (txn) => {
        txn.deleteElement(subjectId);
      });
      await sourceDb.pushChanges({
        description: "Delete subject with aspect",
        retainLocks: true,
      });

      const scan = ChangesetScanner.scan.bind(ChangesetScanner);
      const scannedBatches: DeletionBatch[][] = [];
      const scanSpy = vi
        .spyOn(ChangesetScanner, "scan")
        .mockImplementation(async (...args) => {
          const result = await scan(...args);
          scannedBatches.push(result);
          return result;
        });
      const changesEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changesEditTxn },
        { argsForProcessChanges: {} }
      );
      const processedDeletions: Id64String[] = [];
      const processDeletedElement =
        transformer["processDeletedElement"].bind(transformer);
      transformer["processDeletedElement"] = async (...args) => {
        processedDeletions.push(args[0]);
        return processDeletedElement(...args);
      };
      try {
        await transformer.process();
      } finally {
        scanSpy.mockRestore();
        transformer.dispose();
        changesEditTxn.end();
      }

      expect(scannedBatches).to.have.lengthOf(1);
      const batches = scannedBatches[0];
      expect(
        batches.flatMap((batch) => batch.elements.map((e) => e.ecInstanceId))
      ).to.deep.equal([subjectId]);
      expect(
        batches.flatMap((batch) =>
          batch.externalSourceAspects.map((a) => a.ecInstanceId)
        )
      ).to.deep.equal([aspectId]);
      expect(processedDeletions).to.deep.equal([subjectId]);
      expect(targetDb.elements.tryGetElement(targetSubjectId)).toBeUndefined();
    });

    it("uses changes from a cache without downloading changesets", async () => {
      const { elementId, ecClassId, federationGuid, deleteChangesetIndex } =
        await syncElementThenDeleteInSource();

      const requestedRanges: (readonly [number, number])[] = [];
      const cache: ChangeCache = {
        tryGetChanges: async (_iModel, range) => {
          requestedRanges.push(range);
          return Promise.resolve({
            changes: [{ id: elementId, ecClassId, op: "Deleted" }],
            deletions: {
              elements: [
                { ecInstanceId: elementId, ecClassId, federationGuid },
              ],
              models: [],
              relationships: [],
              externalSourceAspects: [],
            },
          });
        },
      };

      const downloadSpy = vi.spyOn(BriefcaseManager, "downloadChangesets");
      try {
        await processChangesWith(
          (...args) => new CachedChangesTransformer(cache, ...args)
        );
        expect(downloadSpy).not.toHaveBeenCalled();
      } finally {
        downloadSpy.mockRestore();
      }

      expect(requestedRanges).to.deep.equal([
        [deleteChangesetIndex, deleteChangesetIndex],
      ]);
      expect(targetDb.elements.getIdFromFederationGuid(federationGuid)).to.be
        .undefined;
    });

    it("falls back to the default changeset scan for ranges missing from a cache", async () => {
      const { federationGuid, deleteChangesetIndex } =
        await syncElementThenDeleteInSource();

      const requestedRanges: (readonly [number, number])[] = [];
      const cache: ChangeCache = {
        tryGetChanges: async (_iModel, range) => {
          requestedRanges.push(range);
          return Promise.resolve(undefined);
        },
      };

      const downloadSpy = vi.spyOn(BriefcaseManager, "downloadChangesets");
      try {
        await processChangesWith(
          (...args) => new CachedChangesTransformer(cache, ...args)
        );
        expect(
          downloadSpy.mock.calls.map(([args]) => args.range)
        ).to.deep.equal([
          { first: deleteChangesetIndex, end: deleteChangesetIndex },
        ]);
      } finally {
        downloadSpy.mockRestore();
      }

      expect(requestedRanges).to.deep.equal([
        [deleteChangesetIndex, deleteChangesetIndex],
      ]);
      expect(targetDb.elements.getIdFromFederationGuid(federationGuid)).to.be
        .undefined;
    });

    it("combines changes from a scanChanges override with the default scan", async () => {
      const { elementId, ecClassId, federationGuid, deleteChangesetIndex } =
        await syncElementThenDeleteInSource();
      const subjectId = withEditTxn(sourceDb, "insert subject", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "Scanned subject")
      );
      await sourceDb.pushChanges({
        description: "insert subject",
        retainLocks: true,
      });
      const subjectChangesetIndex = sourceDb.changeset.index!;

      // Supplies the delete changeset itself and passes the rest to the default scan.
      class PartlyCachedTransformer extends IModelTransformer {
        protected override async scanChanges(
          ranges: readonly (readonly [number, number])[],
          changedInstanceIds = new ChangedInstanceIds(this.sourceDb)
        ): Promise<ChangeScanResult> {
          const deletionBatches: DeletionBatch[] = [];
          for (const [first, last] of ranges) {
            for (let index = first; index <= last; index++) {
              if (index !== deleteChangesetIndex) {
                const scanned = await super.scanChanges(
                  [[index, index]],
                  changedInstanceIds
                );
                deletionBatches.push(...scanned.deletionBatches);
                continue;
              }
              await changedInstanceIds.addChangeRecord({
                id: elementId,
                ecClassId,
                op: "Deleted",
              });
              deletionBatches.push({
                elements: [
                  { ecInstanceId: elementId, ecClassId, federationGuid },
                ],
                models: [],
                relationships: [],
                externalSourceAspects: [],
              });
            }
          }
          return { changedInstanceIds, deletionBatches };
        }
      }

      const downloadSpy = vi.spyOn(BriefcaseManager, "downloadChangesets");
      try {
        await processChangesWith(
          (...args) => new PartlyCachedTransformer(...args)
        );
        expect(
          downloadSpy.mock.calls.map(([args]) => args.range)
        ).to.deep.equal([
          { first: subjectChangesetIndex, end: subjectChangesetIndex },
        ]);
      } finally {
        downloadSpy.mockRestore();
      }

      expect(targetDb.elements.getIdFromFederationGuid(federationGuid)).to.be
        .undefined;
      const subjectFederationGuid =
        sourceDb.elements.getElement(subjectId).federationGuid!;
      expect(targetDb.elements.getIdFromFederationGuid(subjectFederationGuid))
        .to.not.be.undefined;
    });

    it("doesn't call scanChanges when changedInstanceIds are supplied", async () => {
      const { elementId, federationGuid } =
        await syncElementThenDeleteInSource();

      class UnusedScanTransformer extends IModelTransformer {
        protected override async scanChanges(): Promise<ChangeScanResult> {
          throw new Error("scanChanges should not be called");
        }
      }
      const changedInstanceIds = new ChangedInstanceIds(sourceDb);
      changedInstanceIds.element.deleteIds.add(elementId);
      const editTxn = createStartedEditTxn(targetDb);
      const transformer = new UnusedScanTransformer(
        { source: sourceDb, target: editTxn },
        { argsForProcessChanges: { changedInstanceIds } }
      );
      try {
        await transformer.process();
      } finally {
        transformer.dispose();
        editTxn.end();
      }

      // The deletion record read from the downloaded changeset finds the target element.
      expect(targetDb.elements.getIdFromFederationGuid(federationGuid)).to.be
        .undefined;
    });

    /** Inserts an element, synchronizes it to the target, then deletes it in the source. */
    async function syncElementThenDeleteInSource() {
      const federationGuid = Guid.createValue();
      const elementId = withEditTxn(sourceDb, "insert element", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModelDb.rootSubjectId,
          "SourcePhysicalModel"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModelDb.dictionaryId,
          "SourceCategory",
          {}
        );
        return txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: Code.createEmpty(),
          federationGuid,
          userLabel: "ScannedElement",
        } as PhysicalElementProps);
      });
      await sourceDb.pushChanges({
        description: "insert element",
        retainLocks: true,
      });

      const firstEditTxn = createStartedEditTxn(targetDb);
      const firstTransformer = new IModelTransformer({
        source: sourceDb,
        target: firstEditTxn,
      });
      await firstTransformer.process();
      firstTransformer.dispose();
      firstEditTxn.end();
      await targetDb.pushChanges({
        description: "initial transformation",
        retainLocks: true,
      });
      expect(targetDb.elements.getIdFromFederationGuid(federationGuid)).to.not
        .be.undefined;

      const ecClassId = sourceDb.withQueryReader(
        "SELECT ECClassId FROM bis.Element WHERE ECInstanceId=:elementId",
        (reader) => {
          expect(reader.step()).to.be.true;
          return reader.current[0] as Id64String;
        },
        new QueryBinder().bindId("elementId", elementId)
      );
      withEditTxn(sourceDb, "delete element", (txn) => {
        txn.deleteElement(elementId);
      });
      await sourceDb.pushChanges({
        description: "delete element",
        retainLocks: true,
      });
      const deleteChangesetIndex = sourceDb.changeset.index!;

      return { elementId, ecClassId, federationGuid, deleteChangesetIndex };
    }

    async function processChangesWith(
      createTransformer: (
        ...args: ConstructorParameters<typeof IModelTransformer>
      ) => IModelTransformer = (...args) => new IModelTransformer(...args)
    ): Promise<void> {
      const editTxn = createStartedEditTxn(targetDb);
      const transformer = createTransformer(
        { source: sourceDb, target: editTxn },
        { argsForProcessChanges: {} }
      );
      try {
        await transformer.process();
      } finally {
        transformer.dispose();
        editTxn.end();
      }
    }
  });

  async function prepareBriefcase(name: string) {
    const iModelId = await HubWrappers.createIModel(accessToken, iTwinId, name);

    const newBriefcase = await HubWrappers.downloadAndOpenBriefcase({
      accessToken: await IModelHost.getAccessToken(),
      iTwinId,
      iModelId,
      asOf: IModelVersion.latest().toJSON(),
    });
    await newBriefcase.locks.acquireLocks({
      shared: "0x10",
      exclusive: "0x1",
    });
    return newBriefcase;
  }

  async function closeAndDeleteBriefcase(iModel: BriefcaseDb) {
    await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, iModel);
    await transformerTestHub.deleteIModel({
      iTwinId,
      iModelId: iModel.iModelId,
    });
  }
});
