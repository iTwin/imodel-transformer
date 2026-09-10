/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as path from "node:path";
import {
  ChangeInstance,
  ChangesetReader,
  ElementGroupsMembers,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  PartialChangeUnifier,
  PropertyFilter,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import {
  ChangesetFileProps,
  ElementAspectProps,
  ExternalSourceAspectProps,
  IModel,
} from "@itwin/core-common";
import { ChangesetScanner } from "../../ChangesetScanner";
import { ChangedInstanceIds } from "../../IModelExporter";
import { KnownTestLocations } from "../TestUtils";
import { importElementAspectTestSchema } from "../TestUtils/ElementAspectTestUtils";

// Exercise native filtering and unification against a real pending transaction;
// only replace the file-opening seam, not the reader or its change records.
describe("ChangesetScanner property projection", () => {
  it("preserves deletion metadata and both owners when an aspect moves", async () => {
    const db = StandaloneDb.createEmpty(
      path.join(
        KnownTestLocations.outputDir,
        "scanner-property-projection.bim"
      ),
      { rootSubject: { name: "scanner" }, enableTransactions: true }
    );
    try {
      await importElementAspectTestSchema(db);
      const seed = withEditTxn(db, "create scan fixture", (txn) => {
        const oldOwner = Subject.insert(txn, IModel.rootSubjectId, "old owner");
        const newOwner = Subject.insert(txn, IModel.rootSubjectId, "new owner");
        const deletedElement = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "deleted"
        );
        const insertAspect = (identifier: string, owner = oldOwner) => {
          const props: ExternalSourceAspectProps = {
            classFullName: ExternalSourceAspect.classFullName,
            element: {
              id: owner,
              relClassName: ElementOwnsExternalSourceAspects.classFullName,
            },
            scope: { id: IModel.rootSubjectId },
            kind: "Element",
            identifier,
          };
          return txn.insertAspect(props);
        };
        const unchangedOwner = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "unchanged owner"
        );
        const updatedAspect = insertAspect("updated", unchangedOwner);
        const uniqueOwner = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "unique owner"
        );
        const uniqueAspect = txn.insertAspect({
          classFullName: "ExporterAspectTest:UniqueAspect",
          element: { id: uniqueOwner },
          binaryValue: new Uint8Array([1]),
        } as ElementAspectProps & { binaryValue: Uint8Array });
        const movedAspect = insertAspect("moved");
        const deletedAspect = insertAspect("deleted", newOwner);
        const relationship = ElementGroupsMembers.insert(
          txn,
          oldOwner,
          newOwner
        );
        return {
          oldOwner,
          newOwner,
          deletedElement,
          movedAspect,
          updatedAspect,
          unchangedOwner,
          uniqueOwner,
          uniqueAspect,
          deletedAspect,
          relationship,
        };
      });
      const federationGuid = db.elements.getElement(
        seed.deletedElement
      ).federationGuid;
      expect(federationGuid).toBeTypeOf("string");
      await withEditTxn(db, "change scan fixture", async (txn) => {
        // Construct a historical owner change at the SQLite changeset boundary.
        // updateAspect deliberately does not change an aspect's owner.
        db.withPreparedSqliteStatement(
          "UPDATE bis_ElementMultiAspect SET ElementId=? WHERE Id=?",
          (stmt) => {
            stmt.bindId(1, seed.newOwner);
            stmt.bindId(2, seed.movedAspect);
            stmt.step();
          }
        );
        txn.updateAspect({
          ...db.elements.getAspect(seed.uniqueAspect).toJSON(),
          binaryValue: new Uint8Array([2]),
        } as ElementAspectProps & { binaryValue: Uint8Array });
        txn.updateAspect({
          ...db.elements.getAspect(seed.updatedAspect).toJSON(),
          identifier: "updated identifier",
        } as ExternalSourceAspectProps);
        txn.deleteAspect(seed.deletedAspect);
        txn.deleteRelationship({
          classFullName: ElementGroupsMembers.classFullName,
          id: seed.relationship,
          sourceId: seed.oldOwner,
          targetId: seed.newOwner,
        });
        txn.deleteElement(seed.deletedElement);
        txn.updateElement({
          ...db.elements.getElement(seed.newOwner).toJSON(),
          userLabel: "updated",
        });

        const referenceIds = new ChangedInstanceIds(db);
        const referenceDeletions = [];
        {
          using reader = ChangesetReader.openInMemoryChanges({
            db,
            propFilter: PropertyFilter.BisCoreElement,
          });
          using unifier = new PartialChangeUnifier();
          while (reader.step()) unifier.appendFrom(reader);
          for (const change of unifier.instances) {
            if (
              (change.$meta.op === "Inserted" ||
                change.$meta.op === "Deleted") &&
              change.$meta.tables.every((table) => table.endsWith("Overflow"))
            )
              change.$meta.op = "Updated";
            await referenceIds.addChange(change);
            if (change.$meta.op === "Deleted")
              referenceDeletions.push(
                ChangesetScanner["toDeletionRecord"](db, change)
              );
          }
        }
        expect([...referenceIds.aspectOwnerElementIds].sort()).toEqual(
          [
            seed.oldOwner,
            seed.newOwner,
            seed.unchangedOwner,
            seed.uniqueOwner,
          ].sort()
        );
        const openSpy = vi
          .spyOn(ChangesetReader, "openFile")
          .mockImplementation((args) =>
            ChangesetReader.openInMemoryChanges({
              db,
              propFilter: args.propFilter,
            })
          );
        try {
          const ids = new ChangedInstanceIds(db);
          const files = [{ pathname: "in-memory" } as ChangesetFileProps];
          const deletions = await ids.scanChangesets(files);
          for (const bucket of [
            "element",
            "model",
            "aspect",
            "relationship",
            "codeSpec",
            "font",
          ] as const)
            expect(ids[bucket]).toEqual(referenceIds[bucket]);
          expect([...ids.aspectOwnerElementIds].sort()).toEqual(
            [...referenceIds.aspectOwnerElementIds].sort()
          );
          expect(deletions).toEqual([referenceDeletions]);
          expect(deletions[0]).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                ecInstanceId: seed.deletedElement,
                federationGuid,
              }),
              expect.objectContaining({
                ecInstanceId: seed.relationship,
                sourceECInstanceId: seed.oldOwner,
                targetECInstanceId: seed.newOwner,
              }),
              expect.objectContaining({
                ecInstanceId: seed.deletedAspect,
                elementId: seed.newOwner,
                scopeId: IModel.rootSubjectId,
                kind: "Element",
                identifier: "deleted",
              }),
            ])
          );
          const getAspectSpy = vi.spyOn(db.elements, "getAspect");
          const querySpy = vi.spyOn(db, "withQueryReader");
          try {
            const initialized = await ChangedInstanceIds.initialize({
              iModel: db,
              csFileProps: [...files, ...files],
            });
            expect([...initialized!.aspectOwnerElementIds].sort()).toEqual(
              [...referenceIds.aspectOwnerElementIds].sort()
            );
            expect(getAspectSpy).not.toHaveBeenCalled();
            expect(
              querySpy.mock.calls.filter(([sql]) =>
                sql.includes("IdSet(:aspectIds)")
              )
            ).toHaveLength(1);
          } finally {
            getAspectSpy.mockRestore();
            querySpy.mockRestore();
          }
          const unpopulated = new ChangedInstanceIds(db);
          expect(
            await unpopulated.scanChangesets(files, {
              populateChangedInstanceIds: false,
            })
          ).toEqual(deletions);
          expect(unpopulated.hasChanges).toBe(false);
        } finally {
          openSpy.mockRestore();
        }
      });
    } finally {
      db.close();
    }
  });

  it("resolves uncommitted owners, ignores missing aspects, and clears deferral on failure", async () => {
    const db = StandaloneDb.createEmpty(
      path.join(KnownTestLocations.outputDir, "scanner-owner-fallback.bim"),
      { rootSubject: { name: "scanner owners" }, enableTransactions: true }
    );
    try {
      await withEditTxn(db, "create uncommitted aspect", async (txn) => {
        const owner = Subject.insert(txn, IModel.rootSubjectId, "owner");
        const aspectId = txn.insertAspect({
          classFullName: ExternalSourceAspect.classFullName,
          element: { id: owner },
          scope: { id: IModel.rootSubjectId },
          kind: "Element",
          identifier: "uncommitted",
        } as ExternalSourceAspectProps);
        const classId = db.withQueryReader(
          "SELECT ECClassId FROM BisCore.ExternalSourceAspect",
          (reader) => {
            expect(reader.step()).toBe(true);
            return reader.current[0];
          }
        );
        const change = (id: string) =>
          ({
            ECInstanceId: id,
            ECClassId: classId,
            $meta: {
              op: "Updated",
              stage: "New",
              tables: ["bis_ElementMultiAspect"],
              changeIndexes: [1],
              instanceKey: `${classId}-${id}`,
              propFilter: PropertyFilter.BisCoreElement,
              changeFetchedPropNames: [],
              isIndirectChange: false,
            },
          }) as ChangeInstance;
        const scanSpy = vi
          .spyOn(ChangesetScanner, "scan")
          .mockImplementation(async (_db, _files, ids) => {
            for (let i = 0; i < 3; i++) {
              await ids.addChange(change(aspectId));
              await ids.addChange(change("0xffffffffff"));
            }
            return [];
          });
        const getAspectSpy = vi.spyOn(db.elements, "getAspect");
        try {
          const ids = new ChangedInstanceIds(db);
          await ids.scanChangesets([]);
          expect([...ids.aspectOwnerElementIds]).toEqual([owner]);
          expect(getAspectSpy).not.toHaveBeenCalled();

          // The same object must resolve immediately outside a scan.
          await ids.addChange(change(aspectId));
          expect(getAspectSpy).toHaveBeenCalledTimes(1);
          getAspectSpy.mockClear();
          const failure = new Error("scan failed");
          scanSpy.mockImplementationOnce(async (_db, _files, changes) => {
            await changes.addChange(change(aspectId));
            throw failure;
          });
          await expect(ids.scanChangesets([])).rejects.toBe(failure);
          await ids.addChange(change(aspectId));
          expect(getAspectSpy).toHaveBeenCalledTimes(1);
        } finally {
          getAspectSpy.mockRestore();
          scanSpy.mockRestore();
        }
      });
    } finally {
      db.close();
    }
  });
});
