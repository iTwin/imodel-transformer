/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as path from "node:path";
import {
  ChangesetReader,
  ChangeUnifier,
  ChangeUnifierArgs,
  EditTxn,
  ElementGroupsMembers,
  ExternalSourceAspect,
  PartialChangeUnifier,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { IModelStatus } from "@itwin/core-bentley";
import {
  ChangesetFileProps,
  ExternalSourceAspectProps,
  IModel,
  QueryBinder,
} from "@itwin/core-common";
import {
  addDeletionRecord,
  ChangesetScanner,
  DeletedInstance,
  DeletionBatch,
} from "../../ChangesetScanner";
import { SourceClassKinds } from "../../SourceClassKinds";
import { IModelTransformerError } from "../../IModelTransformerError";
import { expectTransformerError } from "../IModelTransformerUtils";
import { ChangedInstanceIds, ChangedInstanceOps } from "../../IModelExporter";
import { KnownTestLocations } from "../TestUtils";
import { importElementAspectTestSchema } from "../TestUtils/ElementAspectTestUtils";

describe("ChangesetScanner owner resolution", () => {
  let db: StandaloneDb;
  let fixtureIndex = 0;
  // Only the file-opening seam is replaced; native reading and unification remain real.
  const files = [{ pathname: "in-memory" } as ChangesetFileProps];

  beforeEach(async () => {
    db = StandaloneDb.createEmpty(
      path.join(
        KnownTestLocations.outputDir,
        `scanner-owner-resolution-${++fixtureIndex}.bim`
      ),
      { rootSubject: { name: "scanner" }, enableTransactions: true }
    );
    await importElementAspectTestSchema(db);
    vi.spyOn(ChangesetReader, "openFile").mockImplementation((args) =>
      ChangesetReader.openInMemoryChanges({ db, propFilter: args.propFilter })
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
    db.close();
  });

  function insertAspect(txn: EditTxn, owner: string): string {
    const props: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: owner },
      scope: { id: IModel.rootSubjectId },
      kind: "Element",
      identifier: "aspect",
    };
    return txn.insertAspect(props);
  }

  const noDeletions: DeletionBatch = {
    elements: [],
    models: [],
    relationships: [],
    externalSourceAspects: [],
  };

  function classIdFor(className: string, id: string): string {
    return db.withQueryReader(
      `SELECT ECClassId FROM ${className} WHERE ECInstanceId=:id`,
      (reader) => {
        expect(reader.step()).toBe(true);
        return reader.current[0];
      },
      new QueryBinder().bindId("id", id)
    );
  }

  it("retains both historical owners of a moved aspect independently of other changes", async () => {
    const seed = withEditTxn(db, "create moved aspect", (txn) => {
      const oldOwner = Subject.insert(txn, IModel.rootSubjectId, "old owner");
      const newOwner = Subject.insert(txn, IModel.rootSubjectId, "new owner");
      return { oldOwner, newOwner, aspect: insertAspect(txn, oldOwner) };
    });
    await withEditTxn(db, "move aspect", async () => {
      // updateAspect does not change owners. Construct the historical change at the SQLite boundary.
      db.withPreparedSqliteStatement(
        "UPDATE bis_ElementMultiAspect SET ElementId=? WHERE Id=?",
        (stmt) => {
          stmt.bindId(1, seed.newOwner);
          stmt.bindId(2, seed.aspect);
          stmt.step();
        }
      );
      const ids = new ChangedInstanceIds(db);
      const querySpy = vi.spyOn(db, "withQueryReader");
      const getAspectSpy = vi.spyOn(db.elements, "getAspect");
      expect(
        await ChangesetScanner.scan(db, [files], { changedInstanceIds: ids })
      ).toEqual([noDeletions]);
      expect([...ids.aspectOwnerElementIds].sort()).toEqual(
        [seed.oldOwner, seed.newOwner].sort()
      );
      expect([...ids.aspect.updateIds]).toEqual([seed.aspect]);
      expect(querySpy).not.toHaveBeenCalled();
      expect(getAspectSpy).not.toHaveBeenCalled();
    });
  });

  it("resolves unique and multi-aspect owners in one bulk query across repeated changesets", async () => {
    const seed = withEditTxn(db, "create updated aspects", (txn) => {
      const multiOwner = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "multi owner"
      );
      const uniqueOwner = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "unique owner"
      );
      const uniqueProps = {
        classFullName: "ExporterAspectTest:UniqueAspect",
        element: { id: uniqueOwner },
        binaryValue: new Uint8Array([1]),
      };
      return {
        multiOwner,
        uniqueOwner,
        multi: insertAspect(txn, multiOwner),
        unique: txn.insertAspect(uniqueProps),
      };
    });
    await withEditTxn(
      db,
      "update aspects without changing owners",
      async (txn) => {
        const multiProps = {
          ...db.elements.getAspect(seed.multi).toJSON(),
          identifier: "updated",
        };
        const uniqueProps = {
          ...db.elements.getAspect(seed.unique).toJSON(),
          binaryValue: new Uint8Array([2]),
        };
        txn.updateAspect(multiProps);
        txn.updateAspect(uniqueProps);
        const getAspectSpy = vi.spyOn(db.elements, "getAspect");
        const querySpy = vi.spyOn(db, "withQueryReader");
        const ids = await ChangedInstanceIds.initialize({
          iModel: db,
          csFileProps: [...files, ...files],
        });
        expect(ids).toBeDefined();
        expect([...ids!.aspectOwnerElementIds].sort()).toEqual(
          [seed.multiOwner, seed.uniqueOwner].sort()
        );
        expect([...ids!.aspect.updateIds].sort()).toEqual(
          [seed.multi, seed.unique].sort()
        );
        expect(getAspectSpy).not.toHaveBeenCalled();
        expect(querySpy).toHaveBeenCalledTimes(1);
      }
    );
  });

  it("retains deletion metadata from known source values", async () => {
    const seed = withEditTxn(db, "create deleted instances", (txn) => {
      const owner = Subject.insert(txn, IModel.rootSubjectId, "aspect owner");
      const target = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "relationship target"
      );
      const element = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "deleted element"
      );
      const aspect = insertAspect(txn, owner);
      const relationship = ElementGroupsMembers.insert(txn, owner, target);
      return { owner, target, element, aspect, relationship };
    });
    const expected: DeletionBatch = {
      elements: [
        {
          ecInstanceId: seed.element,
          ecClassId: classIdFor(Subject.classFullName, seed.element),
          federationGuid: db.elements.getElement(seed.element).federationGuid,
        },
      ],
      models: [],
      relationships: [
        {
          ecInstanceId: seed.relationship,
          ecClassId: classIdFor(
            ElementGroupsMembers.classFullName,
            seed.relationship
          ),
          sourceECInstanceId: seed.owner,
          targetECInstanceId: seed.target,
        },
      ],
      externalSourceAspects: [
        {
          ecInstanceId: seed.aspect,
          ecClassId: classIdFor(
            ExternalSourceAspect.classFullName,
            seed.aspect
          ),
          elementId: seed.owner,
          scopeId: IModel.rootSubjectId,
          kind: "Element",
          identifier: "aspect",
        },
      ],
    };
    expect(expected.elements[0].federationGuid).toBeTypeOf("string");
    await withEditTxn(db, "delete instances", async (txn) => {
      txn.deleteAspect(seed.aspect);
      txn.deleteRelationship({
        classFullName: ElementGroupsMembers.classFullName,
        id: seed.relationship,
        sourceId: seed.owner,
        targetId: seed.target,
      });
      txn.deleteElement(seed.element);
      const ids = new ChangedInstanceIds(db);
      const records = await ChangesetScanner.scan(db, [files], {
        changedInstanceIds: ids,
      });
      expect(records).toEqual([expected]);
      expect([...ids.aspectOwnerElementIds]).toEqual([seed.owner]);
      expect([...ids.aspect.deleteIds]).toEqual([seed.aspect]);
      expect([...ids.relationship.deleteIds]).toEqual([seed.relationship]);
      expect([...ids.element.deleteIds]).toEqual([seed.element]);
    });
  });

  it("collects deletions without changed IDs or querying current owners", async () => {
    const seed = withEditTxn(db, "create deletion-only fixture", (txn) => {
      const owner = Subject.insert(txn, IModel.rootSubjectId, "owner");
      return { owner, aspect: insertAspect(txn, owner) };
    });
    const classId = classIdFor(ExternalSourceAspect.classFullName, seed.aspect);
    await withEditTxn(db, "delete aspect", async (txn) => {
      txn.deleteAspect(seed.aspect);
      const querySpy = vi.spyOn(db, "withQueryReader");
      const records = await ChangesetScanner.scan(db, [files], {});
      expect(records).toEqual([
        {
          ...noDeletions,
          externalSourceAspects: [
            {
              ecInstanceId: seed.aspect,
              ecClassId: classId,
              elementId: seed.owner,
              scopeId: IModel.rootSubjectId,
              kind: "Element",
              identifier: "aspect",
            },
          ],
        },
      ]);
      expect(querySpy).not.toHaveBeenCalled();
    });
  });

  it("preserves ChangesetReader errors", async () => {
    const readerError = new Error("reader failed");
    vi.spyOn(ChangesetReader, "openFile").mockImplementation(() => {
      throw readerError;
    });
    await expect(
      ChangesetScanner.scan(db, [files], {
        changedInstanceIds: new ChangedInstanceIds(db),
      })
    ).rejects.toBe(readerError);
  });

  describe("bounded unification (#434)", () => {
    function snapshot(ids: ChangedInstanceIds) {
      const ops = (o: ChangedInstanceOps) => ({
        insert: [...o.insertIds].sort(),
        update: [...o.updateIds].sort(),
        delete: [...o.deleteIds].sort(),
      });
      return {
        codeSpec: ops(ids.codeSpec),
        model: ops(ids.model),
        element: ops(ids.element),
        aspect: ops(ids.aspect),
        relationship: ops(ids.relationship),
        aspectOwnerElementIds: [...ids.aspectOwnerElementIds].sort(),
      };
    }

    async function scanSnapshot(csFiles: ChangesetFileProps[]) {
      const ids = new ChangedInstanceIds(db);
      const deletionBatches = await ChangesetScanner.scan(db, [csFiles], {
        changedInstanceIds: ids,
      });
      return { changes: snapshot(ids), deletionBatches };
    }

    it("unifies each changeset natively with a spilling memory budget instead of PartialChangeUnifier", async () => {
      const seed = withEditTxn(db, "create element", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "deleted")
      );
      await withEditTxn(db, "change elements", async (txn) => {
        const inserted = Subject.insert(txn, IModel.rootSubjectId, "inserted");
        txn.deleteElement(seed);
        const fromReader = vi.spyOn(ChangeUnifier, "fromReader");
        const dispose = vi.spyOn(ChangeUnifier.prototype, Symbol.dispose);
        const appendFrom = vi.spyOn(
          PartialChangeUnifier.prototype,
          "appendFrom"
        );
        const { changes } = await scanSnapshot([...files, ...files]);
        expect(fromReader).toHaveBeenCalledTimes(2);
        for (const [, args] of fromReader.mock.calls) {
          // A budget of 0 never spills, leaving memory bound only by changeset size.
          const budget = args?.memoryBudgetBytes;
          expect(
            budget === undefined || (Number.isSafeInteger(budget) && budget > 0)
          ).toBe(true);
        }
        expect(dispose).toHaveBeenCalledTimes(2);
        expect(appendFrom).not.toHaveBeenCalled();
        expect(changes.element.insert).toEqual([inserted]);
        expect(changes.element.delete).toEqual([seed]);
      });
    });

    it("returns the same changes and deletion records when the unifier spills to temporary files", async () => {
      const count = 20;
      const seed = withEditTxn(db, "create seed instances", (txn) => {
        const subjects = Array.from({ length: count }, (_, i) =>
          Subject.insert(txn, IModel.rootSubjectId, `seed ${i}`)
        );
        return {
          subjects,
          aspects: subjects.map((owner) => insertAspect(txn, owner)),
          relationships: subjects
            .slice(1)
            .map((member) =>
              ElementGroupsMembers.insert(txn, subjects[0], member)
            ),
        };
      });
      await withEditTxn(db, "change instances", async (txn) => {
        const inserted = Array.from({ length: count }, (_, i) => {
          const id = Subject.insert(txn, IModel.rootSubjectId, `new ${i}`);
          insertAspect(txn, id);
          return id;
        });
        for (const id of seed.subjects.slice(0, 5))
          txn.updateElement({
            ...db.elements.getElementProps(id),
            userLabel: "updated",
          });
        for (const id of seed.aspects.slice(5, 10)) {
          const aspectProps = {
            ...db.elements.getAspect(id).toJSON(),
            identifier: "updated",
          };
          txn.updateAspect(aspectProps);
        }
        // Move an aspect between owners so both the old and new owners must be retained.
        db.withPreparedSqliteStatement(
          "UPDATE bis_ElementMultiAspect SET ElementId=? WHERE Id=?",
          (stmt) => {
            stmt.bindId(1, seed.subjects[11]);
            stmt.bindId(2, seed.aspects[10]);
            stmt.step();
          }
        );
        txn.deleteRelationship({
          classFullName: ElementGroupsMembers.classFullName,
          id: seed.relationships[11],
          sourceId: seed.subjects[0],
          targetId: seed.subjects[12],
        });
        // Deleting elements also deletes their aspects and relationships.
        const deleted = seed.subjects.slice(15);
        txn.deleteElement(deleted);

        const inMemory = await scanSnapshot(files);
        expect(inMemory.changes.element.insert).toEqual([...inserted].sort());
        expect(inMemory.changes.element.delete).toEqual([...deleted].sort());
        expect(inMemory.changes.aspect.delete).toHaveLength(deleted.length);
        expect(inMemory.changes.relationship.delete).toHaveLength(
          deleted.length + 1
        );
        expect(inMemory.changes.aspectOwnerElementIds).toEqual(
          expect.arrayContaining([seed.subjects[10], seed.subjects[11]])
        );
        expect(inMemory.deletionBatches[0].elements).toHaveLength(
          deleted.length
        );

        const fromReader = ChangeUnifier.fromReader.bind(ChangeUnifier);
        const spilling = vi
          .spyOn(ChangeUnifier, "fromReader")
          .mockImplementation((reader, args?: ChangeUnifierArgs) =>
            fromReader(reader, { ...args, memoryBudgetBytes: 1, batchSize: 3 })
          );
        expect(await scanSnapshot(files)).toEqual(inMemory);
        expect(spilling).toHaveBeenCalledTimes(1);
      });
    });

    it("disposes the reader and preserves the error when unification fails", async () => {
      const unifierError = new Error("unifier failed");
      vi.spyOn(ChangeUnifier, "fromReader").mockImplementation(() => {
        throw unifierError;
      });
      const readerDispose = vi.spyOn(ChangesetReader.prototype, Symbol.dispose);
      await withEditTxn(db, "change element", async (txn) => {
        Subject.insert(txn, IModel.rootSubjectId, "inserted");
        await expect(scanSnapshot(files)).rejects.toBe(unifierError);
      });
      expect(readerDispose).toHaveBeenCalledTimes(1);
    });
  });

  describe("deletion classification", () => {
    const classIdOf = (classFullName: string) => {
      const [schemaName, className] = classFullName.split(".");
      return db.withQueryReader(
        "SELECT c.ECInstanceId FROM meta.ECClassDef c JOIN meta.ECSchemaDef s ON c.Schema.Id = s.ECInstanceId WHERE s.Name = ? AND c.Name = ?",
        (reader) => {
          expect(reader.step()).toBe(true);
          return reader.current[0] as string;
        },
        QueryBinder.from([schemaName, className])
      );
    };

    async function classify(...deleted: DeletedInstance[]) {
      const classKinds = await SourceClassKinds.query(db);
      const batch: DeletionBatch = {
        elements: [],
        models: [],
        relationships: [],
        externalSourceAspects: [],
      };
      for (const instance of deleted)
        addDeletionRecord(batch, instance, classKinds, db);
      return batch;
    }

    it("identifies a relationship deletion missing an endpoint", async () => {
      await expectTransformerError(
        async () =>
          classify({
            ecInstanceId: "0x123",
            ecClassId: classIdOf("BisCore.ElementGroupsMembers"),
          }),
        IModelTransformerError.ChangedInstanceMetadataMissing,
        "Relationship deletion 0x123 is missing an endpoint."
      );
    });

    it("classifies deletions by class rather than owner metadata", async () => {
      const records = await classify(
        // An element row with an owner ID must still be an element.
        {
          ecInstanceId: "0x20",
          ecClassId: classIdOf("BisCore.Subject"),
          elementId: IModel.rootSubjectId,
        },
        // A complete ExternalSourceAspect row without an owner must not become an element.
        {
          ecInstanceId: "0x21",
          ecClassId: classIdOf("BisCore.ExternalSourceAspect"),
          scopeId: IModel.rootSubjectId,
          kind: "Element",
          identifier: "aspect",
        },
        {
          ecInstanceId: "0x22",
          ecClassId: classIdOf("BisCore.DefinitionModel"),
        }
      );
      expect(records).toEqual({
        ...noDeletions,
        elements: [
          {
            ecInstanceId: "0x20",
            ecClassId: classIdOf("BisCore.Subject"),
            federationGuid: undefined,
          },
        ],
        models: [
          {
            ecInstanceId: "0x22",
            ecClassId: classIdOf("BisCore.DefinitionModel"),
          },
        ],
      });
    });

    it.each(["scopeId", "kind", "identifier"] as const)(
      "omits an ExternalSourceAspect deletion without %s",
      async (missingField) => {
        const deleted: DeletedInstance = {
          ecInstanceId: "0x123",
          ecClassId: classIdOf("BisCore.ExternalSourceAspect"),
          elementId: "0x1",
          scopeId: IModel.rootSubjectId,
          kind: "Element",
          identifier: "aspect",
        };
        expect((await classify(deleted)).externalSourceAspects).toHaveLength(1);
        expect(
          await classify({ ...deleted, [missingField]: undefined })
        ).toEqual(noDeletions);
      }
    );

    it("omits an ExternalSourceAspect deletion of a kind other than Element", async () => {
      expect(
        await classify({
          ecInstanceId: "0x123",
          ecClassId: classIdOf("BisCore.ExternalSourceAspect"),
          elementId: "0x1",
          scopeId: IModel.rootSubjectId,
          kind: "Relationship",
          identifier: "aspect",
        })
      ).toEqual(noDeletions);
    });

    it("skips ElementDrivesElement deletions without reading their endpoints", async () => {
      expect(
        await classify({
          ecInstanceId: "0x123",
          ecClassId: classIdOf("BisCore.ElementDrivesElement"),
        })
      ).toEqual(noDeletions);
    });

    it("preserves the error for a class missing from the schema", async () => {
      await expect(
        classify({ ecInstanceId: "0x123", ecClassId: "0xfffffff" })
      ).rejects.toThrow(
        expect.objectContaining({ errorNumber: IModelStatus.NotFound })
      );
    });
  });
});
