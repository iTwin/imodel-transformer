/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as path from "node:path";
import {
  ChangesetReader,
  EditTxn,
  ElementGroupsMembers,
  ExternalSourceAspect,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import {
  ChangesetFileProps,
  ExternalSourceAspectProps,
  IModel,
  QueryBinder,
} from "@itwin/core-common";
import { ChangesetScanner, DeletionRecords } from "../../ChangesetScanner";
import { IModelTransformerError } from "../../IModelTransformerError";
import { expectTransformerError } from "../IModelTransformerUtils";
import { ChangedInstanceIds } from "../../IModelExporter";
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

  const noDeletions: DeletionRecords = {
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
      expect(await ChangesetScanner.scan(db, [files], ids)).toEqual([
        noDeletions,
      ]);
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
    const expected: DeletionRecords = {
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
          classFullName: ElementGroupsMembers.classFullName,
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
      const records = await ChangesetScanner.scan(db, [files], ids);
      expect(records).toEqual([expected]);
      expect([...ids.aspectOwnerElementIds]).toEqual([seed.owner]);
      expect([...ids.aspect.deleteIds]).toEqual([seed.aspect]);
      expect([...ids.relationship.deleteIds]).toEqual([seed.relationship]);
      expect([...ids.element.deleteIds]).toEqual([seed.element]);
    });
  });

  it("collects deletions without populating changed IDs or querying current owners", async () => {
    const seed = withEditTxn(db, "create deletion-only fixture", (txn) => {
      const owner = Subject.insert(txn, IModel.rootSubjectId, "owner");
      return { owner, aspect: insertAspect(txn, owner) };
    });
    const classId = classIdFor(ExternalSourceAspect.classFullName, seed.aspect);
    await withEditTxn(db, "delete aspect", async (txn) => {
      txn.deleteAspect(seed.aspect);
      const ids = new ChangedInstanceIds(db);
      const querySpy = vi.spyOn(db, "withQueryReader");
      const records = await ChangesetScanner.scan(db, [files], ids, {
        populateChangedInstanceIds: false,
      });
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
      expect(ids.hasChanges).toBe(false);
      expect([...ids.aspectOwnerElementIds]).toEqual([]);
      expect(querySpy).not.toHaveBeenCalled();
    });
  });

  it("preserves ChangesetReader errors", async () => {
    const readerError = new Error("reader failed");
    vi.spyOn(ChangesetReader, "openFile").mockImplementation(() => {
      throw readerError;
    });
    await expect(
      ChangesetScanner.scan(db, [files], new ChangedInstanceIds(db))
    ).rejects.toBe(readerError);
  });

  describe("deletion classification", () => {
    const classIds = {
      elements: new Set<string>(),
      models: new Set<string>(),
      relationships: new Set(["0x456"]),
      relationshipsToSkip: new Set<string>(),
    };

    it("identifies a relationship deletion missing an endpoint", async () => {
      await expectTransformerError(
        async () =>
          ChangesetScanner["toDeletionRecords"](
            db,
            [{ ecInstanceId: "0x123", ecClassId: "0x456" }],
            classIds
          ),
        IModelTransformerError.ChangedInstanceMetadataMissing,
        "Relationship deletion 0x123 is missing an endpoint."
      );
    });

    it("omits an ExternalSourceAspect deletion without an identifier", () => {
      const aspectClassId = db.withQueryReader(
        "SELECT ECInstanceId FROM meta.ECClassDef WHERE Name='ExternalSourceAspect'",
        (reader) => {
          expect(reader.step()).toBe(true);
          return reader.current[0] as string;
        }
      );
      const row = {
        ecInstanceId: "0x123",
        ecClassId: aspectClassId,
        elementId: "0x1",
        scopeId: IModel.rootSubjectId,
        kind: "Element",
      };
      expect(
        ChangesetScanner["toDeletionRecords"](db, [row], classIds)
      ).toEqual(noDeletions);
      expect(
        ChangesetScanner["toDeletionRecords"](
          db,
          [{ ...row, identifier: "aspect" }],
          classIds
        ).externalSourceAspects
      ).toHaveLength(1);
    });
  });
});
