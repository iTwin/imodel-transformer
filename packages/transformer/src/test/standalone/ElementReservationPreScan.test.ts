/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  GeometryPart,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SpatialCategory,
  StandaloneDb,
  Subject,
  SubjectOwnsPartitionElements,
  SubjectOwnsSubjects,
  withEditTxn,
} from "@itwin/core-backend";
import { Guid, Id64String } from "@itwin/core-bentley";
import {
  Code,
  CodeScopeSpec,
  CodeSpec,
  GeometryPartProps,
  IModel,
  PhysicalElementProps,
  RelatedElement,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import { describe, expect, it, onTestFinished, vi } from "vitest";
import { IModelTransformer } from "../../IModelTransformer.js";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
  TransformerExtensiveTestScenario,
} from "../IModelTransformerUtils.js";
import { useLocalReservations } from "./ElementReservationTestUtils.js";

const outputFile = (name: string) =>
  IModelTransformerTestUtils.prepareOutputFile(
    "ElementReservationPreScan",
    name
  );

/** Runs the pre-scan on a target that does not need reservations. */
const force = (transformer: IModelTransformer) => {
  transformer.forceElementReservationPreScan = true;
};

/** Releases `resource` when the current test finishes. */
function adopt<T>(resource: T, release: (resource: T) => void): T {
  onTestFinished(() => release(resource));
  return resource;
}

function createDb(name: string): StandaloneDb {
  return StandaloneDb.createEmpty(outputFile(`${name}.bim`), {
    rootSubject: { name },
  });
}

async function createExtensiveSource(name: string): Promise<StandaloneDb> {
  const db = createDb(name);
  await withEditTxn(db, "prepare and populate", async () => {
    await TransformerExtensiveTestScenario.prepareDb(db);
    await TransformerExtensiveTestScenario.populateDb(db);
  });
  return db;
}

/** Imports the source schemas before the target is connected to reservations, which would require the schema lock. */
async function importSourceSchemas(sourceDb: IModelDb, targetDb: IModelDb) {
  const txn = createStartedEditTxn(targetDb);
  const transformer = new IModelTransformer({ source: sourceDb, target: txn });
  try {
    await transformer.processSchemas();
    txn.saveChanges();
  } finally {
    transformer.dispose();
    txn.end();
  }
}

async function transform(
  sourceDb: IModelDb,
  targetDb: IModelDb,
  options: ConstructorParameters<typeof IModelTransformer>[1],
  configure?: (transformer: IModelTransformer) => void
) {
  const txn = createStartedEditTxn(targetDb);
  try {
    const transformer = new IModelTransformer(
      { source: sourceDb, target: txn },
      options
    );
    try {
      configure?.(transformer);
      await transformer.process();
      txn.saveChanges();
      const stats = transformer.elementReservationPreScanStats;
      return stats === undefined ? undefined : { ...stats };
    } finally {
      transformer.dispose();
    }
  } finally {
    txn.end();
  }
}

/** Source elements with a federation GUID and their target element ids. */
function getTransformedGuids(sourceDb: IModelDb, targetDb: IModelDb) {
  const targetIds = new Map<string, Id64String>();
  sourceDb.withPreparedSqliteStatement(
    "SELECT FederationGuid FROM bis_Element WHERE FederationGuid IS NOT NULL",
    (stmt) => {
      while (stmt.nextRow()) {
        const guid = stmt.getValueGuid(0);
        const targetId = targetDb.elements.getIdFromFederationGuid(guid);
        if (targetId !== undefined) targetIds.set(guid, targetId);
      }
    }
  );
  return targetIds;
}

describe("element reservation pre-scan", () => {
  it("inserts every new element of a full transformation with its reserved id", async () => {
    const sourceDb = adopt(
      await createExtensiveSource("extensive-source"),
      (db) => db.close()
    );
    const targetDb = createDb("extensive-target");
    await importSourceSchemas(sourceDb, targetDb);
    const reservations = adopt(
      await useLocalReservations(targetDb, outputFile("extensive-sync.db")),
      (adapter) => adapter.dispose()
    );
    const reserve = vi.spyOn(targetDb.reservations, "reserveElements");

    const stats = (await transform(sourceDb, targetDb, {}))!;

    expect(reserve).toHaveBeenCalledTimes(1);
    expect(stats.identityLoads).toBe(true);
    expect(stats.submitted).toBe(true);
    expect(stats.unreservableScopes).toBe(0);
    expect(stats.insertsWithFederationGuid).toBeGreaterThan(0);
    const targetIds = getTransformedGuids(sourceDb, targetDb);
    let reserved = 0;
    for (const [guid, targetId] of targetIds) {
      const reservation = reservations.sync.findReservedElement(guid);
      if (reservation === undefined) continue;
      reserved++;
      expect(targetId).toBe(reservation.elementId);
    }
    expect(reserved).toBe(stats.insertsWithFederationGuid);
  });

  it("reserves nothing when every element already exists in the target", async () => {
    const sourceDb = adopt(await createExtensiveSource("rerun-source"), (db) =>
      db.close()
    );
    const targetDb = adopt(createDb("rerun-target"), (db) => db.close());
    await importSourceSchemas(sourceDb, targetDb);

    const first = (await transform(sourceDb, targetDb, {}, force))!;
    expect(first.submitted).toBe(false);
    expect(first.insertsWithFederationGuid).toBeGreaterThan(0);

    const second = (await transform(sourceDb, targetDb, {}, force))!;
    expect(second.insertsWithFederationGuid).toBe(0);
    expect(second.insertsWithoutFederationGuid).toBe(0);
    expect(second.classified).toBe(first.classified);
  });

  it("reserves children of a new code scope with the scope's federation GUID", async () => {
    const sourceDb = adopt(createDb("scope-source"), (db) => db.close());
    const targetDb = createDb("scope-target");
    const specName = "ParentScoped";
    const ids = withEditTxn(sourceDb, "hierarchy", (txn) => {
      const spec = sourceDb.codeSpecs.insert(
        txn,
        CodeSpec.create(sourceDb, specName, CodeScopeSpec.Type.ParentElement)
      );
      const insertSubject = (scopeId: string, value: string) =>
        txn.insertElement({
          ...Subject.create(sourceDb, scopeId, value).toJSON(),
          federationGuid: Guid.createValue(),
          code: { spec, scope: scopeId, value },
        });
      const parent = insertSubject(IModel.rootSubjectId, "parent");
      return {
        parent,
        children: Array.from({ length: 4 }, (_, i) =>
          insertSubject(parent, `child-${i}`)
        ),
      };
    });
    const reservations = adopt(
      await useLocalReservations(targetDb, outputFile("scope-sync.db")),
      (adapter) => adapter.dispose()
    );

    const stats = (await transform(sourceDb, targetDb, {}))!;

    expect(stats.insertsWithFederationGuid).toBe(5);
    expect(stats.newScopeCodes).toBe(4);
    const parentGuid = sourceDb.elements.getElement(ids.parent).federationGuid!;
    const targetParentId =
      reservations.sync.findReservedElement(parentGuid)!.elementId;
    for (const childId of ids.children) {
      const guid = sourceDb.elements.getElement(childId).federationGuid!;
      const reservation = reservations.sync.findReservedElement(guid)!;
      expect(reservation.code.scope).toBe(parentGuid);
      const targetChild = targetDb.elements.getElement<Element>(
        reservation.elementId
      );
      expect(targetChild.code.scope).toBe(targetParentId);
    }
  });

  it("matches the code of an element inserted earlier in the same run", async () => {
    const sourceDb = adopt(createDb("planned-code-source"), (db) => db.close());
    const targetDb = createDb("planned-code-target");
    const sharedSpecName = "Shared";
    const specNames = ["SpecA", "SpecB", "SpecC"];
    const ids = withEditTxn(
      sourceDb,
      "elements with one target code",
      (txn) => {
        const [specA, specB, specC] = specNames.map((name) =>
          sourceDb.codeSpecs.insert(
            txn,
            CodeSpec.create(sourceDb, name, CodeScopeSpec.Type.Repository)
          )
        );
        const insert = (
          classFullName: string,
          parent: RelatedElement,
          spec: Id64String,
          value: string
        ) =>
          txn.insertElement({
            classFullName,
            model: IModel.repositoryModelId,
            parent,
            federationGuid: Guid.createValue(),
            code: { spec, scope: IModel.rootSubjectId, value },
          });
        return {
          first: insert(
            Subject.classFullName,
            new SubjectOwnsSubjects(IModel.rootSubjectId),
            specA,
            "dup"
          ),
          // The target's code index compares values without ASCII case.
          sameClass: insert(
            Subject.classFullName,
            new SubjectOwnsSubjects(IModel.rootSubjectId),
            specB,
            "DUP"
          ),
          otherClass: insert(
            PhysicalPartition.classFullName,
            new SubjectOwnsPartitionElements(IModel.rootSubjectId),
            specC,
            "dup"
          ),
        };
      }
    );
    const sharedSpecId = withEditTxn(targetDb, "shared code spec", (txn) =>
      targetDb.codeSpecs.insert(
        txn,
        CodeSpec.create(targetDb, sharedSpecName, CodeScopeSpec.Type.Repository)
      )
    );
    const reservations = adopt(
      await useLocalReservations(targetDb, outputFile("planned-code-sync.db")),
      (adapter) => adapter.dispose()
    );
    const guidOf = (id: Id64String) =>
      sourceDb.elements.getElement(id).federationGuid!;

    const stats = (await transform(sourceDb, targetDb, {}, (transformer) => {
      for (const name of specNames)
        transformer.context.remapCodeSpec(name, sharedSpecName);
    }))!;

    expect(stats.matchedByPlannedCode).toBe(1);
    expect(stats.insertsWithFederationGuid).toBe(2);
    expect(
      reservations.sync.findReservedElement(guidOf(ids.sameClass))
    ).toBeUndefined();
    const firstId = reservations.sync.findReservedElement(
      guidOf(ids.first)
    )!.elementId;
    expect(
      targetDb.elements.queryElementIdByCode(
        new Code({
          spec: sharedSpecId,
          scope: IModel.rootSubjectId,
          value: "dup",
        })
      )
    ).toBe(firstId);
    const otherClass = reservations.sync.findReservedElement(
      guidOf(ids.otherClass)
    )!;
    expect(otherClass.code.value ?? "").toBe("");
    expect(targetDb.elements.getElement(otherClass.elementId).code.value).toBe(
      ""
    );
  });

  it("runs by default only when the target uses server-based reservations", async () => {
    const sourceDb = adopt(createDb("auto-source"), (db) => db.close());
    const plainTarget = adopt(createDb("auto-plain-target"), (db) =>
      db.close()
    );
    expect(await transform(sourceDb, plainTarget, {})).toBeUndefined();

    const reservedTarget = createDb("auto-reserved-target");
    adopt(
      await useLocalReservations(
        reservedTarget,
        outputFile("auto-reserved-sync.db")
      ),
      (adapter) => adapter.dispose()
    );
    expect(await transform(sourceDb, reservedTarget, {})).toBeDefined();
  });

  it("loads only the identity unless a filter or change check is customized", async () => {
    const sourceDb = adopt(createDb("identity-source"), (db) => db.close());
    const plainTarget = adopt(createDb("identity-plain-target"), (db) =>
      db.close()
    );
    const plain = (await transform(sourceDb, plainTarget, {}, force))!;
    expect(plain.identityLoads).toBe(true);

    const filteredTarget = adopt(createDb("identity-filtered-target"), (db) =>
      db.close()
    );
    const filtered = (await transform(
      sourceDb,
      filteredTarget,
      {},
      (transformer) => {
        force(transformer);
        transformer.shouldExportElement = async () => true;
      }
    ))!;
    expect(filtered.identityLoads).toBe(false);
  });

  it("does not run when the target holds the schema lock", async () => {
    const sourceDb = adopt(createDb("lock-source"), (db) => db.close());
    const targetDb = createDb("lock-target");
    adopt(
      await useLocalReservations(targetDb, outputFile("lock-sync.db")),
      (adapter) => adapter.dispose()
    );
    vi.spyOn(targetDb, "holdsSchemaLock", "get").mockReturnValue(true);
    const reserve = vi.spyOn(targetDb.reservations, "reserveElements");

    expect(await transform(sourceDb, targetDb, {})).toBeUndefined();
    expect(reserve).not.toHaveBeenCalled();
  });

  describe("geometry", () => {
    /** A source with a PhysicalObject whose geometry stream references a GeometryPart. */
    function createSourceWithGeometry(name: string) {
      const db = adopt(createDb(name), (sourceDb) => sourceDb.close());
      const ids = withEditTxn(db, "insert geometry", (txn) => {
        const category = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Category",
          new SubCategoryAppearance()
        );
        const model = PhysicalModel.insert(txn, IModel.rootSubjectId, "Model");
        const partProps: GeometryPartProps = {
          classFullName: GeometryPart.classFullName,
          model: IModel.dictionaryId,
          code: GeometryPart.createCode(db, IModel.dictionaryId, "Part"),
          federationGuid: Guid.createValue(),
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
        };
        const part = txn.insertElement(partProps);
        const objectProps: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model,
          category,
          code: Code.createEmpty(),
          federationGuid: Guid.createValue(),
          geom: IModelTransformerTestUtils.createBox(
            Point3d.create(1, 1, 1),
            undefined,
            undefined,
            undefined,
            part
          ),
        };
        const object = txn.insertElement(objectProps);
        return { category, model, object };
      });
      return { db, ...ids };
    }

    it("does not run the native clone on a geometry stream", async () => {
      const source = createSourceWithGeometry("native-clone-source");
      const targetDb = createDb("native-clone-target");
      // The target already has the model and category, so only the geometry stream stands in the way of the native clone.
      const targetIds = withEditTxn(targetDb, "model and category", (txn) => ({
        category: SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Category",
          new SubCategoryAppearance()
        ),
        model: PhysicalModel.insert(txn, IModel.rootSubjectId, "Model"),
      }));
      adopt(
        await useLocalReservations(
          targetDb,
          outputFile("native-clone-sync.db")
        ),
        (adapter) => adapter.dispose()
      );
      const countParts = () =>
        targetDb.withPreparedSqliteStatement(
          "SELECT COUNT(*) FROM bis_Element e JOIN ec_Class c ON c.Id = e.ECClassId WHERE c.Name = 'GeometryPart'",
          (stmt) => (stmt.nextRow() ? stmt.getValueInteger(0) : 0)
        );
      const reserve = targetDb.reservations.reserveElements.bind(
        targetDb.reservations
      );
      let partsWhenReserving: number | undefined;
      vi.spyOn(targetDb.reservations, "reserveElements").mockImplementation(
        async (args) => {
          partsWhenReserving = countParts();
          return reserve(args);
        }
      );

      const stats = (await transform(source.db, targetDb, {}, (transformer) => {
        transformer.context.remapElement(source.category, targetIds.category);
        transformer.context.remapElement(source.model, targetIds.model);
      }))!;

      expect(stats.submitted).toBe(true);
      expect(partsWhenReserving).toBe(0);
      expect(countParts()).toBe(1);
      const objectGuid = source.db.elements.getElement(
        source.object
      ).federationGuid!;
      expect(
        targetDb.elements.getIdFromFederationGuid(objectGuid)
      ).toBeDefined();
    });

    it("gives a custom filter the geometry that the transformation loads", async () => {
      const source = createSourceWithGeometry("filter-geometry-source");
      const targetDb = adopt(createDb("filter-geometry-target"), (db) =>
        db.close()
      );
      const hadGeometry: boolean[] = [];

      await transform(
        source.db,
        targetDb,
        { loadSourceGeometry: true },
        (transformer) => {
          force(transformer);
          transformer.shouldExportElement = async (element) => {
            if (element instanceof PhysicalObject)
              hadGeometry.push(element.geom !== undefined);
            return true;
          };
        }
      );

      expect(hadGeometry).toEqual([true, true]);
    });
  });
});
