/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Guid, Id64String } from "@itwin/core-bentley";
import { CodeScopeSpec, CodeSpec, IModel } from "@itwin/core-common";
import { describe, expect, it, onTestFinished, vi } from "vitest";
import type { ElementReservationPreScanMode } from "../../ElementReservationPreScan";
import { IModelTransformer } from "../../IModelTransformer";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
  TransformerExtensiveTestScenario,
} from "../IModelTransformerUtils";
import { useLocalReservations } from "./ElementReservationTestUtils";

const outputFile = (name: string) =>
  IModelTransformerTestUtils.prepareOutputFile(
    "ElementReservationPreScan",
    name
  );

const modes: ElementReservationPreScanMode[] = ["transform", "identity"];

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
      return { ...transformer.elementReservationPreScanStats! };
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
  it.each(modes)(
    "inserts every new element of a full transformation with its reserved id (%s)",
    async (mode) => {
      const sourceDb = adopt(
        await createExtensiveSource(`extensive-source-${mode}`),
        (db) => db.close()
      );
      const targetDb = createDb(`extensive-target-${mode}`);
      await importSourceSchemas(sourceDb, targetDb);
      const reservations = adopt(
        await useLocalReservations(
          targetDb,
          outputFile(`extensive-sync-${mode}.db`)
        ),
        (adapter) => adapter.dispose()
      );
      const reserve = vi.spyOn(targetDb.reservations, "reserveElements");

      const stats = await transform(sourceDb, targetDb, {
        elementReservationPreScan: mode,
      });

      expect(reserve).toHaveBeenCalledTimes(1);
      expect(stats.identityLoads).toBe(mode === "identity");
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
    }
  );

  it.each(modes)(
    "reserves nothing when every element already exists in the target (%s)",
    async (mode) => {
      const sourceDb = adopt(
        await createExtensiveSource(`rerun-source-${mode}`),
        (db) => db.close()
      );
      const targetDb = adopt(createDb(`rerun-target-${mode}`), (db) =>
        db.close()
      );
      await importSourceSchemas(sourceDb, targetDb);

      const first = await transform(sourceDb, targetDb, {
        elementReservationPreScan: mode,
      });
      expect(first.submitted).toBe(false);
      expect(first.insertsWithFederationGuid).toBeGreaterThan(0);

      const second = await transform(sourceDb, targetDb, {
        elementReservationPreScan: mode,
      });
      expect(second.insertsWithFederationGuid).toBe(0);
      expect(second.insertsWithoutFederationGuid).toBe(0);
      expect(second.classified).toBe(first.classified);
    }
  );

  it.each(modes)(
    "reserves children of a new code scope with the scope's federation GUID (%s)",
    async (mode) => {
      const sourceDb = adopt(createDb(`scope-source-${mode}`), (db) =>
        db.close()
      );
      const targetDb = createDb(`scope-target-${mode}`);
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
        await useLocalReservations(
          targetDb,
          outputFile(`scope-sync-${mode}.db`)
        ),
        (adapter) => adapter.dispose()
      );

      const stats = await transform(sourceDb, targetDb, {
        elementReservationPreScan: mode,
      });

      expect(stats.insertsWithFederationGuid).toBe(5);
      expect(stats.newScopeCodes).toBe(4);
      const parentGuid = sourceDb.elements.getElement(
        ids.parent
      ).federationGuid!;
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
    }
  );

  it("auto mode clones only when the transform is customized", async () => {
    const sourceDb = adopt(createDb("auto-source"), (db) => db.close());
    const plainTarget = adopt(createDb("auto-plain-target"), (db) =>
      db.close()
    );
    const plain = await transform(sourceDb, plainTarget, {});
    expect(plain.mode).toBe("identity");
    expect(plain.identityLoads).toBe(true);

    const filteredTarget = adopt(createDb("auto-filtered-target"), (db) =>
      db.close()
    );
    const filtered = await transform(
      sourceDb,
      filteredTarget,
      {},
      (transformer) => {
        transformer.shouldExportElement = async () => true;
      }
    );
    expect(filtered.mode).toBe("identity");
    expect(filtered.identityLoads).toBe(false);

    class CustomTransformer extends IModelTransformer {
      public override async onTransformElement(sourceElement: Element) {
        return super.onTransformElement(sourceElement);
      }
    }
    const customTarget = adopt(createDb("auto-custom-target"), (db) =>
      db.close()
    );
    const txn = createStartedEditTxn(customTarget);
    const custom = new CustomTransformer({ source: sourceDb, target: txn });
    try {
      await custom.process();
      expect(custom.elementReservationPreScanStats?.mode).toBe("transform");
    } finally {
      custom.dispose();
      txn.end();
    }
  });

  it("does not run when disabled", async () => {
    const sourceDb = adopt(createDb("off-source"), (db) => db.close());
    const targetDb = adopt(createDb("off-target"), (db) => db.close());
    const txn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer(
      { source: sourceDb, target: txn },
      { elementReservationPreScan: "off" }
    );
    await transformer.process();
    expect(transformer.elementReservationPreScanStats).toBeUndefined();
    transformer.dispose();
    txn.end();
  });
});
