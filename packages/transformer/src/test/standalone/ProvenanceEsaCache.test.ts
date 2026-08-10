/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { assert, expect, vi } from "vitest";
import {
  EditTxn,
  ElementGroupsMembers,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import { IModelTransformer } from "../../imodel-transformer";
import { ProvenanceManager } from "../../ProvenanceManager";
import type { SyncType, SyncTypeResolver } from "../../SyncTypeResolver";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

describe("ProvenanceManager scope ESA cache", () => {
  async function count(
    iModelDb: IModelDb,
    classFullName: string
  ): Promise<number> {
    const sql = `SELECT COUNT(*) FROM ${classFullName}`;
    const reader = iModelDb.createQueryReader(sql, undefined, {
      usePrimaryConn: true,
    });
    return (await reader.step()) ? reader.current[0] : 0;
  }

  function createSnapshot(name: string): SnapshotDb {
    const file = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      `${name}.bim`
    );
    return SnapshotDb.createEmpty(file, { rootSubject: { name } });
  }

  function insertElementEsa(
    db: SnapshotDb,
    identifier: string,
    version?: string
  ): Id64String {
    return withEditTxn(db, "insert test esa", (txn) => {
      const aspectProps: ExternalSourceAspectProps = {
        classFullName: ExternalSourceAspect.classFullName,
        element: {
          id: IModel.rootSubjectId,
          relClassName: ElementOwnsExternalSourceAspects.classFullName,
        },
        scope: { id: IModel.rootSubjectId },
        identifier,
        kind: ExternalSourceAspect.Kind.Element,
        version,
      };
      return txn.insertAspect(aspectProps);
    });
  }

  function makeManager(args: {
    sourceDb: IModelDb;
    targetDb: IModelDb;
    syncType: SyncType;
  }): ProvenanceManager {
    const syncTypeResolver = {
      getSyncType: async () => args.syncType,
    } as unknown as SyncTypeResolver;
    return new ProvenanceManager(
      IModel.rootSubjectId,
      {},
      {
        sourceDb: args.sourceDb,
        targetDb: args.targetDb,
        findTargetElementId: (id: Id64String) => id,
      },
      syncTypeResolver,
      new EditTxn(args.targetDb, "ProvenanceEsaCache")
    );
  }

  it("reuses existing element and relationship ESAs via the cache on a second transformation", async () => {
    const sourceDb = createSnapshot("EsaCache-Source");
    withEditTxn(sourceDb, "populate source", (txn) => {
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "EsaCacheCategory",
        {}
      );
      const modelId = PhysicalModel.insert(
        txn,
        IModel.rootSubjectId,
        "EsaCachePhysical"
      );
      const objIds = [1, 2, 3].map((x) => {
        const physicalObjectProps: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: Code.createEmpty(),
          userLabel: `PhysicalObject(${x})`,
        };
        return txn.insertElement(physicalObjectProps);
      });
      txn.insertRelationship({
        classFullName: ElementGroupsMembers.classFullName,
        sourceId: objIds[0],
        targetId: objIds[1],
      });
    });

    const targetDb = createSnapshot("EsaCache-Target");

    const firstTxn = new EditTxn(targetDb, "EsaCache first run");
    firstTxn.start();
    const firstTransformer = new IModelTransformer(
      { source: sourceDb, target: firstTxn },
      { forceExternalSourceAspectProvenance: true }
    );
    await firstTransformer.process();
    firstTxn.saveChanges();
    firstTxn.end();
    firstTransformer.dispose();

    const esaCountAfterFirstRun = await count(
      targetDb,
      ExternalSourceAspect.classFullName
    );
    assert.isAtLeast(esaCountAfterFirstRun, 4); // scope + elements + relationship

    // second run: every element/relationship provenance lookup must be served by
    // the preloaded cache, so the uncached static per-row query must only ever be
    // used for the scope aspect
    const staticQuerySpy = vi.spyOn(
      ProvenanceManager,
      "queryScopeExternalSourceAspect"
    );
    try {
      const secondTxn = new EditTxn(targetDb, "EsaCache second run");
      secondTxn.start();
      const secondTransformer = new IModelTransformer(
        { source: sourceDb, target: secondTxn },
        { forceExternalSourceAspectProvenance: true }
      );
      await secondTransformer.process();
      secondTxn.saveChanges();
      secondTxn.end();
      secondTransformer.dispose();

      expect(staticQuerySpy).toHaveBeenCalled();
      for (const [, aspectProps] of staticQuerySpy.mock.calls) {
        expect(aspectProps.kind).toBe(ExternalSourceAspect.Kind.Scope);
      }
      assert.equal(
        await count(targetDb, ExternalSourceAspect.classFullName),
        esaCountAfterFirstRun,
        "second run must reuse (not duplicate) existing provenance ESAs"
      );
    } finally {
      staticQuerySpy.mockRestore();
    }

    sourceDb.close();
    targetDb.close();
  });

  it("records inserted ESAs so cache misses followed by inserts are found without re-querying", async () => {
    const db = createSnapshot("EsaCache-RecordInserted");
    insertElementEsa(db, "0x111", "v1");

    const manager = makeManager({
      sourceDb: db,
      targetDb: db,
      syncType: "not-sync",
    });

    // first lookup builds the cache from the db
    assert.equal(
      await manager.queryProvenanceForElement("0x111"),
      IModel.rootSubjectId
    );

    // record an aspect that is deliberately NOT in the db: subsequent hits prove
    // that lookups are served from the in-memory cache
    const recordedProps: ExternalSourceAspectProps = {
      id: "0xdead",
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: IModel.rootSubjectId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: IModel.rootSubjectId },
      identifier: "0x222",
      kind: ExternalSourceAspect.Kind.Element,
      version: "v2",
    };
    manager.recordScopeEsa(recordedProps);

    const found = await manager.findScopeEsaForEntity(recordedProps);
    assert.deepEqual(found, {
      aspectId: "0xdead",
      version: "v2",
      jsonProperties: undefined,
    });
    assert.equal(
      await manager.queryProvenanceForElement("0x222"),
      IModel.rootSubjectId
    );
    // first-match semantics: the pre-existing aspect keeps the element→identifier slot
    assert.equal(
      await manager.queryScopeEsaIdentifierByElementId(IModel.rootSubjectId),
      "0x111"
    );

    db.close();
  });

  it("resolves provenance from the source db during a reverse synchronization", async () => {
    const sourceDb = createSnapshot("EsaCache-ReverseSource");
    const targetDb = createSnapshot("EsaCache-ReverseTarget");
    insertElementEsa(sourceDb, "0x333");

    const reverseManager = makeManager({
      sourceDb,
      targetDb,
      syncType: "reverse",
    });
    assert.equal(
      await reverseManager.queryProvenanceForElement("0x333"),
      IModel.rootSubjectId,
      "reverse sync must read provenance from the source db"
    );

    const forwardManager = makeManager({
      sourceDb,
      targetDb,
      syncType: "forward",
    });
    assert.isUndefined(
      await forwardManager.queryProvenanceForElement("0x333"),
      "forward sync must read provenance from the target db, which has no ESAs"
    );

    sourceDb.close();
    targetDb.close();
  });

  it("falls back to per-row queries when the cache is disabled by the size limit", async () => {
    const savedEnv = process.env.IMODEL_TRANSFORMER_MAX_ESA_CACHE_SIZE;
    process.env.IMODEL_TRANSFORMER_MAX_ESA_CACHE_SIZE = "0";
    const db = createSnapshot("EsaCache-Disabled");
    try {
      const aspectId = insertElementEsa(db, "0x444", "v4");
      const manager = makeManager({
        sourceDb: db,
        targetDb: db,
        syncType: "not-sync",
      });

      const staticQuerySpy = vi.spyOn(
        ProvenanceManager,
        "queryScopeExternalSourceAspect"
      );
      try {
        assert.equal(
          await manager.queryProvenanceForElement("0x444"),
          IModel.rootSubjectId
        );
        const found = await manager.findScopeEsaForEntity({
          classFullName: ExternalSourceAspect.classFullName,
          element: { id: IModel.rootSubjectId },
          scope: { id: IModel.rootSubjectId },
          identifier: "0x444",
          kind: ExternalSourceAspect.Kind.Element,
        });
        assert.equal(found?.aspectId, aspectId);
        expect(staticQuerySpy).toHaveBeenCalledTimes(1);
        assert.equal(
          await manager.queryScopeEsaIdentifierByElementId(
            IModel.rootSubjectId
          ),
          "0x444"
        );
      } finally {
        staticQuerySpy.mockRestore();
      }
    } finally {
      if (savedEnv === undefined)
        delete process.env.IMODEL_TRANSFORMER_MAX_ESA_CACHE_SIZE;
      else process.env.IMODEL_TRANSFORMER_MAX_ESA_CACHE_SIZE = savedEnv;
      db.close();
    }
  });
});
