/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EntityReferences,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import {
  Code,
  ConcreteEntityTypes,
  EntityReference,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { expect } from "vitest";
import * as path from "node:path";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";

import { EntityExistenceCache } from "../../EntityExistenceCache";
import { EntityUnifier } from "../../EntityUnifier";
import { IModelTransformer } from "../../IModelTransformer";
import { IModelImporter } from "../../IModelImporter";

describe("EntityExistenceCache", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "EntityExistenceCache"
  );

  beforeAll(async () => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir)) {
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    }
    if (!IModelJsFs.existsSync(outputDir)) {
      IModelJsFs.mkdirSync(outputDir);
    }
  });

  function createDbWithPhysicalObjects(testFileName: string, objCount = 1) {
    const dbPath = IModelTransformerTestUtils.prepareOutputFile(
      "EntityExistenceCache",
      testFileName
    );
    const db = SnapshotDb.createEmpty(dbPath, {
      rootSubject: { name: "EntityExistenceCache" },
    });
    const ids = withEditTxn(db, "insert physical objects", (txn) => {
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "SpatialCategory",
        new SubCategoryAppearance()
      );
      const modelId = PhysicalModel.insert(
        txn,
        IModel.rootSubjectId,
        "PhysicalModel"
      );
      const physicalObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
      };
      const objIds: Id64String[] = [];
      for (let i = 0; i < objCount; ++i)
        objIds.push(txn.insertElement(physicalObjectProps));
      return { categoryId, modelId, objIds };
    });
    return { db, ...ids };
  }

  it("caches positive results so repeat checks don't re-query", async () => {
    const { db, objIds } = createDbWithPhysicalObjects("PositiveCache.bim");
    const cache = new EntityExistenceCache();
    const elemRef: EntityReference = `e${objIds[0]}`;

    const createQueryReader = vi.spyOn(db, "createQueryReader");
    expect(await cache.exists(db, elemRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(1);
    expect(await cache.exists(db, elemRef)).to.be.true;
    expect(await cache.exists(db, elemRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(1);

    createQueryReader.mockRestore();
    db.close();
  });

  it("batches checks, caches positives, and retries negatives", async () => {
    const { db, objIds, modelId } = createDbWithPhysicalObjects(
      "CacheExistsAll.bim",
      2
    );
    const cache = new EntityExistenceCache();
    const existingReferences: EntityReference[] = [
      `e${objIds[0]}`,
      `e${objIds[1]}`,
      `m${modelId}`,
    ];
    const missingReference: EntityReference = "e0xffffff";
    const references = [...existingReferences, missingReference];
    const createQueryReader = vi.spyOn(db, "createQueryReader");

    try {
      const found = await cache.existsAll(db, references);
      expect(found).to.deep.equal(new Set(existingReferences));
      expect(createQueryReader).toHaveBeenCalledTimes(2);

      const cachedFound = await cache.existsAll(db, references);
      expect(cachedFound).to.deep.equal(new Set(existingReferences));
      expect(createQueryReader).toHaveBeenCalledTimes(3);
    } finally {
      createQueryReader.mockRestore();
      db.close();
    }
  });

  it("retains positives discovered by concurrent batch checks", async () => {
    const { db, objIds } = createDbWithPhysicalObjects(
      "ConcurrentBatchChecks.bim",
      2
    );
    const cache = new EntityExistenceCache();
    const references: EntityReference[] = objIds.map(
      (id): EntityReference => `e${id}`
    );
    const createQueryReader = vi.spyOn(db, "createQueryReader");

    try {
      await Promise.all(
        references.map(async (reference) => cache.existsAll(db, [reference]))
      );
      const queryCountAfterConcurrentChecks =
        createQueryReader.mock.calls.length;

      await cache.existsAll(db, references);
      expect(createQueryReader).toHaveBeenCalledTimes(
        queryCountAfterConcurrentChecks
      );
    } finally {
      createQueryReader.mockRestore();
      db.close();
    }
  });

  it("does not cache negative results and finds the entity once it is inserted", async () => {
    const { db } = createDbWithPhysicalObjects("NegativeNotCached.bim");
    const cache = new EntityExistenceCache();

    const partitionId = withEditTxn(db, "insert partition", (txn) =>
      txn.insertElement({
        classFullName: PhysicalPartition.classFullName,
        model: IModel.repositoryModelId,
        parent: {
          id: IModel.rootSubjectId,
          relClassName: "BisCore:SubjectOwnsPartitionElements",
        },
        code: PhysicalPartition.createCode(
          db,
          IModel.rootSubjectId,
          "LatePhysicalModel"
        ),
      })
    );
    const modelRef: EntityReference = `m${partitionId}`;

    const createQueryReader = vi.spyOn(db, "createQueryReader");
    // the submodeled element exists but its submodel doesn't yet
    expect(await cache.exists(db, modelRef)).to.be.false;
    expect(await cache.exists(db, modelRef)).to.be.false;
    expect(createQueryReader).toHaveBeenCalledTimes(2); // negatives are re-queried

    withEditTxn(db, "insert submodel", (txn) =>
      txn.insertModel({
        classFullName: PhysicalModel.classFullName,
        modeledElement: { id: partitionId },
      })
    );

    expect(await cache.exists(db, modelRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(3);
    expect(await cache.exists(db, modelRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(3); // now cached

    createQueryReader.mockRestore();
    db.close();
  });

  it("markExists avoids querying; invalidate and clearDb force a re-query", async () => {
    const { db, objIds } = createDbWithPhysicalObjects("Invalidation.bim");
    const cache = new EntityExistenceCache();
    const elemRef: EntityReference = `e${objIds[0]}`;

    const createQueryReader = vi.spyOn(db, "createQueryReader");
    cache.markExists(db, elemRef);
    expect(await cache.exists(db, elemRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(0);

    cache.invalidate(db, elemRef);
    expect(await cache.exists(db, elemRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(1);

    cache.clearDb(db);
    expect(await cache.exists(db, elemRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(2);

    cache.clear();
    expect(await cache.exists(db, elemRef)).to.be.true;
    expect(createQueryReader).toHaveBeenCalledTimes(3);

    createQueryReader.mockRestore();
    db.close();
  });

  it("invalidates cached elements when the importer deletes and reimports them", async () => {
    const { db, objIds } = createDbWithPhysicalObjects(
      "ImporterElementInvalidation.bim"
    );
    const elementId = objIds[0];
    const elementProps = db.elements.getElement(elementId).toJSON();
    const elementRef = EntityReferences.fromEntityType(
      elementId,
      ConcreteEntityTypes.Element
    );
    const editTxn = createStartedEditTxn(db);
    const importer = new IModelImporter(editTxn, {
      preserveElementIdsForFiltering: true,
    });
    const cache = new EntityExistenceCache();
    importer.registerEntityExistenceCache(cache);
    const clearDb = vi.spyOn(cache, "clearDb");

    try {
      expect(await cache.exists(db, elementRef)).to.be.true;

      await importer.deleteElement(elementId);
      expect(clearDb).toHaveBeenCalledWith(db);
      expect(await cache.exists(db, elementRef)).to.be.false;

      await importer.importElement(elementProps);
      expect(await cache.exists(db, elementRef)).to.be.true;
    } finally {
      importer.unregisterEntityExistenceCache(cache);
      clearDb.mockRestore();
      editTxn.end("abandon");
      db.close();
    }
  });

  it("invalidates cached models when the importer deletes and reimports them", async () => {
    const { db, modelId } = createDbWithPhysicalObjects(
      "ImporterModelInvalidation.bim",
      0
    );
    const modelProps = db.models.getModel(modelId).toJSON();
    const modelRef = EntityReferences.fromEntityType(
      modelId,
      ConcreteEntityTypes.Model
    );
    const editTxn = createStartedEditTxn(db);
    const importer = new IModelImporter(editTxn);
    const cache = new EntityExistenceCache();
    importer.registerEntityExistenceCache(cache);
    const invalidate = vi.spyOn(cache, "invalidate");

    try {
      expect(await cache.exists(db, modelRef)).to.be.true;

      await importer.deleteModel(modelId);
      expect(invalidate).toHaveBeenCalledWith(db, modelRef);
      expect(await cache.exists(db, modelRef)).to.be.false;

      await importer.importModel(modelProps);
      expect(await cache.exists(db, modelRef)).to.be.true;
    } finally {
      importer.unregisterEntityExistenceCache(cache);
      invalidate.mockRestore();
      editTxn.end("abandon");
      db.close();
    }
  });

  it("marks imported elements for same-iModel transformations", async () => {
    const { db, objIds } = createDbWithPhysicalObjects(
      "SameIModelElementMarking.bim"
    );
    const editTxn = createStartedEditTxn(db);
    const transformer = new IModelTransformer(
      { source: db, target: editTxn },
      { noProvenance: true }
    );
    const markExists = vi.spyOn(EntityExistenceCache.prototype, "markExists");

    try {
      await transformer.processElement(objIds[0]);
      expect(
        markExists.mock.calls.some(
          ([, reference]) =>
            EntityReferences.split(reference)[0] === ConcreteEntityTypes.Element
        )
      ).to.be.true;
    } finally {
      markExists.mockRestore();
      transformer.dispose();
      editTxn.end("abandon");
      db.close();
    }
  });

  it("EntityUnifier.existsAll batches checks with one query per entity type", async () => {
    const { db, objIds, modelId } = createDbWithPhysicalObjects(
      "ExistsAll.bim",
      3
    );
    const missingElemRef: EntityReference = "e0xffffff";
    const refs: EntityReference[] = [
      ...objIds.map((id): EntityReference => `e${id}`),
      `m${modelId}`,
      missingElemRef,
    ];

    const createQueryReader = vi.spyOn(db, "createQueryReader");
    const found = await EntityUnifier.existsAll(db, refs);
    // one query for all elements, one for the model
    expect(createQueryReader).toHaveBeenCalledTimes(2);
    for (const id of objIds) expect(found.has(`e${id}`)).to.be.true;
    expect(found.has(`m${modelId}`)).to.be.true;
    expect(found.has(missingElemRef)).to.be.false;

    createQueryReader.mockRestore();
    db.close();
  });

  it("batches repeated source model existence checks", async () => {
    const elementCount = 20;
    const { db: sourceDb } = createDbWithPhysicalObjects(
      "ManyElementsSource.bim",
      elementCount
    );
    const targetDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "EntityExistenceCache",
      "ManyElementsTarget.bim"
    );
    const targetDb = SnapshotDb.createEmpty(targetDbPath, {
      rootSubject: { name: "ManyElementsTarget" },
    });

    const createQueryReader = vi.spyOn(sourceDb, "createQueryReader");
    const markExists = vi.spyOn(EntityExistenceCache.prototype, "markExists");
    const targetEditTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer({
      source: sourceDb,
      target: targetEditTxn,
    });
    let processSucceeded = false;
    try {
      await transformer.process();

      const normalizeQuery = (query: string) =>
        query.replace(/\s+/g, " ").trim().toLowerCase();
      const modelExistenceQueries = createQueryReader.mock.calls.filter(
        ([query]) => {
          const normalizedQuery = normalizeQuery(query);
          return (
            normalizedQuery.includes("from biscore:model") &&
            normalizedQuery.includes("invirtualset(:ids, ecinstanceid)")
          );
        }
      );
      const individualModelExistenceQueries =
        createQueryReader.mock.calls.filter(([query]) => {
          const normalizedQuery = normalizeQuery(query);
          return (
            normalizedQuery.includes("from biscore:model") &&
            /where ecinstanceid\s*=\s*:id\b/.test(normalizedQuery)
          );
        });

      // Every one of the 20 physical objects references the same model. The
      // source batch check should query that model type once, not once per object.
      expect(modelExistenceQueries).toHaveLength(1);
      expect(individualModelExistenceQueries).toHaveLength(0);
      expect(
        markExists.mock.calls.some(
          ([, reference]) =>
            EntityReferences.split(reference)[0] === ConcreteEntityTypes.Element
        )
      ).to.be.false;
      processSucceeded = true;
    } finally {
      createQueryReader.mockRestore();
      markExists.mockRestore();
      transformer.dispose();
      targetEditTxn.end(processSucceeded ? "save" : "abandon");
      sourceDb.close();
      targetDb.close();
    }
  });
});
