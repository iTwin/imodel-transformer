/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EntityReferences,
  IModelDb,
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
} from "../IModelTransformerUtils.js";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations.js";

import { EntityExistenceCache } from "../../EntityExistenceCache.js";
import { EntityUnifier } from "../../EntityUnifier.js";
import { IModelTransformer } from "../../IModelTransformer.js";
import { IModelImporter } from "../../IModelImporter.js";

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

  /** Records the references of `type` that are queried in `db`, whichever query form is used. */
  function spyOnExistenceQueries(db: IModelDb, type: ConcreteEntityTypes) {
    const exists = vi.spyOn(EntityUnifier, "exists");
    const existsAll = vi.spyOn(EntityUnifier, "existsAll");
    const queriedReferences = (): EntityReference[] =>
      [
        ...exists.mock.calls
          .filter(([queriedDb]) => queriedDb === db)
          .map(([, arg]) =>
            "entityReference" in arg
              ? arg.entityReference
              : EntityReferences.from(arg.entity)
          ),
        ...existsAll.mock.calls
          .filter(([queriedDb]) => queriedDb === db)
          .flatMap(([, references]) => [...references]),
      ].filter((reference) => EntityReferences.split(reference)[0] === type);
    const restore = () => {
      exists.mockRestore();
      existsAll.mockRestore();
    };
    return { queriedReferences, restore };
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

  it("skips source existence queries for previously exported models", async () => {
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

    const sourceModelQueries = spyOnExistenceQueries(
      sourceDb,
      ConcreteEntityTypes.Model
    );
    const markExists = vi.spyOn(EntityExistenceCache.prototype, "markExists");
    const targetEditTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer({
      source: sourceDb,
      target: targetEditTxn,
    });
    let processSucceeded = false;
    try {
      await transformer.process();

      // Every physical object references the same model, which was exported from the
      // source before them and is therefore already known to exist.
      expect(sourceModelQueries.queriedReferences()).toHaveLength(0);
      expect(
        markExists.mock.calls.some(
          ([db, reference]) =>
            db === targetDb &&
            EntityReferences.split(reference)[0] === ConcreteEntityTypes.Element
        )
      ).to.be.false;
      processSucceeded = true;
    } finally {
      sourceModelQueries.restore();
      markExists.mockRestore();
      transformer.dispose();
      targetEditTxn.end(processSucceeded ? "save" : "abandon");
      sourceDb.close();
      targetDb.close();
    }
  });

  it("skips source existence queries for previously exported parent elements", async () => {
    const parentCount = 5;
    const childrenPerParent = 4;
    const {
      db: sourceDb,
      categoryId,
      modelId,
      objIds,
    } = createDbWithPhysicalObjects("ParentChildSource.bim", parentCount);
    withEditTxn(sourceDb, "insert child elements", (txn) => {
      for (const parentId of objIds) {
        for (let index = 0; index < childrenPerParent; index++) {
          const childProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            parent: {
              id: parentId,
              relClassName: "BisCore:ElementOwnsChildElements",
            },
          };
          txn.insertElement(childProps);
        }
      }
    });
    const targetDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "EntityExistenceCache",
      "ParentChildTarget.bim"
    );
    const targetDb = SnapshotDb.createEmpty(targetDbPath, {
      rootSubject: { name: "ParentChildTarget" },
    });

    const sourceElementQueries = spyOnExistenceQueries(
      sourceDb,
      ConcreteEntityTypes.Element
    );
    const targetEditTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer({
      source: sourceDb,
      target: targetEditTxn,
    });
    let processSucceeded = false;
    try {
      await transformer.process();

      // Each parent is exported before its children, so no child reference
      // needs a source existence query.
      expect(sourceElementQueries.queriedReferences()).toHaveLength(0);
      expect(
        await targetDb
          .createQueryReader(
            "SELECT count(*) FROM Generic.PhysicalObject WHERE Parent.Id IS NOT NULL",
            undefined,
            { usePrimaryConn: true }
          )
          .toArray()
      ).toEqual([[parentCount * childrenPerParent]]);
      processSucceeded = true;
    } finally {
      sourceElementQueries.restore();
      transformer.dispose();
      targetEditTxn.end(processSucceeded ? "save" : "abandon");
      sourceDb.close();
      targetDb.close();
    }
  });

  it("does not record exported source entities when dangling references are ignored", async () => {
    const elementCount = 5;
    const { db: sourceDb } = createDbWithPhysicalObjects(
      "IgnoreDanglingSource.bim",
      elementCount
    );
    const targetDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "EntityExistenceCache",
      "IgnoreDanglingTarget.bim"
    );
    const targetDb = SnapshotDb.createEmpty(targetDbPath, {
      rootSubject: { name: "IgnoreDanglingTarget" },
    });

    const markExists = vi.spyOn(EntityExistenceCache.prototype, "markExists");
    const targetEditTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer(
      { source: sourceDb, target: targetEditTxn },
      { danglingReferencesBehavior: "ignore" }
    );
    let processSucceeded = false;
    try {
      await transformer.process();

      // "ignore" never checks the source for references, so recording would only cost memory.
      expect(markExists.mock.calls.filter(([db]) => db === sourceDb)).toEqual(
        []
      );
      expect(
        await targetDb
          .createQueryReader(
            "SELECT count(*) FROM Generic.PhysicalObject",
            undefined,
            { usePrimaryConn: true }
          )
          .toArray()
      ).toEqual([[elementCount]]);
      processSucceeded = true;
    } finally {
      markExists.mockRestore();
      transformer.dispose();
      targetEditTxn.end(processSucceeded ? "save" : "abandon");
      sourceDb.close();
      targetDb.close();
    }
  });
});
