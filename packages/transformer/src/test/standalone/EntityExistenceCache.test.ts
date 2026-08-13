/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
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

  it("transforming many elements in the same model issues at most one target model existence query per unique model", async () => {
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

    const createQueryReader = vi.spyOn(targetDb, "createQueryReader");
    const targetEditTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer({
      source: sourceDb,
      target: targetEditTxn,
    });
    await transformer.process();
    targetEditTxn.end();

    const modelExistenceQueries = createQueryReader.mock.calls.filter(
      ([query]) => query.includes("SELECT 1 FROM BisCore:Model")
    );
    // every one of the 20 physical objects references the same model; without the
    // cache this would be one existence query per reference
    expect(modelExistenceQueries.length).to.be.lessThanOrEqual(3);

    createQueryReader.mockRestore();
    transformer.dispose();
    sourceDb.close();
    targetDb.close();
  });
});
