/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ElementOwnsUniqueAspect,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
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
import * as path from "node:path";
import { expect } from "vitest";
import { EntityExistenceCache } from "../../EntityExistenceCache";
import { SourceReferenceValidator } from "../../SourceReferenceValidator";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";

describe("SourceReferenceValidator", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "SourceReferenceValidator"
  );

  beforeAll(() => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir)) {
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    }
    if (!IModelJsFs.existsSync(outputDir)) {
      IModelJsFs.mkdirSync(outputDir);
    }
  });

  function createDb(testFileName: string, objectCount: number) {
    const dbPath = IModelTransformerTestUtils.prepareOutputFile(
      "SourceReferenceValidator",
      testFileName
    );
    const db = SnapshotDb.createEmpty(dbPath, {
      rootSubject: { name: "SourceReferenceValidator" },
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
      const objectIds: Id64String[] = [];
      for (let i = 0; i < objectCount; ++i) {
        objectIds.push(txn.insertElement(physicalObjectProps));
      }
      return { modelId, objectIds };
    });
    return { db, ...ids };
  }

  it("batches element and aspect references across source entities", async () => {
    const { db, objectIds } = createDb("ElementAndAspectReferences.bim", 2);
    await db.importSchemas([
      IModelTransformerTestUtils.getPathToSchemaWithUniqueAspect(),
    ]);
    const aspectId = withEditTxn(db, "insert unique aspect", (txn) => {
      const aspectProps = {
        classFullName: "TestSchema1:MyUniqueAspect",
        element: {
          id: objectIds[0],
          relClassName: ElementOwnsUniqueAspect.classFullName,
        },
        myProp1: "value",
      };
      return txn.insertAspect(aspectProps);
    });
    const cache = new EntityExistenceCache();
    const validator = new SourceReferenceValidator(db, cache, 3);
    const batches: EntityReference[][] = [];
    const existsAll = vi
      .spyOn(cache, "existsAll")
      .mockImplementation(async (queriedDb, references) => {
        const batch = [...references];
        batches.push(batch);
        return EntityExistenceCache.prototype.existsAll.call(
          cache,
          queriedDb,
          batch
        );
      });

    try {
      expect(
        await validator.add([`e${objectIds[0]}`, `a${aspectId}`], objectIds[0])
      ).to.be.undefined;
      expect(existsAll).not.toHaveBeenCalled();

      expect(await validator.add([`e${objectIds[1]}`], objectIds[1])).to.be
        .undefined;
      expect(batches).to.deep.equal([
        [`e${objectIds[0]}`, `a${aspectId}`, `e${objectIds[1]}`],
      ]);
    } finally {
      existsAll.mockRestore();
      db.close();
    }
  });

  it("deduplicates within chunks and flushes the final partial chunk", async () => {
    const { db, objectIds, modelId } = createDb(
      "ChunkBoundariesAndDuplicates.bim",
      3
    );
    const cache = new EntityExistenceCache();
    const validator = new SourceReferenceValidator(db, cache, 2);
    const batches: EntityReference[][] = [];
    const existsAll = vi
      .spyOn(cache, "existsAll")
      .mockImplementation(async (queriedDb, references) => {
        const batch = [...references];
        batches.push(batch);
        return EntityExistenceCache.prototype.existsAll.call(
          cache,
          queriedDb,
          batch
        );
      });

    try {
      await validator.add(
        [`e${objectIds[0]}`, `e${objectIds[0]}`, `e${objectIds[1]}`],
        objectIds[0]
      );
      await validator.add(
        [`m${modelId}`, `m${modelId}`, `e${objectIds[2]}`],
        objectIds[2]
      );
      expect(batches).to.deep.equal([
        [`e${objectIds[0]}`, `e${objectIds[1]}`],
        [`m${modelId}`, `e${objectIds[2]}`],
      ]);

      await validator.add([`e${objectIds[0]}`], objectIds[0]);
      expect(batches).to.have.lengthOf(2);
      await validator.flush();
      expect(batches[2]).to.deep.equal([`e${objectIds[0]}`]);
    } finally {
      existsAll.mockRestore();
      db.close();
    }
  });

  it("reports a real missing reference with its source entity attribution", async () => {
    const { db, objectIds } = createDb("MissingReference.bim", 1);
    const cache = new EntityExistenceCache();
    const validator = new SourceReferenceValidator(db, cache, 2);
    const missingReference: EntityReference = "e0xffffff";

    try {
      expect(
        await validator.add(
          [`e${objectIds[0]}`, missingReference],
          objectIds[0]
        )
      ).to.deep.equal({
        entityId: objectIds[0],
        referenceId: missingReference,
      });
    } finally {
      db.close();
    }
  });

  it("propagates source query failures and retries the retained batch", async () => {
    const { db, objectIds } = createDb("QueryFailure.bim", 1);
    const cache = new EntityExistenceCache();
    const validator = new SourceReferenceValidator(db, cache, 1);
    const queryFailure = new Error("source query failed");
    const batches: EntityReference[][] = [];
    const existsAll = vi
      .spyOn(cache, "existsAll")
      .mockImplementation(async (queriedDb, references) => {
        const batch = [...references];
        batches.push(batch);
        if (batches.length === 1) throw queryFailure;
        return EntityExistenceCache.prototype.existsAll.call(
          cache,
          queriedDb,
          batch
        );
      });
    const reference: EntityReference = `e${objectIds[0]}`;

    try {
      await expect(validator.add([reference], objectIds[0])).rejects.toBe(
        queryFailure
      );

      // Retrying with only already-pending references must query again, not skip validation.
      expect(await validator.add([reference], objectIds[0])).to.be.undefined;
      expect(batches).to.deep.equal([[reference], [reference]]);

      expect(await validator.flush()).to.be.undefined;
      expect(batches).to.have.lengthOf(2);
    } finally {
      existsAll.mockRestore();
      db.close();
    }
  });
});
