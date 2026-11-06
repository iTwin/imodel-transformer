/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";
import {
  EditTxn,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64, Id64String } from "@itwin/core-bentley";
import { IModel, RelationshipProps } from "@itwin/core-common";
import { IModelImporter } from "../../IModelImporter.js";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";

const testRelClass = "TestImporterRelSchema:TestRel";
const otherRelClass = "TestImporterRelSchema:OtherTestRel";

interface TestRelationshipProps extends RelationshipProps {
  label?: string;
}

describe("IModelImporter relationships", () => {
  let targetDb: StandaloneDb;
  let editTxn: EditTxn;
  let importer: IModelImporter;
  let elementIds: Id64String[];

  beforeEach(async (context) => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      `Relationships-${context.task.id}.bim`
    );
    targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "ImporterRelationships" },
    });
    await targetDb.importSchemaStrings([
      `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestImporterRelSchema" alias="tirs" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECRelationshipClass typeName="TestRel" strength="referencing" modifier="None">
    <BaseClass>bis:ElementRefersToElements</BaseClass>
    <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true"><Class class="bis:Element"/></Source>
    <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true"><Class class="bis:Element"/></Target>
    <ECProperty propertyName="Label" typeName="string"/>
  </ECRelationshipClass>
  <ECRelationshipClass typeName="OtherTestRel" strength="referencing" modifier="None">
    <BaseClass>bis:ElementRefersToElements</BaseClass>
    <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true"><Class class="bis:Element"/></Source>
    <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true"><Class class="bis:Element"/></Target>
  </ECRelationshipClass>
</ECSchema>`,
    ]);
    elementIds = withEditTxn(targetDb, "insert endpoints", (txn) =>
      ["A", "B", "C", "D"].map((name) =>
        Subject.create(targetDb, IModel.rootSubjectId, name).insert(txn)
      )
    );
    editTxn = createStartedEditTxn(targetDb);
    importer = new IModelImporter(editTxn);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    if (editTxn.isActive) editTxn.end("abandon");
    targetDb.close();
  });

  const relProps = (
    classFullName: string,
    sourceId: Id64String,
    targetId: Id64String,
    label?: string
  ): TestRelationshipProps => ({
    classFullName,
    sourceId,
    targetId,
    ...(label === undefined ? {} : { label }),
  });

  const insertRelationship = (
    classFullName: string,
    sourceId: Id64String,
    targetId: Id64String,
    label?: string
  ): Id64String =>
    editTxn.insertRelationship(
      relProps(classFullName, sourceId, targetId, label)
    );

  const countRelationships = async (classFullName: string) => {
    const rows = await targetDb
      .createQueryReader(`SELECT COUNT(*) FROM ${classFullName}`, undefined, {
        usePrimaryConn: true,
      })
      .toArray();
    return rows[0][0] as number;
  };

  const getLabel = (id: Id64String) =>
    targetDb.relationships.getInstanceProps<TestRelationshipProps>(
      testRelClass,
      id
    ).label;

  describe("importRelationships", () => {
    it("inserts once and keeps the last item's properties for repeated new endpoints", async () => {
      const [a, b] = elementIds;
      const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");

      const ids = await importer.importRelationships([
        relProps(testRelClass, a, b, "first"),
        relProps(testRelClass, Id64.invalid, b, "invalid"),
        relProps(testRelClass, a, b, "second"),
      ]);

      expect(ids).to.have.length(3);
      expect(Id64.isValidId64(ids[0])).to.be.true;
      expect(ids[1]).to.equal(Id64.invalid);
      expect(ids[2]).to.equal(ids[0]);
      expect(await countRelationships(testRelClass)).to.equal(1);
      expect(getLabel(ids[0])).to.equal("second");
      expect(
        tryGetInstance,
        "only the repeated endpoint pair falls back to a single lookup"
      ).toHaveBeenCalledTimes(1);
    });

    it("updates the existing target relationship with the last item's properties for repeated endpoints", async () => {
      const [a, b] = elementIds;
      const existingId = insertRelationship(testRelClass, a, b, "original");

      // The later item matches the prefetched state, so a stale read would skip its update.
      const ids = await importer.importRelationships([
        relProps(testRelClass, a, b, "changed"),
        relProps(testRelClass, a, b, "original"),
      ]);

      expect(ids).to.deep.equal([existingId, existingId]);
      expect(await countRelationships(testRelClass)).to.equal(1);
      expect(getLabel(existingId)).to.equal("original");
    });

    it("uses one batched read per relationship class for distinct endpoint pairs", async () => {
      const [a, b, c, d] = elementIds;
      const existingId = insertRelationship(testRelClass, a, b, "original");
      const queryInstances = vi.spyOn(targetDb.relationships, "queryInstances");
      const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");

      const ids = await importer.importRelationships([
        relProps(testRelClass, a, b, "updated"),
        relProps(otherRelClass, a, c),
        relProps(testRelClass, c, d, "new"),
        relProps(otherRelClass, b, d),
      ]);

      expect(queryInstances).toHaveBeenCalledTimes(2);
      expect(
        queryInstances.mock.calls.map(([classFullName]) => classFullName)
      ).to.have.members([testRelClass, otherRelClass]);
      expect(tryGetInstance).not.toHaveBeenCalled();
      expect(ids[0]).to.equal(existingId);
      expect(getLabel(existingId)).to.equal("updated");
      expect(new Set(ids).size).to.equal(4);
      expect(await countRelationships(testRelClass)).to.equal(2);
      expect(await countRelationships(otherRelClass)).to.equal(2);
    });
  });

  describe("deleteRelationships", () => {
    it("does nothing for empty input", async () => {
      const bulkDelete = vi.spyOn(editTxn, "deleteRelationships");

      await importer.deleteRelationships([]);

      expect(bulkDelete).not.toHaveBeenCalled();
    });

    it("deletes deduplicated relationships of mixed classes in one call and skips missing ones", async () => {
      const [a, b, c] = elementIds;
      const testRelId = insertRelationship(testRelClass, a, b);
      const otherRelId = insertRelationship(otherRelClass, a, c);
      const keptId = insertRelationship(testRelClass, b, c);
      const missingId = insertRelationship(testRelClass, c, a);
      editTxn.deleteRelationships([
        { id: missingId, classFullName: testRelClass } as RelationshipProps,
      ]);
      const bulkDelete = vi.spyOn(editTxn, "deleteRelationships");

      await importer.deleteRelationships([
        { id: testRelId, classFullName: testRelClass },
        { id: otherRelId, classFullName: otherRelClass },
        { id: testRelId, classFullName: testRelClass },
        { id: missingId, classFullName: testRelClass },
      ]);

      expect(bulkDelete).toHaveBeenCalledTimes(1);
      expect(bulkDelete.mock.calls[0][0].map(({ id }) => id)).to.deep.equal([
        testRelId,
        otherRelId,
        missingId,
      ]);
      expect(targetDb.relationships.tryGetInstance(testRelClass, testRelId)).to
        .be.undefined;
      expect(targetDb.relationships.tryGetInstance(otherRelClass, otherRelId))
        .to.be.undefined;
      expect(targetDb.relationships.tryGetInstance(testRelClass, keptId)).to.not
        .be.undefined;
    });

    it("propagates a bulk delete failure without reporting progress, and abandoning the transaction restores deletions", async () => {
      const [a, b] = elementIds;
      const relId = insertRelationship(testRelClass, a, b);
      editTxn.saveChanges();
      importer.progressInterval = 1;
      const onProgress = vi.spyOn(importer, "onProgress");
      const failure = new Error("bulk delete failed");
      const originalDelete = editTxn.deleteRelationships.bind(editTxn);
      vi.spyOn(editTxn, "deleteRelationships").mockImplementationOnce(
        (props) => {
          originalDelete(props);
          throw failure;
        }
      );

      await expect(
        importer.deleteRelationships([
          { id: relId, classFullName: testRelClass },
        ])
      ).rejects.toBe(failure);

      expect(onProgress).not.toHaveBeenCalled();
      editTxn.abandonChanges();
      expect(targetDb.relationships.tryGetInstance(testRelClass, relId)).to.not
        .be.undefined;
    });
  });
});
