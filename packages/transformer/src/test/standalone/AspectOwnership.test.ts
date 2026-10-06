/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
  EditTxn,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  ExternalSourceAspect,
  IModelDb,
  SnapshotDb,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { AccessToken, Guid, GuidString, Id64String } from "@itwin/core-bentley";
import { ElementAspectProps, IModel } from "@itwin/core-common";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer";
import { elementAspectRecordProperty } from "../../ElementAspectOwnership";
import {
  AspectCountingImporter,
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import * as TestUtils from "../TestUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";

const schemaName = "AspectOwnership";
const recordKey = elementAspectRecordProperty("0x1");
const isRecordProperty = (prop: { namespace: string; name: string }) =>
  prop.namespace === recordKey.namespace && prop.name === recordKey.name;
const uniqueA = `${schemaName}:UniqueA`;
const multiA = `${schemaName}:MultiA`;
const schemaXml = `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaName}" alias="ao" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="UniqueA">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
  <ECEntityClass typeName="MultiA">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
</ECSchema>`;

let dbCount = 0;
async function createDb(name: string): Promise<SnapshotDb> {
  const db = SnapshotDb.createEmpty(
    IModelTransformerTestUtils.prepareOutputFile(
      "AspectOwnership",
      `${name}-${dbCount++}.bim`
    ),
    { rootSubject: { name } }
  );
  await db.importSchemaStrings([schemaXml]);
  withEditTxn(db, "save schema", () => {});
  return db;
}

function addAspect(
  txn: EditTxn,
  classFullName: string,
  ownerId: Id64String,
  value: string
): Id64String {
  return txn.insertAspect({
    classFullName,
    element:
      classFullName === uniqueA
        ? new ElementOwnsUniqueAspect(ownerId)
        : new ElementOwnsMultiAspects(ownerId),
    value,
  } as ElementAspectProps);
}

/** Inserts a SpatialCategory named "shared" (matched by Code across sources) with the given aspects. */
async function createSource(
  name: string,
  aspects: Array<[string, string]>
): Promise<{
  db: SnapshotDb;
  categoryId: Id64String;
  aspectIds: Id64String[];
}> {
  const db = await createDb(name);
  return withEditTxn(db, "seed", (txn) => {
    const categoryId = SpatialCategory.insert(
      txn,
      IModel.dictionaryId,
      "shared",
      {}
    );
    const aspectIds = aspects.map(([cls, value]) =>
      addAspect(txn, cls, categoryId, value)
    );
    return { db, categoryId, aspectIds };
  });
}

interface Writes {
  aspects: { inserted: number; updated: number; deleted: number };
  records: { inserted: number; updated: number; deleted: number };
}

/** Runs one transformation and counts its content aspect writes and its ownership record writes. */
async function run(
  source: IModelDb,
  target: IModelDb,
  options: IModelTransformOptions
): Promise<Writes> {
  const txn = createStartedEditTxn(target);
  const importer = new AspectCountingImporter(txn);
  const records = { inserted: 0, updated: 0, deleted: 0 };
  // Records are file properties; deleteFileProperty goes through saveFileProperty.
  const saveProperty = txn.saveFileProperty.bind(txn);
  vi.spyOn(txn, "saveFileProperty").mockImplementation(
    (prop: any, value: any, blob: any) => {
      if (isRecordProperty(prop)) {
        if (value === undefined) records.deleted++;
        else if (target.queryFilePropertyString(prop) === undefined)
          records.inserted++;
        else records.updated++;
      }
      return saveProperty(prop, value, blob);
    }
  );
  try {
    const transformer = new IModelTransformer(
      { source, target: importer },
      options
    );
    await transformer.process();
    transformer.dispose();
    txn.end();
  } catch (error) {
    txn.end("abandon");
    throw error;
  }
  return {
    aspects: {
      inserted: importer.numElementAspectsInserted,
      updated: importer.numElementAspectsUpdated,
      deleted: importer.numElementAspectsDeleted,
    },
    records,
  };
}

const noWrites = {
  aspects: { inserted: 0, updated: 0, deleted: 0 },
  records: { inserted: 0, updated: 0, deleted: 0 },
};

/** Test-schema aspects of a target element as `class=value` with IDs, sorted by value. */
function readAspects(db: IModelDb, elementId: Id64String) {
  return db.elements
    .getAspects(elementId)
    .filter((a) => a.classFullName.startsWith(`${schemaName}:`))
    .map((a) => ({
      id: a.id,
      key: `${a.classFullName.split(":")[1]}=${a.asAny.value}`,
    }))
    .sort((a, b) => a.key.localeCompare(b.key));
}
const keys = (db: IModelDb, id: Id64String) =>
  readAspects(db, id).map((a) => a.key);

interface AspectRecord {
  sourceIModel: GuidString;
  sourceAspect: Id64String;
}

/** Ownership records of the element's aspects: target aspect ID -> record. */
function readRecords(
  db: IModelDb,
  elementId: Id64String
): Map<Id64String, AspectRecord> {
  const records = new Map<Id64String, AspectRecord>();
  for (const a of db.elements.getAspects(elementId)) {
    const value = db.queryFilePropertyString(elementAspectRecordProperty(a.id));
    if (value !== undefined) records.set(a.id, JSON.parse(value));
  }
  return records;
}

/** All ownership records in the iModel, including ones whose aspect no longer exists. */
function countAllRecords(db: IModelDb): number {
  return db.withSqliteStatement(
    "SELECT count(*) FROM be_Prop WHERE Namespace=? AND Name=?",
    (statement) => {
      statement.bindString(1, recordKey.namespace);
      statement.bindString(2, recordKey.name);
      statement.step();
      return statement.getValue(0).getInteger();
    }
  );
}

/** Simulates a target written before records existed. */
function dropRecords(db: IModelDb, elementId: Id64String): void {
  withEditTxn(db, "drop records", (txn) => {
    for (const a of db.elements.getAspects(elementId))
      txn.deleteFileProperty(elementAspectRecordProperty(a.id));
  });
}

async function createTarget(): Promise<{
  db: SnapshotDb;
  scope1: Id64String;
  scope2: Id64String;
}> {
  const db = await createDb("Target");
  const { scope1, scope2 } = withEditTxn(db, "scopes", (txn) => ({
    scope1: Subject.insert(txn, IModel.rootSubjectId, "scope1"),
    scope2: Subject.insert(txn, IModel.rootSubjectId, "scope2"),
  }));
  return { db, scope1, scope2 };
}

const sharedCategory = (db: IModelDb) =>
  SpatialCategory.queryCategoryIdByName(db, IModel.dictionaryId, "shared")!;

describe("ElementAspect ownership", () => {
  it("1. keeps S1's aspects when S2 maps to the same element and has none", async () => {
    const s1 = await createSource("S1", [
      [multiA, "s1-a"],
      [multiA, "s1-b"],
      [uniqueA, "s1-u"],
    ]);
    const s2 = await createSource("S2", []);
    const t = await createTarget();

    const first = await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    expect(first.aspects.inserted).to.equal(3);
    expect(first.records.inserted).to.equal(3);
    const cat = sharedCategory(t.db);
    const before = readAspects(t.db, cat);

    expect(
      await run(s2.db, t.db, { targetScopeElementId: t.scope2 })
    ).to.deep.equal(noWrites);
    expect(readAspects(t.db, cat)).to.deep.equal(before);

    // 5. Unchanged reruns write nothing, for either source.
    expect(
      await run(s1.db, t.db, { targetScopeElementId: t.scope1 })
    ).to.deep.equal(noWrites);
    expect(
      await run(s2.db, t.db, { targetScopeElementId: t.scope2 })
    ).to.deep.equal(noWrites);
    expect(readAspects(t.db, cat)).to.deep.equal(before);
  });

  it("2. keeps both sources' multi-aspects of one class; the first source keeps a shared unique slot", async () => {
    const s1 = await createSource("S1", [
      [multiA, "s1-a"],
      [uniqueA, "s1-u"],
    ]);
    const s2 = await createSource("S2", [
      [multiA, "s2-a"],
      [uniqueA, "s2-u"],
    ]);
    const t = await createTarget();
    await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    const cat = sharedCategory(t.db);

    const second = await run(s2.db, t.db, { targetScopeElementId: t.scope2 });
    expect(second.aspects).to.deep.equal({
      inserted: 1,
      updated: 0,
      deleted: 0,
    });
    expect(keys(t.db, cat)).to.deep.equal([
      "MultiA=s1-a",
      "MultiA=s2-a",
      "UniqueA=s1-u",
    ]);

    expect(
      await run(s1.db, t.db, { targetScopeElementId: t.scope1 })
    ).to.deep.equal(noWrites);
    expect(
      await run(s2.db, t.db, { targetScopeElementId: t.scope2 })
    ).to.deep.equal(noWrites);
    expect(keys(t.db, cat)).to.deep.equal([
      "MultiA=s1-a",
      "MultiA=s2-a",
      "UniqueA=s1-u",
    ]);
  });

  it("3. applies S1's later updates and deletes only to S1's copies (full rerun)", async () => {
    const s1 = await createSource("S1", [
      [multiA, "same"],
      [multiA, "s1-b"],
      [uniqueA, "s1-u"],
    ]);
    // S2 has an identical aspect, which content matching cannot tell apart.
    const s2 = await createSource("S2", [[multiA, "same"]]);
    const t = await createTarget();
    await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    await run(s2.db, t.db, { targetScopeElementId: t.scope2 });
    const cat = sharedCategory(t.db);
    const s2Copy = readAspects(t.db, cat).filter(
      (a) => a.key === "MultiA=same"
    );
    expect(s2Copy.length).to.equal(2);

    withEditTxn(s1.db, "change S1", (txn) => {
      txn.deleteAspect(s1.aspectIds[0]);
      txn.updateAspect({
        ...s1.db.elements.getAspect(s1.aspectIds[1]).toJSON(),
        value: "s1-b2",
      } as ElementAspectProps);
      txn.deleteAspect(s1.aspectIds[2]);
    });
    const writes = await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    expect(writes.aspects).to.deep.equal({
      inserted: 0,
      updated: 1,
      deleted: 2,
    });
    expect(writes.records).to.deep.equal({
      inserted: 0,
      updated: 0,
      deleted: 2,
    });
    expect(keys(t.db, cat)).to.deep.equal(["MultiA=s1-b2", "MultiA=same"]);
    expect(
      await run(s2.db, t.db, { targetScopeElementId: t.scope2 })
    ).to.deep.equal(noWrites);
    expect(
      await run(s1.db, t.db, { targetScopeElementId: t.scope1 })
    ).to.deep.equal(noWrites);
  });

  it("4. imports one source in parts with different target scopes without duplicates", async () => {
    const s = await createSource("S", [
      [multiA, "a"],
      [multiA, "b"],
      [uniqueA, "u"],
    ]);
    const t = await createTarget();
    await run(s.db, t.db, { targetScopeElementId: t.scope1 });
    const cat = sharedCategory(t.db);
    const before = readAspects(t.db, cat);

    expect(
      await run(s.db, t.db, { targetScopeElementId: t.scope2 })
    ).to.deep.equal(noWrites);
    expect(readAspects(t.db, cat)).to.deep.equal(before);

    // A later delete, seen first by part 2, is applied once and not undone by part 1.
    withEditTxn(s.db, "delete", (txn) => txn.deleteAspect(s.aspectIds[0]));
    const part2 = await run(s.db, t.db, { targetScopeElementId: t.scope2 });
    expect(part2.aspects.deleted).to.equal(1);
    expect(
      await run(s.db, t.db, { targetScopeElementId: t.scope1 })
    ).to.deep.equal(noWrites);
    expect(keys(t.db, cat)).to.deep.equal(["MultiA=b", "UniqueA=u"]);
  });

  it("6. adopts a copied target's aspects without aspect writes on the first sync", async () => {
    const s = await createSource("S", [
      [multiA, "a"],
      [multiA, "b"],
      [uniqueA, "u"],
    ]);
    const forkFile = IModelTransformerTestUtils.prepareOutputFile(
      "AspectOwnership",
      `Fork-${dbCount++}.bim`
    );
    const fork = SnapshotDb.createFrom(s.db, forkFile);
    const before = readAspects(fork, s.categoryId);

    const first = await run(s.db, fork, {
      wasSourceIModelCopiedToTarget: true,
    });
    expect(first.aspects).to.deep.equal({
      inserted: 0,
      updated: 0,
      deleted: 0,
    });
    expect(first.records.inserted).to.equal(3);
    expect(readAspects(fork, s.categoryId)).to.deep.equal(before);
    expect(await run(s.db, fork, {})).to.deep.equal(noWrites);
  });

  it("7. adopts a pre-existing target's unrecorded aspects once, then tracks them", async () => {
    const s1 = await createSource("S1", [
      [multiA, "a"],
      [uniqueA, "u"],
    ]);
    const s2 = await createSource("S2", []);
    const t = await createTarget();
    await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    const cat = sharedCategory(t.db);
    const before = readAspects(t.db, cat);
    dropRecords(t.db, cat);

    const adopt = await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    expect(adopt.aspects).to.deep.equal({
      inserted: 0,
      updated: 0,
      deleted: 0,
    });
    expect(adopt.records.inserted).to.equal(2);
    expect(readAspects(t.db, cat)).to.deep.equal(before);
    // From now on S2 can't touch them.
    expect(
      await run(s2.db, t.db, { targetScopeElementId: t.scope2 })
    ).to.deep.equal(noWrites);
    expect(readAspects(t.db, cat)).to.deep.equal(before);
  });

  it("7b. a pre-existing shared target loses unrecorded aspects if the other source runs first", async () => {
    const s1 = await createSource("S1", [[multiA, "a"]]);
    const s2 = await createSource("S2", []);
    const t = await createTarget();
    await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    const cat = sharedCategory(t.db);
    dropRecords(t.db, cat);
    // Same as #447 today: unrecorded aspects look like S2's own leftovers.
    const writes = await run(s2.db, t.db, { targetScopeElementId: t.scope2 });
    expect(writes.aspects.deleted).to.equal(1);
    // S1's next run puts it back and records it.
    await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    expect(keys(t.db, cat)).to.deep.equal(["MultiA=a"]);
  });

  it("8. does not copy a source's own records with includeSourceProvenance", async () => {
    const s0 = await createSource("S0", [
      [multiA, "a"],
      [uniqueA, "u"],
    ]);
    const mid = await createDb("Mid");
    await run(s0.db, mid, {});
    const midCat = sharedCategory(mid);
    expect(readRecords(mid, midCat).size).to.equal(2);

    const t = await createTarget();
    const options = {
      includeSourceProvenance: true,
      targetScopeElementId: t.scope1,
    };
    await run(mid, t.db, options);
    const cat = sharedCategory(t.db);
    expect(keys(t.db, cat)).to.deep.equal(["MultiA=a", "UniqueA=u"]);
    // Only this hop's records: one per copied content aspect, none copied from Mid.
    const records = [...readRecords(t.db, cat).values()];
    expect(records.length).to.equal(2);
    expect(records.every((r) => r.sourceIModel === mid.iModelId)).to.equal(
      true
    );
    expect(await run(mid, t.db, options)).to.deep.equal(noWrites);
  });

  it("9. a fork of a target syncs its source's updates; inherited upstream records are dropped", async () => {
    // S has records of its own upstream import from U.
    const u = await createSource("U", [
      [multiA, "a"],
      [uniqueA, "u"],
    ]);
    const s = await createDb("S");
    await run(u.db, s, {});
    const sCat = sharedCategory(s);
    expect(readRecords(s, sCat).size).to.equal(2);

    const fork = SnapshotDb.createFrom(
      s,
      IModelTransformerTestUtils.prepareOutputFile(
        "AspectOwnership",
        `Fork-${dbCount++}.bim`
      )
    );
    // The fork inherits S's scope for U on the root subject, so S needs its own scope.
    const fromS = withEditTxn(fork, "scope", (txn) =>
      Subject.insert(txn, IModel.rootSubjectId, "fromS")
    );
    const first = await run(s, fork, {
      wasSourceIModelCopiedToTarget: true,
      targetScopeElementId: fromS,
    });
    expect(first.aspects).to.deep.equal({
      inserted: 0,
      updated: 0,
      deleted: 0,
    });
    expect(
      [...readRecords(fork, sCat).values()].every(
        (r) => r.sourceIModel === s.iModelId
      )
    ).to.equal(true);

    // Without dropping U's records, S would treat every forked aspect as foreign.
    withEditTxn(s, "edit", (txn) => {
      for (const aspect of s.elements.getAspects(sCat))
        if (aspect.classFullName.startsWith(`${schemaName}:`))
          txn.updateAspect({ ...aspect.toJSON(), value: "edited" } as any);
    });
    const sync = await run(s, fork, { targetScopeElementId: fromS });
    expect(sync.aspects.updated).to.equal(2);
    expect(keys(fork, sCat)).to.deep.equal(["MultiA=edited", "UniqueA=edited"]);
  });

  it("11. keeps each source's element provenance on a shared element without a FederationGuid", async () => {
    // Without a FederationGuid, each source's provenance of the shared category
    // is an Element ExternalSourceAspect scoped to that source's target scope.
    const s1 = await createSource("S1", [[multiA, "a"]]);
    const s2 = await createSource("S2", []);
    for (const s of [s1, s2])
      withEditTxn(s.db, "drop FederationGuid", (txn) =>
        txn.updateElement({
          ...s.db.elements.getElementProps(s.categoryId),
          federationGuid: Guid.empty,
        })
      );
    const t = await createTarget();
    const options = (targetScopeElementId: Id64String) => ({
      targetScopeElementId,
      includeSourceProvenance: true,
    });
    const provenanceScopes = (ownerId: Id64String) =>
      (
        t.db.elements.getAspects(
          ownerId,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[]
      )
        .filter((a) => a.kind === ExternalSourceAspect.Kind.Element)
        .map((a) => a.scope?.id)
        .sort();

    await run(s1.db, t.db, options(t.scope1));
    const cat = sharedCategory(t.db);
    await run(s2.db, t.db, options(t.scope2));
    expect(provenanceScopes(cat)).to.deep.equal([t.scope1, t.scope2].sort());
    expect(keys(t.db, cat)).to.deep.equal(["MultiA=a"]);

    expect(await run(s1.db, t.db, options(t.scope1))).to.deep.equal(noWrites);
    expect(provenanceScopes(cat)).to.deep.equal([t.scope1, t.scope2].sort());
  });

  it("10. removes records whose target aspect no longer exists", async () => {
    const s1 = await createSource("S1", [
      [multiA, "a"],
      [multiA, "b"],
    ]);
    const s2 = await createSource("S2", []);
    const t = await createTarget();
    await run(s1.db, t.db, { targetScopeElementId: t.scope1 });
    const cat = sharedCategory(t.db);
    expect(countAllRecords(t.db)).to.equal(2);

    // Deleted outside the transformer; the next run of any source drops its record.
    withEditTxn(t.db, "delete one", (txn) =>
      txn.deleteAspect(readAspects(t.db, cat)[0].id)
    );
    const writes = await run(s2.db, t.db, { targetScopeElementId: t.scope2 });
    expect(writes.records.deleted).to.equal(1);
    expect(countAllRecords(t.db)).to.equal(1);
    expect(keys(t.db, cat)).to.deep.equal(["MultiA=b"]);
  });
});

describe("ElementAspect ownership with change processing", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let restoreCheckpointDownload: (() => void) | undefined;

  beforeAll(async () => {
    transformerTestHub.start("AspectOwnership", KnownTestLocations.outputDir);
    iTwinId = transformerTestHub.iTwinId;
    accessToken = await HubWrappers.getAccessToken(
      TestUtils.TestUserType.Regular
    );
  });
  beforeEach(() => {
    restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
  });
  afterEach(() => {
    restoreCheckpointDownload?.();
    restoreCheckpointDownload = undefined;
  });
  afterAll(() => transformerTestHub.stop());

  async function createHubIModel(
    name: string,
    seed: (db: SnapshotDb) => void | Promise<void>
  ): Promise<BriefcaseDb> {
    const db = await createDb(name);
    await seed(db);
    const version0 = db.pathName;
    db.close();
    const iModelId = await transformerTestHub.createNewIModel({
      iTwinId,
      iModelName: `${name}-${dbCount++}`,
      version0,
      noLocks: true,
    });
    return HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId,
    });
  }

  it("3. processChanges applies S1's update and delete only to S1's copies after S2 ran", async () => {
    const seedSource = (aspects: Array<[string, string]>) => (db: SnapshotDb) =>
      withEditTxn(db, "seed", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "shared",
          {}
        );
        for (const [cls, value] of aspects)
          addAspect(txn, cls, categoryId, value);
      });
    const s1 = await createHubIModel(
      "S1",
      seedSource([
        [multiA, "same"],
        [multiA, "s1-b"],
        [uniqueA, "s1-u"],
      ])
    );
    const s2 = await createHubIModel("S2", seedSource([[multiA, "same"]]));
    const t = await createHubIModel("T", (db) => {
      withEditTxn(db, "scopes", (txn) => {
        Subject.insert(txn, IModel.rootSubjectId, "scope1");
        Subject.insert(txn, IModel.rootSubjectId, "scope2");
      });
    });
    const scope = (name: string) =>
      t.elements.getElement({
        code: Subject.createCode(t, IModel.rootSubjectId, name),
      }).id;
    const scope1 = scope("scope1");
    const scope2 = scope("scope2");
    const push = async (db: BriefcaseDb, description: string) =>
      db.pushChanges({ accessToken, description });

    await run(s1, t, { targetScopeElementId: scope1 });
    await push(t, "S1 processAll");
    await run(s2, t, { targetScopeElementId: scope2 });
    await push(t, "S2 processAll");
    const cat = sharedCategory(t);
    expect(keys(t, cat)).to.deep.equal([
      "MultiA=s1-b",
      "MultiA=same",
      "MultiA=same",
      "UniqueA=s1-u",
    ]);
    const s2Same = readAspects(t, cat).filter((a) => a.key === "MultiA=same");

    const startChangeset = { index: s1.changeset.index! + 1 };
    const s1Aspects = s1.elements.getAspects(sharedCategory(s1));
    const byValue = (v: string) => s1Aspects.find((a) => a.asAny.value === v)!;
    withEditTxn(s1, "change S1 aspects", (txn) => {
      txn.deleteAspect(byValue("same").id);
      txn.updateAspect({
        ...byValue("s1-b").toJSON(),
        value: "s1-b2",
      } as ElementAspectProps);
      txn.deleteAspect(byValue("s1-u").id);
    });
    await push(s1, "change S1 aspects");

    const writes = await run(s1, t, {
      targetScopeElementId: scope1,
      argsForProcessChanges: { startChangeset },
    });
    expect(writes.aspects).to.deep.equal({
      inserted: 0,
      updated: 1,
      deleted: 2,
    });
    expect(writes.records.deleted).to.equal(2);
    expect(keys(t, cat)).to.deep.equal(["MultiA=s1-b2", "MultiA=same"]);
    // The surviving "same" is S2's copy, not S1's.
    const remaining = readAspects(t, cat).find((a) => a.key === "MultiA=same")!;
    const records = readRecords(t, cat);
    expect(records.get(remaining.id)?.sourceIModel).to.equal(s2.iModelId);
    expect(s2Same.map((a) => a.id)).to.include(remaining.id);

    for (const db of [s1, s2, t]) db.close();
  });
});
