/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect } from "vitest";
import * as path from "node:path";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
  ChangesetReader,
  EditTxn,
  ElementOwnsExternalSourceAspects,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  ExternalSourceAspect,
  IModelDb,
  IModelJsFs,
  SnapshotDb,
  withEditTxn,
} from "@itwin/core-backend";
import { AccessToken, GuidString, Id64String } from "@itwin/core-bentley";
import {
  ElementAspectProps,
  ExternalSourceAspectProps,
  IModel,
  QueryBinder,
} from "@itwin/core-common";
import {
  IModelExporter,
  IModelTransformer,
  IModelTransformOptions,
} from "../../imodel-transformer";
import { HubWrappers } from "../IModelTransformerUtils";
import * as TestUtils from "../TestUtils";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";
import {
  deleted,
  populateTimelineSeed,
  runTimeline,
  Timeline,
  TimelineIModelState,
  TimelineStateChange,
} from "../TestUtils/TimelineTestUtil";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";

const schemaName = "AspectSync";
const uniqueA = `${schemaName}:UniqueA`;

const schemaXml = `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaName}" alias="as" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="UniqueA">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
  <ECEntityClass typeName="MultiA">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
  <ECEntityClass typeName="MultiB">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
</ECSchema>`;

const isTestAspectClass = (classFullName: string) =>
  classFullName.startsWith(`${schemaName}:`) ||
  classFullName.startsWith(`${schemaName}.`);

/**
 * Aspects of the seed, keyed by the user label of their owner. Each value is
 * `<class>=<value>`, and the values are unique per owner so a test can name
 * an aspect by its value.
 */
const seedAspects: Record<string, string[]> = {
  "1": [
    "UniqueA=1-u",
    "MultiA=1-a1",
    "MultiA=1-a2",
    "MultiA=1-a3",
    "MultiB=1-b1",
  ],
  "2": ["UniqueA=2-u", "MultiA=2-a1"],
  "3": ["MultiA=3-a1"],
};

interface AspectRow {
  id: Id64String;
  /** `<class>=<value>`, as in {@link seedAspects}. */
  content: string;
}

function ownerOf(db: IModelDb, label: string): Id64String {
  const ownerId = IModelTestUtils.queryByUserLabel(db, label);
  expect(ownerId, `no element labeled ${label}`).not.toBeUndefined();
  return ownerId;
}

const byId = (a: Id64String, b: Id64String) => {
  const [x, y] = [BigInt(a), BigInt(b)];
  return x < y ? -1 : x > y ? 1 : 0;
};

/** Test-schema aspects of an owner in ECInstanceId order. */
function readAspects(db: IModelDb, label: string): AspectRow[] {
  return db.elements
    .getAspects(ownerOf(db, label))
    .filter((aspect) => isTestAspectClass(aspect.classFullName))
    .map((aspect) => ({
      id: aspect.id,
      content: `${aspect.classFullName.split(":")[1]}=${(aspect.toJSON() as { value?: string }).value}`,
    }))
    .sort((a, b) => byId(a.id, b.id));
}

/** Sorted `<class>=<value>` contents of an owner's test-schema aspects. */
const contentsOf = (db: IModelDb, label: string) =>
  readAspects(db, label)
    .map(({ content }) => content)
    .sort();

const idsOf = (db: IModelDb, label: string) =>
  readAspects(db, label).map(({ id }) => id);

function idOf(db: IModelDb, label: string, content: string): Id64String {
  const row = readAspects(db, label).find((r) => r.content === content);
  expect(row, `no ${content} aspect on ${label}`).not.toBeUndefined();
  return row!.id;
}

/** Counts all aspects, of any class, that an owner ID still has. */
async function aspectCountOfOwner(
  db: IModelDb,
  ownerId: Id64String
): Promise<number> {
  let total = 0;
  for (const table of ["ElementMultiAspect", "ElementUniqueAspect"])
    for await (const row of db.createQueryReader(
      `SELECT COUNT(*) FROM bis.${table} WHERE Element.Id = ?`,
      QueryBinder.from([ownerId])
    ))
      total += row[0] as number;
  return total;
}

/** Identifier of a connector's ExternalSourceAspect on owner 1 of the seed. */
const seedConnectorId = "connector-element-1";

/** A connector's provenance for owner 1, scoped outside any transformation. */
function connectorAspectProps(
  db: IModelDb,
  identifier: string,
  version: string
): ExternalSourceAspectProps {
  return {
    classFullName: ExternalSourceAspect.classFullName,
    element: new ElementOwnsExternalSourceAspects(ownerOf(db, "1")),
    scope: { id: IModel.dictionaryId },
    kind: ExternalSourceAspect.Kind.Element,
    identifier,
    version,
  };
}

const classFullNameOf = (className: string) => `${schemaName}:${className}`;

/** Edits test-schema aspects by owner label and `<class>=<value>` content. */
class AspectEditor {
  public constructor(
    private readonly _db: IModelDb,
    private readonly _txn: EditTxn
  ) {}

  public insert(label: string, content: string): Id64String {
    const [className, value] = content.split("=");
    const classFullName = classFullNameOf(className);
    const ownerId = ownerOf(this._db, label);
    return this._txn.insertAspect({
      classFullName,
      element:
        classFullName === uniqueA
          ? new ElementOwnsUniqueAspect(ownerId)
          : new ElementOwnsMultiAspects(ownerId),
      value,
    } as ElementAspectProps);
  }

  public update(label: string, content: string, newValue: string): void {
    const aspect = this._db.elements.getAspect(idOf(this._db, label, content));
    this._txn.updateAspect({
      ...aspect.toJSON(),
      value: newValue,
    } as ElementAspectProps);
  }

  public delete(label: string, content: string): void {
    this._txn.deleteAspect(idOf(this._db, label, content));
  }
}

const editAspects = (
  edit: (aspects: AspectEditor) => void
): TimelineStateChange => ({
  manualUpdate(db: IModelDb) {
    withEditTxn(db, "edit aspects", (txn) => edit(new AspectEditor(db, txn)));
  },
});

interface AspectWrites {
  inserted: number;
  updated: number;
  deleted: number;
}

/** Aspect rows of the test schema in a sync's local changes, by operation. */
interface AspectChanges {
  inserted: Id64String[];
  updated: Id64String[];
  deleted: Id64String[];
}

/** What a sync wrote to test-schema aspects. */
interface SyncRecord {
  /** Writes made through the importer's aspect hooks. */
  writes: AspectWrites;
  /** Aspect rows in the target's changes, i.e. what a changeset consumer sees. */
  changes: AspectChanges;
}

function countAspectWrites(transformer: IModelTransformer): AspectWrites {
  const writes: AspectWrites = { inserted: 0, updated: 0, deleted: 0 };
  const importer = transformer.importer as any;
  const hooks = {
    onInsertElementAspect: "inserted",
    onUpdateElementAspect: "updated",
    onDeleteElementAspect: "deleted",
  } as const;
  for (const [hook, key] of Object.entries(hooks)) {
    const original = importer[hook].bind(importer);
    importer[hook] = async (aspect: { classFullName: string }) => {
      if (isTestAspectClass(aspect.classFullName)) writes[key]++;
      return original(aspect);
    };
  }
  return writes;
}

function readAspectChanges(db: IModelDb): AspectChanges {
  const opById = new Map<Id64String, string>();
  let reader: ChangesetReader;
  try {
    reader = ChangesetReader.openLocalChanges({
      db,
      includeInMemoryChanges: true,
      rowOptions: { classIdsToClassNames: true },
    });
  } catch (error) {
    // The sync wrote nothing at all.
    if (error instanceof Error && error.message === "no local changes")
      return { inserted: [], updated: [], deleted: [] };
    throw error;
  }
  try {
    while (reader.step()) {
      if (!reader.isECTable) continue;
      const instance = reader.inserted ?? reader.deleted;
      const className = instance?.ECClassId;
      if (typeof className === "string" && isTestAspectClass(className))
        opById.set(instance!.ECInstanceId, reader.op);
    }
  } finally {
    reader.close();
  }
  const idsWithOp = (op: string) =>
    [...opById]
      .filter(([, o]) => o === op)
      .map(([id]) => id)
      .sort(byId);
  return {
    inserted: idsWithOp("Inserted"),
    updated: idsWithOp("Updated"),
    deleted: idsWithOp("Deleted"),
  };
}

/** A sync timeline step that records what the sync wrote to test-schema aspects. */
function recordedSync(
  source: string,
  record: SyncRecord[]
): TimelineStateChange {
  let writes: AspectWrites | undefined;
  return {
    sync: [
      source,
      {
        initTransformer: (transformer) => {
          writes = countAspectWrites(transformer);
        },
        assert: {
          afterProcessChanges: (transformer) => {
            record.push({
              writes: writes!,
              changes: readAspectChanges(transformer.targetDb),
            });
          },
        },
      },
    ],
  };
}

const noWrites: SyncRecord = {
  writes: { inserted: 0, updated: 0, deleted: 0 },
  changes: { inserted: [], updated: [], deleted: [] },
};

describe("ElementAspect synchronization", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "ElementAspectSync"
  );
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let seed: TimelineIModelState;
  let restoreCheckpointDownload: (() => void) | undefined;

  beforeAll(async () => {
    transformerTestHub.start("ElementAspectSync", KnownTestLocations.outputDir);
    iTwinId = transformerTestHub.iTwinId;
    IModelJsFs.recursiveMkDirSync(outputDir);
    accessToken = await HubWrappers.getAccessToken(
      TestUtils.TestUserType.Regular
    );
  });

  afterAll(() => transformerTestHub.stop());

  beforeEach(async (context) => {
    restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
    const name = context.task.name.replace(/\W+/g, "-").slice(0, 80);
    const seedFile = path.join(outputDir, `${name}-seed.bim`);
    if (IModelJsFs.existsSync(seedFile)) IModelJsFs.removeSync(seedFile);
    const seedDb = SnapshotDb.createEmpty(seedFile, {
      rootSubject: { name: "AspectSyncSeed" },
    });
    await seedDb.importSchemaStrings([schemaXml]);
    withEditTxn(seedDb, "save schema", () => {});
    const state = { 1: 1, 2: 1, 3: 1 };
    populateTimelineSeed(seedDb, state);
    withEditTxn(seedDb, "seed aspects", (txn) => {
      const editor = new AspectEditor(seedDb, txn);
      for (const [label, contents] of Object.entries(seedAspects))
        for (const content of contents) editor.insert(label, content);
      txn.insertAspect(connectorAspectProps(seedDb, seedConnectorId, "v1"));
    });
    seedDb.performCheckpoint();
    // runTimeline only reads the seed's path, after a checkpoint.
    seed = { db: seedDb as any as BriefcaseDb, id: "aspect-sync-seed", state };
  });

  afterEach(() => {
    seed.db.close();
    restoreCheckpointDownload?.();
    restoreCheckpointDownload = undefined;
  });

  /** Runs a timeline that starts with master from the seed and a branch of it. */
  async function run(
    steps: Timeline[number][],
    transformerOpts?: IModelTransformOptions
  ) {
    const timeline: Timeline = [
      { master: { seed } },
      { branch: { branch: "master" } },
      ...steps,
    ];
    const result = await runTimeline(timeline, {
      iTwinId,
      accessToken,
      transformerOpts,
    });
    const master = result.trackedIModels.get("master")!.db;
    const branch = result.trackedIModels.get("branch")!.db;
    return { master, branch, tearDown: result.tearDown };
  }

  describe("forward sync", () => {
    it("writes nothing when the source changes no aspects", async () => {
      const syncs: SyncRecord[] = [];
      let branchIdsBefore: Id64String[][] = [];
      const { master, branch, tearDown } = await run([
        {
          assert: ({ branch: b }) => {
            branchIdsBefore = ["1", "2", "3"].map((l) => idsOf(b.db, l));
          },
        },
        // Change every aspect owner and add an element.
        { master: { 1: 2, 2: 2, 3: 2, 4: 1 } },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(syncs).to.deep.equal([noWrites]);
        for (const [i, label] of ["1", "2", "3"].entries()) {
          expect(contentsOf(branch, label)).to.deep.equal(
            contentsOf(master, label)
          );
          expect(idsOf(branch, label)).to.deep.equal(branchIdsBefore[i]);
        }
      } finally {
        await tearDown();
      }
    });

    it("updates matched aspects in place when one class has deletes, updates, and inserts", async () => {
      const syncs: SyncRecord[] = [];
      let before: AspectRow[] = [];
      const { master, branch, tearDown } = await run([
        {
          assert: ({ branch: b }) => {
            before = readAspects(b.db, "1");
          },
        },
        {
          master: editAspects((aspects) => {
            aspects.delete("1", "MultiA=1-a1");
            aspects.update("1", "MultiA=1-a3", "1-a3-changed");
            aspects.insert("1", "MultiA=1-a4");
          }),
        },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(contentsOf(branch, "1")).to.deep.equal(contentsOf(master, "1"));
        // Multi-aspects match by position within their class: the three source
        // aspects take over the three existing MultiA slots in ECInstanceId order.
        const multiAIds = before
          .filter(({ content }) => content.startsWith("MultiA="))
          .map(({ id }) => id);
        expect(
          readAspects(branch, "1")
            .filter(({ content }) => content.startsWith("MultiA="))
            .map(({ id, content }) => [id, content])
        ).to.deep.equal([
          [multiAIds[0], "MultiA=1-a2"],
          [multiAIds[1], "MultiA=1-a3-changed"],
          [multiAIds[2], "MultiA=1-a4"],
        ]);
        expect(syncs).to.deep.equal([
          {
            writes: { inserted: 0, updated: 3, deleted: 0 },
            changes: { inserted: [], updated: multiAIds, deleted: [] },
          },
        ]);
        // Other classes on the owner are not written.
        expect(idsOf(branch, "1")).to.deep.equal(before.map(({ id }) => id));
      } finally {
        await tearDown();
      }
    });

    it("applies changes from several changesets in one sync", async () => {
      const syncs: SyncRecord[] = [];
      let before: AspectRow[] = [];
      const { master, branch, tearDown } = await run([
        {
          assert: ({ branch: b }) => {
            before = readAspects(b.db, "1");
          },
        },
        { master: editAspects((a) => a.insert("1", "MultiA=1-a4")) },
        {
          master: editAspects((a) =>
            a.update("1", "MultiA=1-a2", "1-a2-changed")
          ),
        },
        {
          master: editAspects((a) => {
            // Inserted and deleted before the sync, so the sync never sees it.
            a.delete("1", "MultiA=1-a4");
            a.delete("1", "MultiA=1-a1");
          }),
        },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(contentsOf(branch, "1")).to.deep.equal(contentsOf(master, "1"));
        const [slot1, slot2, slot3] = before
          .filter(({ content }) => content.startsWith("MultiA="))
          .map(({ id }) => id);
        expect(syncs).to.deep.equal([
          {
            writes: { inserted: 0, updated: 2, deleted: 1 },
            changes: {
              inserted: [],
              updated: [slot1, slot2],
              deleted: [slot3],
            },
          },
        ]);
      } finally {
        await tearDown();
      }
    });

    it("applies aspect changes whose owner element did not change", async () => {
      const syncs: SyncRecord[] = [];
      let before3: AspectRow[] = [];
      const { master, branch, tearDown } = await run([
        {
          assert: ({ branch: b }) => {
            before3 = readAspects(b.db, "3");
          },
        },
        {
          master: editAspects((a) => {
            // Empty every class of owner 2.
            a.delete("2", "UniqueA=2-u");
            a.delete("2", "MultiA=2-a1");
            // Add a unique aspect to owner 3 and update its multi-aspect.
            a.insert("3", "UniqueA=3-u");
            a.update("3", "MultiA=3-a1", "3-a1-changed");
          }),
        },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(contentsOf(branch, "2")).to.deep.equal([]);
        expect(contentsOf(branch, "3")).to.deep.equal(contentsOf(master, "3"));
        expect(idOf(branch, "3", "MultiA=3-a1-changed")).to.equal(
          before3[0].id
        );
        expect(syncs[0].writes).to.deep.equal({
          inserted: 1,
          updated: 1,
          deleted: 2,
        });
      } finally {
        await tearDown();
      }
    });

    it("updates in place when the source replaces a multi-aspect with another of its class", async () => {
      const syncs: SyncRecord[] = [];
      let b1 = "";
      const { master, branch, tearDown } = await run([
        {
          assert: ({ branch: b }) => {
            b1 = idOf(b.db, "1", "MultiB=1-b1");
          },
        },
        {
          master: editAspects((a) => {
            a.delete("1", "MultiB=1-b1");
            a.insert("1", "MultiB=1-b2");
          }),
        },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(contentsOf(branch, "1")).to.deep.equal(contentsOf(master, "1"));
        expect(idOf(branch, "1", "MultiB=1-b2")).to.equal(b1);
        expect(syncs).to.deep.equal([
          {
            writes: { inserted: 0, updated: 1, deleted: 0 },
            changes: { inserted: [], updated: [b1], deleted: [] },
          },
        ]);
      } finally {
        await tearDown();
      }
    });

    it("updates in place when a unique aspect is deleted and added again before the sync", async () => {
      const syncs: SyncRecord[] = [];
      let unique = "";
      const { master, branch, tearDown } = await run([
        {
          assert: ({ branch: b }) => {
            unique = idOf(b.db, "1", "UniqueA=1-u");
          },
        },
        { master: editAspects((a) => a.delete("1", "UniqueA=1-u")) },
        { master: editAspects((a) => a.insert("1", "UniqueA=1-u-again")) },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(contentsOf(branch, "1")).to.deep.equal(contentsOf(master, "1"));
        expect(idOf(branch, "1", "UniqueA=1-u-again")).to.equal(unique);
        expect(syncs).to.deep.equal([
          {
            writes: { inserted: 0, updated: 1, deleted: 0 },
            changes: { inserted: [], updated: [unique], deleted: [] },
          },
        ]);
      } finally {
        await tearDown();
      }
    });

    it("removes the aspects of a deleted owner with it", async () => {
      const syncs: SyncRecord[] = [];
      let owner2 = "";
      const { branch, tearDown } = await run([
        {
          assert: ({ branch: b }) => {
            owner2 = ownerOf(b.db, "2");
          },
        },
        { master: { 2: deleted } },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(branch.elements.tryGetElementProps(owner2)).to.be.undefined;
        expect(await aspectCountOfOwner(branch, owner2)).to.equal(0);
        // The aspects go with their element, not through the aspect hooks.
        expect(syncs[0].writes).to.deep.equal({
          inserted: 0,
          updated: 0,
          deleted: 0,
        });
        expect(syncs[0].changes.deleted).to.have.length(2);
      } finally {
        await tearDown();
      }
    });

    it("applies changes across owner batches the same way", async () => {
      const exporter = IModelExporter as unknown as Record<string, number>;
      const batchSizeKey = "_elementAspectOwnerBatchSize";
      const batchSize = exporter[batchSizeKey];
      // One owner per batch, so every owner crosses a batch boundary.
      exporter[batchSizeKey] = 1;
      const syncs: SyncRecord[] = [];
      try {
        const { master, branch, tearDown } = await run([
          {
            master: editAspects((a) => {
              a.delete("1", "MultiA=1-a1");
              a.update("1", "MultiA=1-a3", "1-a3-changed");
              a.insert("1", "MultiA=1-a4");
              a.delete("2", "UniqueA=2-u");
              a.update("2", "MultiA=2-a1", "2-a1-changed");
              a.insert("3", "MultiB=3-b1");
            }),
          },
          { master: { 1: 2, 2: 2, 3: 2 } },
          { branch: recordedSync("master", syncs) },
        ]);
        try {
          for (const label of ["1", "2", "3"])
            expect(contentsOf(branch, label)).to.deep.equal(
              contentsOf(master, label)
            );
          expect(syncs[0].writes).to.deep.equal({
            inserted: 1,
            updated: 4,
            deleted: 1,
          });
        } finally {
          await tearDown();
        }
      } finally {
        exporter[batchSizeKey] = batchSize;
      }
    });
  });

  describe("reverse sync", () => {
    it("applies the branch's inserts, updates, and deletes to master", async () => {
      const syncs: SyncRecord[] = [];
      let before: AspectRow[] = [];
      const { master, branch, tearDown } = await run([
        {
          assert: ({ master: m }) => {
            before = readAspects(m.db, "1");
          },
        },
        {
          branch: editAspects((a) => {
            a.delete("1", "MultiA=1-a2");
            a.insert("1", "MultiA=1-a5");
            a.update("1", "UniqueA=1-u", "1-u-branch");
            a.delete("2", "UniqueA=2-u");
          }),
        },
        { master: recordedSync("branch", syncs) },
      ]);
      try {
        for (const label of ["1", "2", "3"])
          expect(contentsOf(master, label)).to.deep.equal(
            contentsOf(branch, label)
          );
        const slots = before
          .filter(({ content }) => content.startsWith("MultiA="))
          .map(({ id }) => id);
        const unique = before.find(({ content }) =>
          content.startsWith("UniqueA=")
        )!.id;
        // MultiA [a1, a2, a3] becomes [a1, a3, a5]: a1 keeps its slot, and a3
        // and a5 take over the next two.
        expect(syncs[0].writes).to.deep.equal({
          inserted: 0,
          updated: 3,
          deleted: 1,
        });
        expect(syncs[0].changes.inserted).to.deep.equal([]);
        expect(syncs[0].changes.updated).to.deep.equal(
          [unique, slots[1], slots[2]].sort(byId)
        );
        expect(syncs[0].changes.deleted).to.have.length(1);
      } finally {
        await tearDown();
      }
    });

    it("writes no aspects on the forward sync after a reverse sync", async () => {
      const syncs: SyncRecord[] = [];
      await (
        await run([
          {
            branch: editAspects((a) => {
              a.delete("1", "MultiA=1-a1");
              a.insert("2", "MultiB=2-b1");
              a.update("3", "MultiA=3-a1", "3-a1-branch");
            }),
          },
          { master: recordedSync("branch", syncs) },
          { branch: recordedSync("master", syncs) },
        ])
      ).tearDown();
      expect(syncs[0].writes).not.to.deep.equal(noWrites.writes);
      expect(syncs[1]).to.deep.equal(noWrites);
    });

    it("writes no aspects when the branch changes only elements", async () => {
      const syncs: SyncRecord[] = [];
      let before: Id64String[][] = [];
      const { master, tearDown } = await run([
        {
          assert: ({ master: m }) => {
            before = ["1", "2", "3"].map((l) => idsOf(m.db, l));
          },
        },
        { branch: { 1: 2, 2: 2, 3: 2 } },
        { master: recordedSync("branch", syncs) },
      ]);
      try {
        expect(syncs).to.deep.equal([noWrites]);
        expect(["1", "2", "3"].map((l) => idsOf(master, l))).to.deep.equal(
          before
        );
      } finally {
        await tearDown();
      }
    });
  });

  // A sync makes the aspects of every owner it exports match the source's. It
  // has no record of which side last changed an aspect, so the target's own
  // aspect edits on those owners are replaced, while owners the sync does not
  // export keep them.
  describe("aspects edited on the target", () => {
    it("are replaced by the source's on an owner whose element changed in the source", async () => {
      const syncs: SyncRecord[] = [];
      const { master, branch, tearDown } = await run([
        {
          branch: editAspects((a) => {
            a.insert("1", "MultiA=1-branch-only");
            a.update("1", "MultiA=1-a2", "1-a2-branch");
            a.delete("1", "MultiB=1-b1");
          }),
        },
        { master: { 1: 2 } },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(contentsOf(branch, "1")).to.deep.equal(contentsOf(master, "1"));
        expect(contentsOf(branch, "1")).to.deep.equal(
          [...seedAspects["1"]].sort()
        );
        expect(syncs[0].writes).to.deep.equal({
          inserted: 1,
          updated: 1,
          deleted: 1,
        });
      } finally {
        await tearDown();
      }
    });

    it("are kept on owners the source did not change", async () => {
      const syncs: SyncRecord[] = [];
      const { branch, tearDown } = await run([
        {
          branch: editAspects((a) => {
            a.insert("3", "MultiB=3-branch-only");
            a.update("3", "MultiA=3-a1", "3-a1-branch");
          }),
        },
        { master: { 1: 2 } },
        { branch: recordedSync("master", syncs) },
      ]);
      try {
        expect(contentsOf(branch, "3")).to.deep.equal([
          "MultiA=3-a1-branch",
          "MultiB=3-branch-only",
        ]);
        expect(syncs).to.deep.equal([noWrites]);
      } finally {
        await tearDown();
      }
    });

    it("are replaced by the branch's on master when a reverse sync exports their owner", async () => {
      const syncs: SyncRecord[] = [];
      const { master, branch, tearDown } = await run([
        {
          master: editAspects((a) => {
            a.insert("1", "MultiA=1-master-only");
            a.update("3", "MultiA=3-a1", "3-a1-master");
          }),
        },
        { branch: { 1: 2 } },
        { master: recordedSync("branch", syncs) },
      ]);
      try {
        expect(contentsOf(master, "1")).to.deep.equal(contentsOf(branch, "1"));
        expect(contentsOf(master, "1")).not.to.include("MultiA=1-master-only");
        // The reverse sync did not export owner 3, so master's edit stays.
        expect(contentsOf(master, "3")).to.deep.equal(["MultiA=3-a1-master"]);
        expect(syncs[0].writes).to.deep.equal({
          inserted: 0,
          updated: 0,
          deleted: 1,
        });
      } finally {
        await tearDown();
      }
    });

    // A connector's ExternalSourceAspects on master, scoped to the
    // connector's source rather than to a transformation.
    // Without includeSourceProvenance a sync neither copies nor reconciles
    // ExternalSourceAspects, so master's connector provenance is left alone.
    // With it, as the transformations service runs, the connector provenance
    // is reconciled like any other aspect of an exported owner: the branch's
    // copy wins. Main's delete-and-rebuild did the same.
    describe.each([false, true])(
      "connector provenance on master (includeSourceProvenance: %s)",
      (includeSourceProvenance) => {
        const versionsOf = (db: IModelDb, identifier: string) =>
          db.elements
            .getAspects(ownerOf(db, "1"), ExternalSourceAspect.classFullName)
            .map((aspect) => aspect as ExternalSourceAspect)
            .filter((aspect) => aspect.identifier === identifier)
            .map((aspect) => aspect.version);

        it("added after the fork is deleted only with includeSourceProvenance when a reverse sync exports its owner", async () => {
          const { master, tearDown } = await run(
            [
              {
                master: {
                  manualUpdate(db: IModelDb) {
                    withEditTxn(db, "connector adds provenance", (txn) => {
                      txn.insertAspect(
                        connectorAspectProps(db, "connector-added", "v1")
                      );
                    });
                  },
                },
              },
              { branch: { 1: 2 } },
              { master: { sync: ["branch"] } },
            ],
            { includeSourceProvenance }
          );
          try {
            expect(versionsOf(master, "connector-added")).to.deep.equal(
              includeSourceProvenance ? [] : ["v1"]
            );
          } finally {
            await tearDown();
          }
        });

        it("updated after the fork is reverted only with includeSourceProvenance when a reverse sync exports its owner", async () => {
          const { master, tearDown } = await run(
            [
              {
                master: {
                  manualUpdate(db: IModelDb) {
                    const aspect = db.elements
                      .getAspects(
                        ownerOf(db, "1"),
                        ExternalSourceAspect.classFullName
                      )
                      .find(
                        (a) =>
                          (a as ExternalSourceAspect).identifier ===
                          seedConnectorId
                      )!;
                    withEditTxn(db, "connector updates provenance", (txn) => {
                      txn.updateAspect({
                        ...aspect.toJSON(),
                        version: "v2",
                      } as ElementAspectProps);
                    });
                  },
                },
              },
              { branch: { 1: 2 } },
              { master: { sync: ["branch"] } },
            ],
            { includeSourceProvenance }
          );
          try {
            expect(versionsOf(master, seedConnectorId)).to.deep.equal([
              includeSourceProvenance ? "v1" : "v2",
            ]);
          } finally {
            await tearDown();
          }
        });
      }
    );
  });
});
