/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect, vi } from "vitest";
import {
  EditTxn,
  IModelDb,
  RelationshipProps,
  SourceAndTarget,
  StandaloneDb,
  Subject,
} from "@itwin/core-backend";
import { IModel, QueryBinder } from "@itwin/core-common";
import { Id64, Id64String } from "@itwin/core-bentley";
import { hasEntityChanged, IModelImporter } from "../../IModelImporter";
import { TargetRelationshipLookup } from "../../TargetRelationshipLookup";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

const relationshipConstraints = `
    <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true">
      <Class class="bis:Element"/>
    </Source>
    <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true">
      <Class class="bis:Element"/>
    </Target>`;

const testSchema = `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestRelLookup" alias="trl" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEnumeration typeName="Color" backingTypeName="int" isStrict="true">
    <ECEnumerator name="Red" value="1"/>
    <ECEnumerator name="Blue" value="2"/>
  </ECEnumeration>
  <ECRelationshipClass typeName="TestRefersTo" strength="referencing" modifier="None">
    <BaseClass>bis:ElementRefersToElements</BaseClass>
    <ECProperty propertyName="Note" typeName="string"/>
    <ECProperty propertyName="Rank" typeName="int"/>
    <ECProperty propertyName="Weight" typeName="double"/>
    <ECProperty propertyName="Flag" typeName="boolean"/>
    <ECProperty propertyName="Big" typeName="long"/>
    <ECProperty propertyName="Stamp" typeName="dateTime"/>
    <ECProperty propertyName="Shade" typeName="Color"/>
    <ECProperty propertyName="OtherId" typeName="long" extendedTypeName="Id"/>
    ${relationshipConstraints}
  </ECRelationshipClass>
  <ECRelationshipClass typeName="TestRefersToChild" strength="referencing" modifier="None">
    <BaseClass>TestRefersTo</BaseClass>
    <ECProperty propertyName="ChildNote" typeName="string"/>
    ${relationshipConstraints}
  </ECRelationshipClass>
  <ECRelationshipClass typeName="TestPointRefersTo" strength="referencing" modifier="None">
    <BaseClass>bis:ElementRefersToElements</BaseClass>
    <ECProperty propertyName="Location" typeName="point3d"/>
    ${relationshipConstraints}
  </ECRelationshipClass>
</ECSchema>`;

const refersTo = "TestRelLookup:TestRefersTo";
const refersToChild = "TestRelLookup:TestRefersToChild";
const pointRefersTo = "TestRelLookup:TestPointRefersTo";
const groupsMembers = "BisCore:ElementGroupsMembers";
const elementRefersToElements = "BisCore:ElementRefersToElements";
const drivesElement = "BisCore:ElementDrivesElement";

type Decision = "insert" | "update" | "none";

class RecordingImporter extends IModelImporter {
  public decisions: Decision[] = [];
  public insertedIds: Id64String[] = [];
  /** Optional replacement for the base insert behavior. */
  public insertOverride?: (
    props: RelationshipProps
  ) => Promise<Id64String> | undefined;

  protected override async onInsertRelationship(
    props: RelationshipProps
  ): Promise<Id64String> {
    this.decisions.push("insert");
    const overridden = this.insertOverride?.(props);
    const id = await (overridden ?? super.onInsertRelationship(props));
    this.insertedIds.push(id);
    return id;
  }

  protected override async onUpdateRelationship(
    props: RelationshipProps
  ): Promise<void> {
    this.decisions.push("update");
    this.updatedIds.push(props.id!);
    await super.onUpdateRelationship(props);
  }

  public updatedIds: Id64String[] = [];

  /** The decision made by the last import, where no insert or update means the relationship was unchanged. */
  public async importAndDecide(
    props: RelationshipProps
  ): Promise<{ decision: Decision; id: Id64String }> {
    const before = this.decisions.length;
    const id = await this.importRelationship(props);
    expect(this.decisions.length - before).to.be.at.most(1);
    return { decision: this.decisions[before] ?? "none", id };
  }

  /** Like [[importAndDecide]], but also reports the decision when core rejects the write, such as for an abstract class. */
  public async decide(
    props: RelationshipProps
  ): Promise<{ decision: Decision; id?: Id64String }> {
    try {
      return await this.importAndDecide(props);
    } catch (error) {
      const decision = this.decisions.at(-1);
      if (decision === undefined) throw error;
      return {
        decision,
        id: decision === "update" ? this.updatedIds.at(-1) : undefined,
      };
    }
  }
}

/** The decision that the importer made before it batched relationship lookups. */
function expectedDecision(
  db: IModelDb,
  props: RelationshipProps
): { decision: Decision; id?: Id64String } {
  const existing = db.relationships.tryGetInstance(props.classFullName, {
    sourceId: props.sourceId,
    targetId: props.targetId,
  });
  if (existing === undefined) return { decision: "insert" };
  return {
    decision: hasEntityChanged(existing, { ...props, id: existing.id })
      ? "update"
      : "none",
    id: existing.id,
  };
}

describe("IModelImporter.importRelationship lookup", () => {
  let targetDb: StandaloneDb;
  let editTxn: EditTxn;
  let importer: RecordingImporter;
  let elementIds: Id64String[];
  let dbCount = 0;
  const defaultCoreLookupsBeforeCount =
    TargetRelationshipLookup.coreLookupsBeforeCount;

  const props = (
    classFullName: string,
    sourceId: Id64String,
    targetId: Id64String,
    extra: Record<string, unknown> = {}
  ): RelationshipProps => ({ classFullName, sourceId, targetId, ...extra });

  const fullValues = (): Record<string, unknown> => ({
    note: "a",
    rank: 1,
    weight: 1.5,
    flag: true,
    big: 2 ** 40,
    stamp: "2024-01-02T03:04:05.000",
    shade: 2,
    otherId: elementIds[5],
  });

  beforeEach(async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      `RelationshipLookup${++dbCount}.bim`
    );
    targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "RelationshipLookup" },
    });
    await targetDb.importSchemaStrings([testSchema]);
    editTxn = createStartedEditTxn(targetDb);
    elementIds = [0, 1, 2, 3, 4, 5].map((i) =>
      Subject.create(targetDb, IModel.rootSubjectId, `Subject${i}`).insert(
        editTxn
      )
    );
    editTxn.saveChanges();
    importer = new RecordingImporter(editTxn);
    // Most tests preload on the first lookup, which makes the preloaded path answer it.
    TargetRelationshipLookup.coreLookupsBeforeCount = 0;
  });

  afterEach(() => {
    TargetRelationshipLookup.coreLookupsBeforeCount =
      defaultCoreLookupsBeforeCount;
    vi.restoreAllMocks();
    if (editTxn.isActive) editTxn.end("abandon");
    targetDb.close();
  });

  it("makes the same decisions as core lookups without per-relationship queries", async () => {
    const [e0, e1, e2, e3, e4] = elementIds;
    editTxn.insertRelationship(props(refersTo, e0, e1, fullValues()));
    editTxn.insertRelationship(props(refersTo, e1, e2));
    editTxn.insertRelationship(
      props(refersToChild, e2, e3, { ...fullValues(), childNote: "c" })
    );
    editTxn.insertRelationship(
      props(groupsMembers, e3, e4, { memberPriority: 3 })
    );
    editTxn.saveChanges();

    const pairs: Array<[Id64String, Id64String]> = [
      [e0, e1],
      [e1, e2],
      [e2, e3],
      [e3, e4],
      [e4, e0],
    ];
    const variants: Array<Record<string, unknown>> = [
      {},
      fullValues(),
      { ...fullValues(), weight: 2.5 },
      { ...fullValues(), stamp: "2024-01-02T03:04:06.000" },
      { ...fullValues(), childNote: "c" },
      { memberPriority: 3 },
      { memberPriority: 4 },
    ];
    const seen = new Set<Decision>();
    for (const classFullName of [
      refersTo,
      refersToChild,
      groupsMembers,
      elementRefersToElements,
    ]) {
      for (const [sourceId, targetId] of pairs) {
        for (const variant of variants) {
          const caseProps = props(classFullName, sourceId, targetId, variant);
          const expected = expectedDecision(targetDb, caseProps);
          const caseImporter = new RecordingImporter(editTxn);
          const tryGetInstance = vi.spyOn(
            targetDb.relationships,
            "tryGetInstance"
          );
          const actual = await caseImporter.decide({ ...caseProps });
          const label = `${classFullName} ${sourceId}->${targetId} ${JSON.stringify(variant)}`;
          expect(actual.decision, label).to.equal(expected.decision);
          if (expected.id !== undefined)
            expect(actual.id, label).to.equal(expected.id);
          expect(tryGetInstance, label).not.toHaveBeenCalled();
          tryGetInstance.mockRestore();
          seen.add(expected.decision);
          editTxn.abandonChanges();
        }
      }
    }
    expect([...seen].sort()).to.deep.equal(["insert", "none", "update"]);
  });

  it("finds relationships inserted earlier in the same run and updates only changed ones", async () => {
    const [e0, e1] = elementIds;
    const first = await importer.importAndDecide(
      props(refersTo, e0, e1, { note: "first" })
    );
    expect(first.decision).to.equal("insert");

    const unchanged = await importer.importAndDecide(
      props(refersTo, e0, e1, { note: "first" })
    );
    expect(unchanged).to.deep.equal({ decision: "none", id: first.id });

    const changed = await importer.importAndDecide(
      props(refersTo, e0, e1, { note: "second" })
    );
    expect(changed).to.deep.equal({ decision: "update", id: first.id });
    expect(
      targetDb.relationships.getInstanceProps<
        RelationshipProps & { note?: string }
      >(refersTo, first.id).note
    ).to.equal("second");
    expect(
      await importer.importAndDecide(
        props(refersTo, e0, e1, { note: "second" })
      )
    ).to.deep.equal({ decision: "none", id: first.id });
  });

  describe("with the default preload threshold", () => {
    const isCount = (ecsql: string) => ecsql.includes("COUNT(*)");

    beforeEach(() => {
      TargetRelationshipLookup.coreLookupsBeforeCount =
        defaultCoreLookupsBeforeCount;
      expect(TargetRelationshipLookup.coreLookupsBeforeCount).to.equal(32);
      expect(TargetRelationshipLookup.rowsPerCoreLookup).to.equal(24);
    });

    let subjectCount = 0;
    const insertSubjects = (count: number) => {
      const ids = Array.from({ length: count }, () =>
        Subject.create(
          targetDb,
          IModel.rootSubjectId,
          `Extra${subjectCount++}`
        ).insert(editTxn)
      );
      editTxn.saveChanges();
      return ids;
    };

    /** Distinct source and target pairs of the specified elements. */
    const pairsOf = (ids: Id64String[]) =>
      ids.flatMap((sourceId) =>
        ids
          .filter((targetId) => targetId !== sourceId)
          .map((targetId) => ({ sourceId, targetId }))
      );

    /** A target with 960 relationships, which justifies a preload after 960 / 24 = 40 lookups. */
    const insert960Relationships = () => {
      const pairs = pairsOf(insertSubjects(32)).slice(0, 960);
      for (const pair of pairs)
        editTxn.insertRelationship(
          props(refersTo, pair.sourceId, pair.targetId)
        );
      editTxn.saveChanges();
      return pairs;
    };

    it("counts a target class without relationships once and preloads it right away", async () => {
      const pairs = pairsOf(insertSubjects(8)).slice(0, 40);
      const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");
      const createQueryReader = vi.spyOn(targetDb, "createQueryReader");

      for (const [i, pair] of pairs.entries()) {
        expect(
          (
            await importer.importAndDecide(
              props(groupsMembers, pair.sourceId, pair.targetId, {
                memberPriority: 1,
              })
            )
          ).decision
        ).to.equal("insert");
        expect(tryGetInstance).toHaveBeenCalledTimes(Math.min(i + 1, 32));
        expect(createQueryReader).toHaveBeenCalledTimes(i < 32 ? 0 : 2);
      }
      expect(isCount(createQueryReader.mock.calls[0][0])).to.be.true;
      expect(
        targetDb.relationships.tryGetInstance(groupsMembers, pairs[35])?.id
      ).to.equal(importer.insertedIds[35]);
    });

    it("never preloads for a small import into a large target", async () => {
      const pairs = insert960Relationships();
      const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");
      const createQueryReader = vi.spyOn(targetDb, "createQueryReader");

      for (const pair of pairs.slice(0, 39))
        expect(
          (
            await importer.importAndDecide(
              props(refersTo, pair.sourceId, pair.targetId)
            )
          ).decision
        ).to.equal("none");

      expect(tryGetInstance).toHaveBeenCalledTimes(39);
      expect(createQueryReader).toHaveBeenCalledTimes(1);
      expect(isCount(createQueryReader.mock.calls[0][0])).to.be.true;
    });

    it("preloads once lookups reach the counted relationships divided by 24", async () => {
      const pairs = insert960Relationships();
      const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");
      const createQueryReader = vi.spyOn(targetDb, "createQueryReader");

      for (const [i, pair] of pairs.slice(0, 50).entries()) {
        expect(
          (
            await importer.importAndDecide(
              props(refersTo, pair.sourceId, pair.targetId)
            )
          ).decision
        ).to.equal("none");
        expect(tryGetInstance).toHaveBeenCalledTimes(Math.min(i + 1, 39));
        expect(createQueryReader).toHaveBeenCalledTimes(
          i < 32 ? 0 : i < 39 ? 1 : 2
        );
      }

      // Element deletes clear the lookup, so the class is looked up with core and counted again.
      await importer.deleteElements(new Set(insertSubjects(1)));
      tryGetInstance.mockClear();
      await importer.importAndDecide(
        props(refersTo, pairs[0].sourceId, pairs[0].targetId)
      );
      expect(tryGetInstance).toHaveBeenCalledTimes(1);
    });

    it("makes the same decisions as core before and after the preload", async () => {
      const ids = elementIds.slice(0, 5);
      const pairs = pairsOf(ids);
      editTxn.insertRelationship(
        props(refersTo, ids[0], ids[1], { note: "seed" })
      );
      editTxn.insertRelationship(props(refersTo, ids[0], ids[2]));
      editTxn.insertRelationship(
        props(refersToChild, ids[0], ids[3], { note: "seed", childNote: "c" })
      );
      editTxn.saveChanges();
      const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");
      const createQueryReader = vi.spyOn(targetDb, "createQueryReader");

      const writtenAfterPreload = new Set<string>();
      const seen = new Set<Decision>();
      for (let i = 0; i < 70; i++) {
        const pair = pairs[i % pairs.length];
        // Pairs alternate between notes, so later rounds both update relationships and leave them unchanged.
        const round = Math.floor(i / pairs.length);
        const note = `v${round === 0 ? 0 : (i + round) % 2}`;
        const caseProps = props(refersTo, pair.sourceId, pair.targetId, {
          note,
        });
        const expected = expectedDecision(targetDb, caseProps);
        const preloaded = createQueryReader.mock.calls.length === 2;
        const key = `${pair.sourceId}/${pair.targetId}`;
        tryGetInstance.mockClear();

        const actual = await importer.importAndDecide(caseProps);

        const label = `lookup ${i + 1} ${key} ${note}`;
        expect(actual.decision, label).to.equal(expected.decision);
        if (expected.id !== undefined)
          expect(actual.id, label).to.equal(expected.id);
        seen.add(expected.decision);
        // Before the preload, core finds every relationship. After it, core finds only those the importer has since written.
        const usesCore = i < 32 || (preloaded && writtenAfterPreload.has(key));
        expect(tryGetInstance, label).toHaveBeenCalledTimes(usesCore ? 1 : 0);
        if (i >= 32 && actual.decision !== "none") writtenAfterPreload.add(key);
      }
      expect(createQueryReader).toHaveBeenCalledTimes(2);
      expect([...seen].sort()).to.deep.equal(["insert", "none", "update"]);
    });
  });

  it("matches a subclass instance through a base-class import, but not the reverse", async () => {
    const [e0, e1, e2, e3] = elementIds;
    const childId = editTxn.insertRelationship(
      props(refersToChild, e0, e1, { note: "n", childNote: "c" })
    );
    const baseId = editTxn.insertRelationship(props(refersTo, e2, e3));

    expect(
      await importer.importAndDecide(props(refersTo, e0, e1, { note: "n" }))
    ).to.deep.equal({ decision: "none", id: childId });
    const throughChild = await importer.importAndDecide(
      props(refersToChild, e2, e3)
    );
    expect(throughChild.decision).to.equal("insert");
    expect(throughChild.id).to.not.equal(baseId);
  });

  it("falls back to core for classes it cannot preload", async () => {
    const [e0, e1, e2, e3] = elementIds;
    const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");
    const importerLookups: string[] = [];

    for (const [classFullName, sourceId, targetId, extra] of [
      [pointRefersTo, e0, e1, { location: { x: 1, y: 2, z: 3 } }],
      [drivesElement, e2, e3, { priority: 1 }],
    ] as const) {
      tryGetInstance.mockClear();
      const first = await importer.importAndDecide(
        props(classFullName, sourceId, targetId, extra)
      );
      expect(first.decision).to.equal("insert");
      importerLookups.push(...tryGetInstance.mock.calls.map((call) => call[0]));
      const again = props(classFullName, sourceId, targetId, extra);
      const expected = expectedDecision(targetDb, again);
      expect(expected.id).to.equal(first.id);
      tryGetInstance.mockClear();
      expect(await importer.importAndDecide(again)).to.deep.equal(expected);
      importerLookups.push(...tryGetInstance.mock.calls.map((call) => call[0]));
    }
    expect(importerLookups).to.deep.equal([
      pointRefersTo,
      pointRefersTo,
      drivesElement,
      drivesElement,
    ]);

    // Like core, an unknown class fails the lookup.
    await expect(
      importer.importRelationship(props("TestRelLookup:Missing", e0, e1))
    ).rejects.toThrow();
  });

  it("delegates source and target pairs with several matching relationships to core", async () => {
    const [e0, e1, e2, e3] = elementIds;
    // inserted so that id order differs from member priority order
    editTxn.insertRelationship(
      props(groupsMembers, e0, e1, { memberPriority: 5 })
    );
    editTxn.insertRelationship(
      props(groupsMembers, e0, e1, { memberPriority: 1 })
    );
    editTxn.insertRelationship(props(refersToChild, e2, e3));
    editTxn.insertRelationship(
      props(groupsMembers, e2, e3, { memberPriority: 2 })
    );
    editTxn.insertRelationship(props(refersTo, e2, e3));
    editTxn.insertRelationship(props(refersTo, e3, e0));
    editTxn.saveChanges();

    for (const classFullName of [
      elementRefersToElements,
      groupsMembers,
      refersTo,
      refersToChild,
    ]) {
      for (const pair of [
        { sourceId: e0, targetId: e1 },
        { sourceId: e2, targetId: e3 },
        { sourceId: e3, targetId: e0 },
      ] satisfies SourceAndTarget[]) {
        const caseProps = props(classFullName, pair.sourceId, pair.targetId);
        const expected = expectedDecision(targetDb, caseProps);
        if (expected.decision === "insert") continue;
        const caseImporter = new RecordingImporter(editTxn);
        expect(
          await caseImporter.decide(caseProps),
          `${classFullName} ${pair.sourceId}->${pair.targetId}`
        ).to.deep.equal(expected);
        editTxn.abandonChanges();
      }
    }

    // core returns the lowest member priority for the root class and same-class duplicates
    const lowestPriority = targetDb.createQueryReader(
      `SELECT ECInstanceId FROM ${groupsMembers} WHERE SourceECInstanceId=? AND TargetECInstanceId=? ORDER BY MemberPriority LIMIT 1`,
      new QueryBinder().bindId(1, e0).bindId(2, e1),
      { usePrimaryConn: true }
    );
    expect(await lowestPriority.step()).to.be.true;
    expect(
      (await importer.importAndDecide(props(groupsMembers, e0, e1))).id
    ).to.equal(lowestPriority.current[0]);
  });

  it("sees relationships that element deletes remove through cascades", async () => {
    const [e0, e1] = elementIds;
    const existingId = editTxn.insertRelationship(props(refersTo, e0, e1));
    expect(
      await importer.importAndDecide(props(refersTo, e0, e1))
    ).to.deep.equal({ decision: "none", id: existingId });

    const sourceProps = targetDb.elements.getElementProps(e0);
    await importer.deleteElements(new Set([e0]));
    expect(targetDb.relationships.tryGetInstance(refersTo, existingId)).to.be
      .undefined;
    // reuse the deleted source id, as preserveElementIdsForFiltering does
    editTxn.insertElement(sourceProps, { forceUseId: true });

    const reinserted = await importer.importAndDecide(props(refersTo, e0, e1));
    expect(reinserted.decision).to.equal("insert");
    expect(reinserted.id).to.not.equal(existingId);
  });

  it("sees relationships deleted through the importer", async () => {
    const [e0, e1] = elementIds;
    const existingId = editTxn.insertRelationship(props(refersTo, e0, e1));
    expect(
      (await importer.importAndDecide(props(refersTo, e0, e1))).id
    ).to.equal(existingId);

    await importer.deleteRelationship({
      id: existingId,
      classFullName: refersTo,
    });

    const reinserted = await importer.importAndDecide(props(refersTo, e0, e1));
    expect(reinserted.decision).to.equal("insert");
    expect(reinserted.id).to.not.equal(existingId);
  });

  it("preloads again after a schema import", async () => {
    const [e0, e1, e2, e3] = elementIds;
    const laterRefersTo = "TestRelLookupLater:LaterRefersTo";
    // Like core, a class missing from the target fails the lookup.
    await expect(
      importer.importRelationship(props(laterRefersTo, e0, e1))
    ).rejects.toThrow();
    expect(
      (await importer.importAndDecide(props(refersTo, e2, e3))).decision
    ).to.equal("insert");

    editTxn.end();
    await targetDb.importSchemaStrings([
      `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestRelLookupLater" alias="trll" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECRelationshipClass typeName="LaterRefersTo" strength="referencing" modifier="None">
    <BaseClass>bis:ElementRefersToElements</BaseClass>
    ${relationshipConstraints}
  </ECRelationshipClass>
</ECSchema>`,
    ]);
    editTxn.start();
    const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");
    const createQueryReader = vi.spyOn(targetDb, "createQueryReader");

    expect(
      (await importer.importAndDecide(props(laterRefersTo, e0, e1))).decision
    ).to.equal("insert");
    expect(
      (await importer.importAndDecide(props(refersTo, e2, e3))).decision
    ).to.equal("none");
    expect(tryGetInstance).not.toHaveBeenCalled();
    // a count and a preload for each class
    expect(createQueryReader).toHaveBeenCalledTimes(4);
  });

  it("reads external writes again after finalize", async () => {
    const [e0, e1, e2, e3] = elementIds;
    await importer.importAndDecide(props(refersTo, e0, e1));
    importer.finalize();

    // written directly between runs, not through the importer
    const externalId = editTxn.insertRelationship(props(refersTo, e2, e3));
    expect(
      await importer.importAndDecide(props(refersTo, e2, e3))
    ).to.deep.equal({ decision: "none", id: externalId });
  });

  it("keeps onInsertRelationship and onUpdateRelationship overrides working", async () => {
    const [e0, e1, e2, e3] = elementIds;
    // an override that writes a different value than the one it receives
    importer.insertOverride = async (insertProps) =>
      editTxn.insertRelationship({
        ...insertProps,
        memberPriority: 9,
      } as RelationshipProps);
    const inserted = await importer.importAndDecide(
      props(groupsMembers, e0, e1, { memberPriority: 1 })
    );
    expect(inserted.decision).to.equal("insert");
    expect(
      await importer.importAndDecide(
        props(groupsMembers, e0, e1, { memberPriority: 1 })
      )
    ).to.deep.equal({ decision: "update", id: inserted.id });

    // an override that skips the insert
    importer.insertOverride = async () => Id64.invalid;
    expect(
      (await importer.importAndDecide(props(groupsMembers, e2, e3))).decision
    ).to.equal("insert");
    expect(
      (await importer.importAndDecide(props(groupsMembers, e2, e3))).decision
    ).to.equal("insert");
    expect(
      targetDb.relationships.tryGetInstance(groupsMembers, {
        sourceId: e2,
        targetId: e3,
      })
    ).to.be.undefined;
  });
});
