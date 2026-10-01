/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect, vi } from "vitest";
import {
  EditTxn,
  Relationship,
  RelationshipProps,
  SourceAndTarget,
  StandaloneDb,
  Subject,
} from "@itwin/core-backend";
import { IModel, QueryBinder } from "@itwin/core-common";
import { Id64String } from "@itwin/core-bentley";
import { IModelImporter } from "../../IModelImporter";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

const testSchema = `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestRelImport" alias="tri" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECRelationshipClass typeName="TestRefersTo" strength="referencing" modifier="None">
    <BaseClass>bis:ElementRefersToElements</BaseClass>
    <ECProperty propertyName="Note" typeName="string"/>
    <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true">
      <Class class="bis:Element"/>
    </Source>
    <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true">
      <Class class="bis:Element"/>
    </Target>
  </ECRelationshipClass>
  <ECRelationshipClass typeName="TestRefersToChild" strength="referencing" modifier="None">
    <BaseClass>TestRefersTo</BaseClass>
    <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true">
      <Class class="bis:Element"/>
    </Source>
    <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true">
      <Class class="bis:Element"/>
    </Target>
  </ECRelationshipClass>
</ECSchema>`;

const refersTo = "TestRelImport:TestRefersTo";
const refersToChild = "TestRelImport:TestRefersToChild";
const groupsMembers = "BisCore:ElementGroupsMembers";
const elementRefersToElements = "BisCore:ElementRefersToElements";
const drivesElement = "BisCore:ElementDrivesElement";

class CountingImporter extends IModelImporter {
  public inserted: Id64String[] = [];
  public updated: RelationshipProps[] = [];

  protected override async onInsertRelationship(
    props: RelationshipProps
  ): Promise<Id64String> {
    const id = await super.onInsertRelationship(props);
    this.inserted.push(id);
    return id;
  }

  protected override async onUpdateRelationship(
    props: RelationshipProps
  ): Promise<void> {
    this.updated.push({ ...props });
    await super.onUpdateRelationship(props);
  }

  public findExisting(
    classFullName: string,
    sourceAndTarget: SourceAndTarget
  ): Relationship | undefined {
    return (this as any).tryGetExistingRelationship(
      classFullName,
      sourceAndTarget
    );
  }
}

describe("IModelImporter.importRelationship", () => {
  let targetDb: StandaloneDb;
  let editTxn: EditTxn;
  let importer: CountingImporter;
  let elementIds: Id64String[];
  let dbCount = 0;

  beforeEach(async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      `Relationships${++dbCount}.bim`
    );
    targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "Relationships" },
    });
    await targetDb.importSchemaStrings([testSchema]);
    editTxn = createStartedEditTxn(targetDb);
    elementIds = [0, 1, 2, 3, 4].map((i) =>
      Subject.create(targetDb, IModel.rootSubjectId, `Subject${i}`).insert(
        editTxn
      )
    );
    editTxn.saveChanges();
    importer = new CountingImporter(editTxn);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    if (editTxn.isActive) editTxn.end("abandon");
    targetDb.close();
  });

  const props = (
    classFullName: string,
    sourceId: Id64String,
    targetId: Id64String,
    extra: Record<string, unknown> = {}
  ): RelationshipProps => ({ classFullName, sourceId, targetId, ...extra });

  it("inserts a missing ElementRefersToElements relationship without the ECSQL lookup", async () => {
    const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");
    const [a, b] = elementIds;

    const id = await importer.importRelationship(
      props(refersTo, a, b, { note: "first" })
    );

    expect(importer.inserted).to.deep.equal([id]);
    expect(tryGetInstance).not.toHaveBeenCalled();
    expect(
      targetDb.relationships.getInstanceProps<
        RelationshipProps & { note?: string }
      >(refersTo, id).note
    ).to.equal("first");
  });

  it("finds relationships inserted earlier in the same run and updates only changed ones", async () => {
    const [a, b] = elementIds;
    const id = await importer.importRelationship(
      props(refersTo, a, b, { note: "first" })
    );
    const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");

    expect(
      await importer.importRelationship(
        props(refersTo, a, b, { note: "first" })
      )
    ).to.equal(id);
    expect(importer.updated).to.have.length(0);

    expect(
      await importer.importRelationship(
        props(refersTo, a, b, { note: "second" })
      )
    ).to.equal(id);
    expect(importer.inserted).to.deep.equal([id]);
    expect(importer.updated.map((p) => p.id)).to.deep.equal([id]);
    expect(
      targetDb.relationships.getInstanceProps<
        RelationshipProps & { note?: string }
      >(refersTo, id).note
    ).to.equal("second");
    // existing relationships are loaded by id
    expect(tryGetInstance).toHaveBeenCalledTimes(2);
    expect(tryGetInstance).toHaveBeenNthCalledWith(1, refersTo, id);
  });

  it("matches a subclass instance through a base-class import", async () => {
    const [a, b] = elementIds;
    const childId = editTxn.insertRelationship(props(refersToChild, a, b));

    const id = await importer.importRelationship(props(refersTo, a, b));

    expect(id).to.equal(childId);
    expect(importer.inserted).to.have.length(0);
  });

  it("does not match a base-class instance through a subclass import", async () => {
    const [a, b] = elementIds;
    const baseId = editTxn.insertRelationship(props(refersTo, a, b));

    const id = await importer.importRelationship(props(refersToChild, a, b));

    expect(id).to.not.equal(baseId);
    expect(importer.inserted).to.deep.equal([id]);
  });

  it("returns the same relationship as core for member priority duplicates", () => {
    const [a, b, c, d] = elementIds;
    // inserted so that id order differs from priority order
    editTxn.insertRelationship(
      props(groupsMembers, a, b, { memberPriority: 5 })
    );
    const lowestPriorityId = editTxn.insertRelationship(
      props(groupsMembers, a, b, { memberPriority: 1 })
    );
    editTxn.insertRelationship(props(refersTo, c, d));

    const pairs: SourceAndTarget[] = [
      { sourceId: a, targetId: b },
      { sourceId: c, targetId: d },
      { sourceId: d, targetId: a },
    ];
    let matched = 0;
    for (const classFullName of [
      elementRefersToElements,
      groupsMembers,
      refersTo,
      refersToChild,
    ]) {
      for (const pair of pairs) {
        const expected = targetDb.relationships.tryGetInstance(
          classFullName,
          pair
        );
        const actual = importer.findExisting(classFullName, pair);
        expect(actual?.id, `${classFullName} ${pair.sourceId}`).to.equal(
          expected?.id
        );
        expect(actual?.classFullName).to.equal(expected?.classFullName);
        if (expected) matched++;
      }
    }
    expect(matched).to.equal(4);
    expect(
      importer.findExisting(groupsMembers, { sourceId: a, targetId: b })?.id
    ).to.equal(lowestPriorityId);
  });

  it("prefers the lowest class id and then member priority when several classes match", async () => {
    const [a, b] = elementIds;
    editTxn.insertRelationship(props(refersToChild, a, b));
    editTxn.insertRelationship(
      props(groupsMembers, a, b, { memberPriority: 2 })
    );
    editTxn.insertRelationship(props(refersTo, a, b));
    editTxn.insertRelationship(
      props(groupsMembers, a, b, { memberPriority: 1 })
    );
    const pair = { sourceId: a, targetId: b };

    for (const classFullName of [
      elementRefersToElements,
      refersTo,
      refersToChild,
    ]) {
      const reader = targetDb.createQueryReader(
        `SELECT ECInstanceId FROM ${classFullName} WHERE SourceECInstanceId=? AND TargetECInstanceId=? ORDER BY ECClassId, MemberPriority LIMIT 1`,
        new QueryBinder().bindId(1, a).bindId(2, b),
        { usePrimaryConn: true }
      );
      expect(await reader.step()).to.be.true;
      expect(importer.findExisting(classFullName, pair)?.id).to.equal(
        reader.current[0]
      );
    }
    // core's root-class query scans the same unique index
    expect(importer.findExisting(elementRefersToElements, pair)?.id).to.equal(
      targetDb.relationships.tryGetInstance(elementRefersToElements, pair)?.id
    );
  });

  it("falls back to core for relationships outside ElementRefersToElements", async () => {
    const [a, b] = elementIds;
    const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");

    const id = await importer.importRelationship(
      props(drivesElement, a, b, { priority: 1 })
    );
    expect(
      await importer.importRelationship(
        props(drivesElement, a, b, { priority: 1 })
      )
    ).to.equal(id);

    expect(importer.inserted).to.deep.equal([id]);
    expect(tryGetInstance).toHaveBeenCalledTimes(2);
    for (const call of tryGetInstance.mock.calls)
      expect(call).to.deep.equal([drivesElement, { sourceId: a, targetId: b }]);
  });

  it("falls back to core for classes missing from the target and resolves them again later", async () => {
    const [a, b] = elementIds;
    const missing = "TestRelImportLater:LaterRefersTo";
    const tryGetInstance = vi.spyOn(targetDb.relationships, "tryGetInstance");

    expect(() =>
      importer.findExisting(missing, { sourceId: a, targetId: b })
    ).to.throw();
    expect(tryGetInstance).toHaveBeenCalledWith(missing, {
      sourceId: a,
      targetId: b,
    });

    editTxn.saveChanges();
    editTxn.end();
    await targetDb.importSchemaStrings([
      testSchema
        .replace(
          'schemaName="TestRelImport" alias="tri"',
          'schemaName="TestRelImportLater" alias="tril"'
        )
        .replace('typeName="TestRefersTo"', 'typeName="LaterRefersTo"')
        .replace(
          "<BaseClass>TestRefersTo</BaseClass>",
          "<BaseClass>LaterRefersTo</BaseClass>"
        ),
    ]);
    tryGetInstance.mockClear();

    // the same importer resolves the class now that it exists
    expect(importer.findExisting(missing, { sourceId: a, targetId: b })).to.be
      .undefined;
    expect(tryGetInstance).not.toHaveBeenCalled();
  });
});
