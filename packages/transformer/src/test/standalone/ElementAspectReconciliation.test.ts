/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect, vi } from "vitest";
import {
  EditTxn,
  ElementAspect,
  ElementOwnsExternalSourceAspects,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  ExternalSourceAspect,
  IModelDb,
  SnapshotDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Guid, Id64String } from "@itwin/core-bentley";
import {
  ElementAspectProps,
  ExternalSourceAspectProps,
  IModel,
} from "@itwin/core-common";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer";
import {
  CountingIModelImporter,
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

const schemaName = "AspectReconciliation";
const uniqueA = `${schemaName}:UniqueA`;
const multiA = `${schemaName}:MultiA`;
const multiB = `${schemaName}:MultiB`;
const derivedMultiA = `${schemaName}:DerivedMultiA`;
const derivedUniqueA = `${schemaName}:DerivedUniqueA`;

// Derived classes are declared first so their ECClassIds sort before their base classes.
const schemaXml = `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaName}" alias="ar" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="DerivedMultiA">
    <BaseClass>MultiA</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="DerivedUniqueA">
    <BaseClass>UniqueA</BaseClass>
  </ECEntityClass>
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

class AspectCountingImporter extends CountingIModelImporter {
  public numElementAspectsDeleted = 0;

  protected override async onDeleteElementAspect(
    aspect: ElementAspect
  ): Promise<void> {
    this.numElementAspectsDeleted++;
    await super.onDeleteElementAspect(aspect);
  }
}

interface AspectSummary {
  id: Id64String;
  classFullName: string;
  value: string;
}

/** Returns the test-schema aspects of an owner sorted by class and value. */
function readAspects(db: IModelDb, ownerId: Id64String): AspectSummary[] {
  return db.elements
    .getAspects(ownerId)
    .filter((aspect) => aspect.classFullName.startsWith(`${schemaName}:`))
    .map((aspect) => ({
      id: aspect.id,
      classFullName: aspect.classFullName,
      value: aspect.asAny.value as string,
    }))
    .sort((a, b) =>
      `${a.classFullName}/${a.value}`.localeCompare(
        `${b.classFullName}/${b.value}`
      )
    );
}

const withoutIds = (aspects: AspectSummary[]) =>
  aspects.map(({ classFullName, value }) => ({ classFullName, value }));

function insertAspect(
  txn: EditTxn,
  classFullName: string,
  ownerId: Id64String,
  value: string
): Id64String {
  const element =
    classFullName === uniqueA || classFullName === derivedUniqueA
      ? new ElementOwnsUniqueAspect(ownerId)
      : new ElementOwnsMultiAspects(ownerId);
  return txn.insertAspect({
    classFullName,
    element,
    value,
  } as ElementAspectProps);
}

describe("ElementAspect reconciliation", () => {
  let sourceDb: SnapshotDb;
  let targetDb: SnapshotDb;
  let owners: Id64String[];
  let sourceAspectIds: Record<string, Id64String>;

  beforeEach(async (context) => {
    const name = context.task.name.replace(/\W+/g, "-");
    sourceDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementAspectReconciliation",
        `${name}-Source.bim`
      ),
      { rootSubject: { name: "AspectReconciliationSource" } }
    );
    targetDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementAspectReconciliation",
        `${name}-Target.bim`
      ),
      { rootSubject: { name: "AspectReconciliationTarget" } }
    );
    for (const db of [sourceDb, targetDb]) {
      await db.importSchemaStrings([schemaXml]);
      withEditTxn(db, "save schema", () => {});
    }

    sourceAspectIds = {};
    owners = withEditTxn(sourceDb, "insert source aspects", (txn) => {
      const ownerIds = [0, 1, 2].map((i) =>
        Subject.insert(txn, IModel.rootSubjectId, `Owner${i}`)
      );
      const add = (key: string, classFullName: string, owner: number) => {
        sourceAspectIds[key] = insertAspect(
          txn,
          classFullName,
          ownerIds[owner],
          key
        );
      };
      add("o0-unique", uniqueA, 0);
      add("o0-a1", multiA, 0);
      add("o0-a2", multiA, 0);
      add("o0-b1", multiB, 0);
      add("o1-unique", uniqueA, 1);
      add("o1-a1", multiA, 1);
      add("o1-a2", multiA, 1);
      add("o1-a3", multiA, 1);
      add("o2-unique", uniqueA, 2);
      add("o2-a1", multiA, 2);
      add("o2-d1", derivedMultiA, 2);
      add("o2-b1", multiB, 2);
      return ownerIds;
    });
  });

  afterEach(() => {
    sourceDb.close();
    targetDb.close();
  });

  async function transform(
    run: (transformer: IModelTransformer) => Promise<void> = async (t) =>
      t.process(),
    options?: IModelTransformOptions
  ): Promise<{ importer: AspectCountingImporter; targetOwners: Id64String[] }> {
    const editTxn = createStartedEditTxn(targetDb);
    try {
      const importer = new AspectCountingImporter(editTxn);
      const transformer = new IModelTransformer(
        { source: sourceDb, target: importer },
        options
      );
      await run(transformer);
      const targetOwners = owners.map((id) =>
        transformer.context.findTargetElementId(id)
      );
      transformer.dispose();
      editTxn.end();
      return { importer, targetOwners };
    } catch (error) {
      editTxn.end("abandon");
      throw error;
    }
  }

  function expectTargetMatchesSource(targetOwners: Id64String[]): void {
    owners.forEach((sourceOwner, i) => {
      expect(withoutIds(readAspects(targetDb, targetOwners[i]))).to.deep.equal(
        withoutIds(readAspects(sourceDb, sourceOwner))
      );
    });
  }

  const expectAspectWrites = (
    importer: AspectCountingImporter,
    inserted: number,
    updated: number,
    deleted: number
  ) =>
    expect({
      inserted: importer.numElementAspectsInserted,
      updated: importer.numElementAspectsUpdated,
      deleted: importer.numElementAspectsDeleted,
    }).to.deep.equal({ inserted, updated, deleted });

  /** Spies on target reads that bypass the owner batch's loaded aspects until `stop` is called. */
  function spyOnTargetReads() {
    const getAspects = vi.spyOn(targetDb.elements, "getAspects");
    const queries = vi.spyOn(targetDb, "createQueryReader");
    return {
      stop: () => {
        const reads = {
          /** Owners whose aspects were read from the target instead of the batch. */
          owners: new Set(getAspects.mock.calls.map(([id]) => id)),
          /** Queries for unique aspects of a related class. */
          relatedUniqueQueries: queries.mock.calls.filter(([ecsql]) =>
            ecsql.includes("ClassHasAllBaseClasses")
          ).length,
        };
        getAspects.mockRestore();
        queries.mockRestore();
        return reads;
      },
    };
  }

  it("does not write aspects when processAll reruns without source changes", async () => {
    // A first import inserts each owner's unique aspect. The owners have no
    // unique aspect of another class, so the batch answers that without a query.
    const firstReads = spyOnTargetReads();
    const first = await transform();
    expect(firstReads.stop().relatedUniqueQueries).to.equal(0);
    expectAspectWrites(first.importer, 12, 0, 0);
    expectTargetMatchesSource(first.targetOwners);
    const before = first.targetOwners.map((id) => readAspects(targetDb, id));

    // Owner batches load their target aspects up front, so an unchanged rerun
    // does not read each owner's aspects separately.
    const getAspects = vi.spyOn(targetDb.elements, "getAspects");
    const second = await transform();
    expect(getAspects).not.toHaveBeenCalled();
    getAspects.mockRestore();
    expectAspectWrites(second.importer, 0, 0, 0);
    expect(
      second.targetOwners.map((id) => readAspects(targetDb, id))
    ).to.deep.equal(before);
  });

  it("does not write aspects when a subset operation reruns without source changes", async () => {
    const first = await transform();
    const before = first.targetOwners.map((id) => readAspects(targetDb, id));

    const second = await transform(async (transformer) => {
      await transformer.processChildElements(IModel.rootSubjectId);
    });
    expectAspectWrites(second.importer, 0, 0, 0);
    expect(
      second.targetOwners.map((id) => readAspects(targetDb, id))
    ).to.deep.equal(before);
  });

  it("applies only changed, added, and removed source aspects", async () => {
    const first = await transform();
    const before = first.targetOwners.map((id) => readAspects(targetDb, id));
    const targetId = (owner: number, value: string) =>
      before[owner].find((aspect) => aspect.value === value)!.id;

    withEditTxn(sourceDb, "change source aspects", (txn) => {
      // Owner 0: update the unique aspect and one multi-aspect, add a
      // multi-aspect, and remove the only aspect of MultiB.
      txn.updateAspect({
        ...sourceDb.elements.getAspect(sourceAspectIds["o0-unique"]).toJSON(),
        value: "o0-unique-changed",
      } as ElementAspectProps);
      txn.updateAspect({
        ...sourceDb.elements.getAspect(sourceAspectIds["o0-a2"]).toJSON(),
        value: "o0-a2-changed",
      } as ElementAspectProps);
      insertAspect(txn, multiA, owners[0], "o0-a3");
      txn.deleteAspect(sourceAspectIds["o0-b1"]);
      // Owner 1: remove the unique aspect and the first of three multi-aspects.
      txn.deleteAspect(sourceAspectIds["o1-unique"]);
      txn.deleteAspect(sourceAspectIds["o1-a1"]);
    });

    const reads = spyOnTargetReads();
    const second = await transform();
    // Only owners written during the batch are read from the target afterward.
    expect(reads.stop().owners).not.to.include(first.targetOwners[2]);
    expectTargetMatchesSource(second.targetOwners);
    // Updates: o0-unique, o0-a2, and o1's two remaining MultiA aspects, which
    // shift into the first two existing slots. Deletes: o0-b1, o1-unique, and
    // o1's surplus MultiA slot.
    expectAspectWrites(second.importer, 1, 4, 3);

    const after = second.targetOwners.map((id) => readAspects(targetDb, id));
    const afterId = (owner: number, value: string) =>
      after[owner].find((aspect) => aspect.value === value)?.id;
    expect(afterId(0, "o0-unique-changed")).to.equal(targetId(0, "o0-unique"));
    expect(afterId(0, "o0-a1")).to.equal(targetId(0, "o0-a1"));
    expect(afterId(0, "o0-a2-changed")).to.equal(targetId(0, "o0-a2"));
    // Multi-aspects match by position within a class: the remaining source
    // aspects take over the first two target slots and the last slot goes.
    expect(afterId(1, "o1-a2")).to.equal(targetId(1, "o1-a1"));
    expect(afterId(1, "o1-a3")).to.equal(targetId(1, "o1-a2"));
    expect(after[1].map(({ id }) => id)).not.to.include(targetId(1, "o1-a3"));
    expect(after[2]).to.deep.equal(before[2]);
  });

  it("matches multi-aspect counts when the source adds and removes aspects of a class", async () => {
    const first = await transform();
    const before = first.targetOwners.map((id) => readAspects(targetDb, id));

    withEditTxn(sourceDb, "grow and shrink multi-aspects", (txn) => {
      insertAspect(txn, multiB, owners[0], "o0-b2");
      insertAspect(txn, multiB, owners[0], "o0-b3");
      txn.deleteAspect(sourceAspectIds["o1-a2"]);
      txn.deleteAspect(sourceAspectIds["o1-a3"]);
    });
    const second = await transform();
    expectTargetMatchesSource(second.targetOwners);
    expectAspectWrites(second.importer, 2, 0, 2);
    expect(second.targetOwners).to.deep.equal(first.targetOwners);
    // The aspects that still have a source counterpart keep their IDs.
    const after = first.targetOwners.map((id) => readAspects(targetDb, id));
    const idOf = (aspects: AspectSummary[][], owner: number, value: string) =>
      aspects[owner].find((aspect) => aspect.value === value)?.id;
    expect(idOf(after, 0, "o0-b1")).to.equal(idOf(before, 0, "o0-b1"));
    expect(idOf(after, 1, "o1-a1")).to.equal(idOf(before, 1, "o1-a1"));
  });

  it("deletes nothing and discards loaded aspects when an owner batch fails", async () => {
    const first = await transform();
    const before = first.targetOwners.map((id) => readAspects(targetDb, id));
    withEditTxn(sourceDb, "remove a multi-aspect", (txn) => {
      txn.deleteAspect(sourceAspectIds["o0-b1"]);
    });

    class FailingTransformer extends IModelTransformer {
      public override async onExportElementMultiAspects(): Promise<void> {
        throw new Error("aspect export failed");
      }
    }
    const editTxn = createStartedEditTxn(targetDb);
    const importer = new AspectCountingImporter(editTxn);
    const transformer = new FailingTransformer({
      source: sourceDb,
      target: importer,
    });
    try {
      await expect(transformer.process()).rejects.toThrow(
        "aspect export failed"
      );
      // The uncommitted target still has every aspect, and nothing remains
      // loaded for later reads.
      expect(importer.numElementAspectsDeleted).to.equal(0);
      expect(
        first.targetOwners.map((id) => readAspects(targetDb, id))
      ).to.deep.equal(before);
      expect(
        importer.elementAspectCleanup.getAspects(first.targetOwners[0], multiB)
      ).to.equal(undefined);
    } finally {
      transformer.dispose();
      editTxn.end("abandon");
    }

    const second = await transform();
    expectTargetMatchesSource(second.targetOwners);
    expectAspectWrites(second.importer, 0, 0, 1);
  });

  // Unique aspects of one class hierarchy share a single slot per element, so
  // an included base-class aspect replaces an excluded derived-class one.
  it("replaces an excluded derived unique aspect when the source has its base class", async () => {
    const first = await transform();
    const owner0 = first.targetOwners[0];
    withEditTxn(targetDb, "use a target-only derived unique aspect", (txn) => {
      txn.deleteAspect(
        readAspects(targetDb, owner0).find(
          (aspect) => aspect.classFullName === uniqueA
        )!.id
      );
      insertAspect(txn, derivedUniqueA, owner0, "target-only");
    });

    const second = await transform(async (transformer) => {
      transformer.exporter.excludeElementAspectClass(derivedUniqueA);
      await transformer.process();
    });
    expectAspectWrites(second.importer, 1, 0, 1);
    expect(
      readAspects(targetDb, owner0)
        .filter(({ classFullName }) => classFullName !== multiA)
        .filter(({ classFullName }) => classFullName !== multiB)
        .map(({ classFullName, value }) => `${classFullName}=${value}`)
    ).to.deep.equal([`${uniqueA}=o0-unique`]);
  });

  it("follows a unique aspect that moves between a base and derived class", async () => {
    await transform();

    const derivedId = withEditTxn(sourceDb, "use derived class", (txn) => {
      txn.deleteAspect(sourceAspectIds["o0-unique"]);
      return insertAspect(txn, derivedUniqueA, owners[0], "o0-derived");
    });
    const second = await transform();
    expectTargetMatchesSource(second.targetOwners);
    expectAspectWrites(second.importer, 1, 0, 1);

    withEditTxn(sourceDb, "use base class", (txn) => {
      txn.deleteAspect(derivedId);
      insertAspect(txn, uniqueA, owners[0], "o0-base");
    });
    const third = await transform();
    expectTargetMatchesSource(third.targetOwners);
    expectAspectWrites(third.importer, 1, 0, 1);
  });

  // A source schema migration can move aspects to another class. The moved
  // aspects have no exact-class match, so they are replaced once.
  it.each([
    {
      to: "an unrelated class",
      owner: 1,
      keys: ["o1-a1", "o1-a2", "o1-a3"],
      classFullName: multiB,
    },
    {
      to: "their base class",
      owner: 2,
      keys: ["o2-d1"],
      classFullName: multiA,
    },
    {
      to: "a derived class",
      owner: 1,
      keys: ["o1-a1", "o1-a2", "o1-a3"],
      classFullName: derivedMultiA,
    },
  ])(
    "replaces multi-aspects moved to $to once and reuses them afterward",
    async ({ owner, keys, classFullName }) => {
      const first = await transform();
      const readTarget = (targetOwners: Id64String[]) =>
        targetOwners.map((targetOwner) => readAspects(targetDb, targetOwner));
      const before = readTarget(first.targetOwners);

      withEditTxn(sourceDb, "move aspects to another class", (txn) => {
        for (const key of keys) {
          txn.deleteAspect(sourceAspectIds[key]);
          insertAspect(txn, classFullName, owners[owner], key);
        }
      });
      const second = await transform();
      expectTargetMatchesSource(second.targetOwners);
      expectAspectWrites(second.importer, keys.length, 0, keys.length);
      const after = readTarget(second.targetOwners);
      after.forEach((aspects, i) => {
        if (i !== owner) expect(aspects).to.deep.equal(before[i]);
      });

      const third = await transform();
      expectAspectWrites(third.importer, 0, 0, 0);
      expect(readTarget(third.targetOwners)).to.deep.equal(after);
    }
  );

  it("keeps target-only property values and updates their aspects on each rerun", async () => {
    const first = await transform();
    // A target schema upgrade adds a property that the source doesn't have.
    const multiAClass = `<ECEntityClass typeName="MultiA">
    <BaseClass>bis:ElementMultiAspect</BaseClass>`;
    await targetDb.importSchemaStrings([
      schemaXml.replace('version="01.00.00"', 'version="01.00.01"').replace(
        multiAClass,
        `${multiAClass}
    <ECProperty propertyName="Extra" typeName="string"/>`
      ),
    ]);
    withEditTxn(targetDb, "save schema", () => {});

    const unset = await transform();
    expectAspectWrites(unset.importer, 0, 0, 0);

    const [aspect] = targetDb.elements.getAspects(
      first.targetOwners[1],
      multiA
    );
    withEditTxn(targetDb, "set target-only value", (txn) =>
      txn.updateAspect({
        ...aspect.toJSON(),
        extra: "target-only",
      } as ElementAspectProps)
    );
    // The change check sees the target-only value as a difference, but the
    // update does not clear it, so every rerun updates the aspect again.
    for (let run = 0; run < 2; run++) {
      const rerun = await transform();
      expectAspectWrites(rerun.importer, 0, 1, 0);
      expect(targetDb.elements.getAspect(aspect.id).asAny.extra).to.equal(
        "target-only"
      );
    }
  });

  it("reuses source ExternalSourceAspects whose scope maps to the target scope", async () => {
    // These source aspects are scoped to the root Subject, which is also the
    // default target scope, but they are not transformer provenance.
    withEditTxn(sourceDb, "insert source provenance", (txn) => {
      for (const identifier of ["doc-1", "doc-2"]) {
        txn.insertAspect({
          classFullName: ExternalSourceAspect.classFullName,
          element: new ElementOwnsExternalSourceAspects(owners[0]),
          scope: { id: IModel.rootSubjectId },
          identifier,
          kind: "Document",
        } as ExternalSourceAspectProps);
      }
    });
    const readEsas = (targetOwner: Id64String) =>
      (
        targetDb.elements.getAspects(
          targetOwner,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[]
      )
        .map(({ id, kind, identifier }) => ({ id, kind, identifier }))
        .sort((a, b) => a.id.localeCompare(b.id));
    const options = {
      includeSourceProvenance: true,
      forceExternalSourceAspectProvenance: true,
    };

    const first = await transform(undefined, options);
    const before = readEsas(first.targetOwners[0]);
    expect(before.map(({ kind }) => kind).sort()).to.deep.equal([
      "Document",
      "Document",
      ExternalSourceAspect.Kind.Element,
    ]);

    const second = await transform(undefined, options);
    expectAspectWrites(second.importer, 0, 0, 0);
    expect(readEsas(second.targetOwners[0])).to.deep.equal(before);
  });

  it("keeps element provenance of other target scopes on a shared element", async () => {
    // Without a FederationGuid, the owner's provenance is an Element
    // ExternalSourceAspect scoped to each target scope that imports it. Importing
    // the same source under two scopes maps both to one target element.
    withEditTxn(sourceDb, "drop FederationGuid", (txn) =>
      txn.updateElement({
        ...sourceDb.elements.getElementProps(owners[0]),
        federationGuid: Guid.empty,
      })
    );
    const [scope1, scope2] = withEditTxn(targetDb, "insert scopes", (txn) => [
      Subject.insert(txn, IModel.rootSubjectId, "Scope1"),
      Subject.insert(txn, IModel.rootSubjectId, "Scope2"),
    ]);
    const options = (targetScopeElementId: Id64String) => ({
      includeSourceProvenance: true,
      targetScopeElementId,
    });
    const elementProvenanceScopes = (targetOwner: Id64String) =>
      (
        targetDb.elements.getAspects(
          targetOwner,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[]
      )
        .filter((a) => a.kind === ExternalSourceAspect.Kind.Element)
        .map((a) => a.scope?.id)
        .sort();

    const first = await transform(undefined, options(scope1));
    const second = await transform(undefined, options(scope2));
    expect(second.targetOwners[0]).to.equal(first.targetOwners[0]);
    expect(elementProvenanceScopes(first.targetOwners[0])).to.deep.equal(
      [scope1, scope2].sort()
    );

    const rerun = await transform(undefined, options(scope1));
    expectAspectWrites(rerun.importer, 0, 0, 0);
    expect(elementProvenanceScopes(first.targetOwners[0])).to.deep.equal(
      [scope1, scope2].sort()
    );
    expectTargetMatchesSource(rerun.targetOwners);
  });

  it("reuses element provenance cloned from another transformation's scope", async () => {
    // The source got owners[0] by its own transformation into a scope Subject,
    // so it has that scope's Scope and Element ExternalSourceAspects. Cloned
    // into the target, they look like provenance of another target scope.
    withEditTxn(sourceDb, "clone upstream provenance", (txn) => {
      const scope = Subject.insert(txn, IModel.rootSubjectId, "Upstream");
      txn.insertAspect({
        classFullName: ExternalSourceAspect.classFullName,
        element: new ElementOwnsExternalSourceAspects(scope),
        scope: { id: IModel.rootSubjectId },
        identifier: Guid.createValue(),
        kind: ExternalSourceAspect.Kind.Scope,
      } as ExternalSourceAspectProps);
      txn.insertAspect({
        classFullName: ExternalSourceAspect.classFullName,
        element: new ElementOwnsExternalSourceAspects(owners[0]),
        scope: { id: scope },
        identifier: "0x123",
        kind: ExternalSourceAspect.Kind.Element,
      } as ExternalSourceAspectProps);
    });
    const options = { includeSourceProvenance: true };
    const upstreamProvenance = (targetOwner: Id64String) =>
      (
        targetDb.elements.getAspects(
          targetOwner,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[]
      ).filter((a) => a.identifier === "0x123");

    const first = await transform(undefined, options);
    expect(upstreamProvenance(first.targetOwners[0]).length).to.equal(1);
    // Its scope is the target copy of the upstream scope Subject.
    expect(upstreamProvenance(first.targetOwners[0])[0].scope?.id).to.equal(
      first.importer.targetDb.elements.queryElementIdByCode(
        Subject.createCode(targetDb, IModel.rootSubjectId, "Upstream")
      )
    );

    const second = await transform(undefined, options);
    expectAspectWrites(second.importer, 0, 0, 0);
    expect(upstreamProvenance(second.targetOwners[0]).length).to.equal(1);

    // Cleanup never deletes provenance, so the target keeps cloned provenance
    // that the source deletes.
    withEditTxn(sourceDb, "delete upstream provenance", (txn) => {
      for (const aspect of sourceDb.elements.getAspects(
        owners[0],
        ExternalSourceAspect.classFullName
      ))
        if ((aspect as ExternalSourceAspect).identifier === "0x123")
          txn.deleteAspect(aspect.id);
    });
    const third = await transform(undefined, options);
    expectAspectWrites(third.importer, 0, 0, 0);
    expect(upstreamProvenance(third.targetOwners[0]).length).to.equal(1);
  });

  describe("Scope ExternalSourceAspects cloned from the source", () => {
    // A source that was itself a transformation target carries Scope aspects on
    // ordinary elements. Only the current target scope's Scope aspect is the
    // transformer's own, so cloned ones follow the source.
    const options = { includeSourceProvenance: true };

    function insertScopeAspect(identifier: string): Id64String {
      return withEditTxn(sourceDb, "insert Scope aspect", (txn) =>
        txn.insertAspect({
          classFullName: ExternalSourceAspect.classFullName,
          element: new ElementOwnsExternalSourceAspects(owners[0]),
          scope: { id: IModel.rootSubjectId },
          identifier,
          kind: ExternalSourceAspect.Kind.Scope,
        } as ExternalSourceAspectProps)
      );
    }

    const scopeAspects = (targetOwner: Id64String) =>
      (
        targetDb.elements.getAspects(
          targetOwner,
          ExternalSourceAspect.classFullName
        ) as ExternalSourceAspect[]
      )
        .filter((a) => a.kind === ExternalSourceAspect.Kind.Scope)
        .map(({ scope, identifier }) => ({ scope: scope?.id, identifier }));

    it("follows the source when it replaces a cloned Scope aspect", async () => {
      const firstId = insertScopeAspect("First aspect");
      const first = await transform(undefined, options);
      expect(scopeAspects(first.targetOwners[0])).to.deep.equal([
        { scope: IModel.rootSubjectId, identifier: "First aspect" },
      ]);

      withEditTxn(sourceDb, "delete Scope aspect", (txn) =>
        txn.deleteAspect(firstId)
      );
      insertScopeAspect("Second aspect");
      const second = await transform(undefined, options);
      expect(scopeAspects(second.targetOwners[0])).to.deep.equal([
        { scope: IModel.rootSubjectId, identifier: "Second aspect" },
      ]);
    });

    it("follows the source when it changes a cloned Scope aspect's identifier", async () => {
      const aspectId = insertScopeAspect("Old identifier");
      await transform(undefined, options);

      withEditTxn(sourceDb, "change Scope aspect identifier", (txn) =>
        txn.updateAspect({
          ...sourceDb.elements.getAspect(aspectId).toJSON(),
          identifier: "New identifier",
        } as ExternalSourceAspectProps)
      );
      const second = await transform(undefined, options);
      expectAspectWrites(second.importer, 0, 1, 0);
      expect(scopeAspects(second.targetOwners[0])).to.deep.equal([
        { scope: IModel.rootSubjectId, identifier: "New identifier" },
      ]);
    });

    it("updates the file copy of a cloned Scope aspect whose scope is remapped", async () => {
      // The target starts as a file copy of the source, and the source root
      // Subject maps to another Subject, so the cloned aspect's scope no longer
      // matches the file copy's.
      insertScopeAspect("The coolest aspect");
      targetDb.close();
      targetDb = SnapshotDb.createFrom(
        sourceDb,
        IModelTransformerTestUtils.prepareOutputFile(
          "ElementAspectReconciliation",
          "ScopeAspectFileCopy-Target.bim"
        )
      );
      const channel = withEditTxn(targetDb, "insert channel", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "Channel")
      );
      const intoChannel = async (transformer: IModelTransformer) => {
        transformer.context.remapElement(IModel.rootSubjectId, channel);
        await transformer.process();
      };

      const first = await transform(intoChannel, options);
      expect(first.targetOwners[0]).to.equal(owners[0]);
      expect(scopeAspects(owners[0])).to.deep.equal([
        { scope: channel, identifier: "The coolest aspect" },
      ]);

      const second = await transform(intoChannel, options);
      expectAspectWrites(second.importer, 0, 0, 0);
      expect(scopeAspects(owners[0])).to.deep.equal([
        { scope: channel, identifier: "The coolest aspect" },
      ]);
    });
  });

  it("deletes target aspects of classes that became empty in the source", async () => {
    await transform();

    withEditTxn(sourceDb, "empty aspect classes", (txn) => {
      txn.deleteAspect(sourceAspectIds["o2-unique"]);
      txn.deleteAspect(sourceAspectIds["o2-a1"]);
      txn.deleteAspect(sourceAspectIds["o2-d1"]);
      txn.deleteAspect(sourceAspectIds["o2-b1"]);
    });
    const second = await transform();
    expectTargetMatchesSource(second.targetOwners);
    expect(readAspects(targetDb, second.targetOwners[2])).to.deep.equal([]);
    expectAspectWrites(second.importer, 0, 0, 4);
  });
});
