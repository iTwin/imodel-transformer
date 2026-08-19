/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementOwnsChildElements,
  IModelDb,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { Code, IModel, PhysicalElementProps } from "@itwin/core-common";
import { expect, vi } from "vitest";
import * as path from "node:path";
import {
  ChangedInstanceIds,
  IModelExporter,
  IModelExportHandler,
} from "../../IModelExporter";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";

/** Recorded traversal event: callback kind, element id, and the isUpdate flag for exports. */
type ChangeTraversalEvent =
  | [kind: "should" | "pre" | "skip", id: Id64String]
  | [kind: "export", id: Id64String, isUpdate: boolean | undefined];

class RecordingHandler extends IModelExportHandler {
  public events: ChangeTraversalEvent[] = [];
  public rejectedIds = new Set<Id64String>();

  public get exportedIds(): Id64String[] {
    return this.events
      .filter(([kind]) => kind === "export")
      .map(([, id]) => id);
  }

  public get skippedIds(): Id64String[] {
    return this.events.filter(([kind]) => kind === "skip").map(([, id]) => id);
  }

  public override async shouldExportElement(element: Element) {
    this.events.push(["should", element.id]);
    return !this.rejectedIds.has(element.id);
  }

  public override async preExportElement(element: Element) {
    this.events.push(["pre", element.id]);
  }

  public override async onExportElement(
    element: Element,
    isUpdate: boolean | undefined
  ) {
    this.events.push(["export", element.id, isUpdate]);
  }

  public override async onSkipElement(elementId: Id64String) {
    this.events.push(["skip", elementId]);
  }
}

/** Inserts a subject tree below the root Subject. Node arrays are child specs. */
interface TreeSpec {
  name: string;
  children?: TreeSpec[];
}

function insertSubjectTree(
  db: IModelDb,
  specs: TreeSpec[]
): Map<string, Id64String> {
  return withEditTxn(db, "insert subject tree", (txn) => {
    const ids = new Map<string, Id64String>();
    const insertNode = (spec: TreeSpec, parentId: Id64String) => {
      const id = Subject.insert(txn, parentId, spec.name);
      ids.set(spec.name, id);
      for (const child of spec.children ?? []) insertNode(child, id);
    };
    for (const spec of specs) insertNode(spec, IModel.rootSubjectId);
    return ids;
  });
}

describe("IModelExporter changed-element traversal", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "ExporterChangedElementTraversal"
  );

  beforeAll(() => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir))
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    if (!IModelJsFs.existsSync(outputDir)) IModelJsFs.mkdirSync(outputDir);
  });

  function createSourceDb(testName: string): SnapshotDb {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "ExporterChangedElementTraversal",
      `${testName}.bim`
    );
    return SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: testName },
    });
  }

  /** The mixed hierarchy used by most tests:
   * A (A1 (A1a, A1b), A2), B (B1)
   */
  const mixedSpec: TreeSpec[] = [
    {
      name: "A",
      children: [
        { name: "A1", children: [{ name: "A1a" }, { name: "A1b" }] },
        { name: "A2" },
      ],
    },
    { name: "B", children: [{ name: "B1" }] },
  ];

  interface ChangesModeSetup {
    sourceDb: SnapshotDb;
    ids: Map<string, Id64String>;
    handler: RecordingHandler;
    exporter: IModelExporter;
    changes: ChangedInstanceIds;
  }

  /** Creates a snapshot with the mixed subject tree and an exporter in changes mode
   * (the RepositoryModel is pre-marked as updated, matching the LastMod behavior of
   * real changesets whenever a contained element changes).
   */
  function setupChangesMode(testName: string): ChangesModeSetup {
    const sourceDb = createSourceDb(testName);
    const ids = insertSubjectTree(sourceDb, mixedSpec);
    const handler = new RecordingHandler();
    const exporter = new IModelExporter(sourceDb);
    exporter.registerHandler(handler);
    const changes = new ChangedInstanceIds(sourceDb);
    changes.model.updateIds.add(IModel.repositoryModelId);
    exporter["_sourceDbChanges"] = changes;
    return { sourceDb, ids, handler, exporter, changes };
  }

  it("exports only changed elements with no callbacks for unchanged ancestors", async () => {
    const { sourceDb, ids, handler, exporter, changes } = setupChangesMode(
      "OnlyChangedExported"
    );
    try {
      // a deep inserted leaf and an updated root sibling; everything between is untouched
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.element.updateIds.add(ids.get("B")!);

      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.events).to.deep.equal([
        ["should", ids.get("A1a")!],
        ["pre", ids.get("A1a")!],
        ["export", ids.get("A1a")!, false],
        ["should", ids.get("B")!],
        ["pre", ids.get("B")!],
        ["export", ids.get("B")!, true],
      ]);
    } finally {
      sourceDb.close();
    }
  });

  it("orders changed parents before changed children depth-first", async () => {
    const { sourceDb, ids, handler, exporter, changes } =
      setupChangesMode("ParentBeforeChild");
    try {
      changes.element.updateIds.add(ids.get("A")!);
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.element.insertIds.add(ids.get("A2")!);
      changes.element.updateIds.add(ids.get("B1")!);

      await exporter.exportModelContents(IModel.repositoryModelId);

      // depth-first pre-order restricted to the changed set:
      // A, then its changed descendants (A1a before sibling-subtree A2), then B1
      expect(handler.exportedIds).to.deep.equal([
        ids.get("A")!,
        ids.get("A1a")!,
        ids.get("A2")!,
        ids.get("B1")!,
      ]);
      expect(handler.skippedIds).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  it("suppresses changed descendants of a rejected changed ancestor", async () => {
    const { sourceDb, ids, handler, exporter, changes } =
      setupChangesMode("RejectedAncestor");
    try {
      changes.element.updateIds.add(ids.get("A")!);
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.element.updateIds.add(ids.get("B")!);
      handler.rejectedIds.add(ids.get("A")!);

      await exporter.exportModelContents(IModel.repositoryModelId);

      // A is rejected via shouldExportElement: onSkipElement fires and the whole
      // subtree is pruned, dropping the changed descendant A1a silently
      expect(handler.events).to.deep.equal([
        ["should", ids.get("A")!],
        ["skip", ids.get("A")!],
        ["should", ids.get("B")!],
        ["pre", ids.get("B")!],
        ["export", ids.get("B")!, true],
      ]);
    } finally {
      sourceDb.close();
    }
  });

  it("fires onSkipElement for unchanged excluded elements and prunes their subtree", async () => {
    const { sourceDb, ids, handler, exporter, changes } =
      setupChangesMode("UnchangedExcluded");
    try {
      // A1 is untouched by the changeset but explicitly excluded; the exclusion
      // check runs before the changed-set check, so it still gets onSkipElement
      // and its changed descendant is suppressed
      exporter.excludeElement(ids.get("A1")!);
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.element.updateIds.add(ids.get("B")!);

      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.events).to.deep.equal([
        ["skip", ids.get("A1")!],
        ["should", ids.get("B")!],
        ["pre", ids.get("B")!],
        ["export", ids.get("B")!, true],
      ]);
    } finally {
      sourceDb.close();
    }
  });

  it("fires onSkipElement for changed excluded elements without loading them", async () => {
    const { sourceDb, ids, handler, exporter, changes } =
      setupChangesMode("ChangedExcluded");
    try {
      exporter.excludeElement(ids.get("A")!);
      changes.element.updateIds.add(ids.get("A")!);
      changes.element.insertIds.add(ids.get("A1a")!);

      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.events).to.deep.equal([["skip", ids.get("A")!]]);
    } finally {
      sourceDb.close();
    }
  });

  it("drops changed elements inside models absent from the changed-model set", async () => {
    const sourceDb = createSourceDb("UnchangedModelGate");
    try {
      const { modelId, objectId } = withEditTxn(
        sourceDb,
        "insert physical hierarchy",
        (txn) => {
          const physModelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysModel"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "Category",
            {}
          );
          const props: PhysicalElementProps = {
            category: categoryId,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
            model: physModelId,
          };
          return { modelId: physModelId, objectId: txn.insertElement(props) };
        }
      );
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      const changes = new ChangedInstanceIds(sourceDb);
      // the element changed but its model is NOT in the changed-model set;
      // exportModelContents relies on model LastMod bumping and skips everything
      changes.element.updateIds.add(objectId);
      exporter["_sourceDbChanges"] = changes;

      await exporter.exportModelContents(modelId);

      expect(handler.events).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  it("exports changed contents of a changed physical model with parent-child nesting", async () => {
    const sourceDb = createSourceDb("PhysicalNesting");
    try {
      const inserted = withEditTxn(
        sourceDb,
        "insert nested physical objects",
        (txn) => {
          const physModelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysModel"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "Category",
            {}
          );
          const props = (parentId?: Id64String): PhysicalElementProps => ({
            category: categoryId,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
            model: physModelId,
            parent: parentId
              ? new ElementOwnsChildElements(parentId)
              : undefined,
          });
          const topId = txn.insertElement(props());
          const midId = txn.insertElement(props(topId));
          const leafId = txn.insertElement(props(midId));
          return { physModelId, topId, midId, leafId };
        }
      );
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      const changes = new ChangedInstanceIds(sourceDb);
      changes.model.updateIds.add(inserted.physModelId);
      changes.element.insertIds.add(inserted.leafId);
      changes.element.updateIds.add(inserted.topId);
      exporter["_sourceDbChanges"] = changes;

      await exporter.exportModelContents(inserted.physModelId);

      // top (changed) exported before leaf (changed); mid (unchanged) is silent
      expect(handler.events).to.deep.equal([
        ["should", inserted.topId],
        ["pre", inserted.topId],
        ["export", inserted.topId, true],
        ["should", inserted.leafId],
        ["pre", inserted.leafId],
        ["export", inserted.leafId, false],
      ]);
    } finally {
      sourceDb.close();
    }
  });

  it("scopes exportChildElements to the given root's subtree in changes mode", async () => {
    const { sourceDb, ids, handler, exporter, changes } =
      setupChangesMode("ChildScope");
    try {
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.element.updateIds.add(ids.get("B1")!);

      await exporter.exportChildElements(ids.get("A")!);

      // B1 is outside A's subtree and must not be exported by this call
      expect(handler.events).to.deep.equal([
        ["should", ids.get("A1a")!],
        ["pre", ids.get("A1a")!],
        ["export", ids.get("A1a")!, false],
      ]);
    } finally {
      sourceDb.close();
    }
  });

  it("pairs exportChildElements(root) with skipRootSubject model contents like exportChanges does", async () => {
    const { sourceDb, ids, handler, exporter, changes } = setupChangesMode(
      "SkipRootPropagation"
    );
    try {
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.element.updateIds.add(ids.get("B")!);
      // the root Subject itself being "changed" must not export it in this mode
      changes.element.updateIds.add(IModel.rootSubjectId);

      // replicate the skipPropagateChangesToRootElements sequence from exportChanges
      await exporter.exportChildElements(IModel.rootSubjectId);
      await exporter.exportModelContents(
        IModel.repositoryModelId,
        Element.classFullName,
        true
      );

      expect(handler.exportedIds).to.deep.equal([
        ids.get("A1a")!,
        ids.get("B")!,
      ]);
      expect(handler.skippedIds).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  it("exports nothing when the changed-element set is empty", async () => {
    const { sourceDb, handler, exporter } = setupChangesMode("EmptyChanges");
    try {
      await exporter.exportModelContents(IModel.repositoryModelId);
      expect(handler.events).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  it("keeps visitElements=false a no-op in changes mode", async () => {
    const { sourceDb, ids, handler, exporter, changes } =
      setupChangesMode("VisitElementsOff");
    try {
      changes.element.insertIds.add(ids.get("A1a")!);
      exporter.visitElements = false;

      await exporter.exportModelContents(IModel.repositoryModelId);
      await exporter.exportChildElements(IModel.rootSubjectId);

      expect(handler.events).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  /** Forces the legacy per-element `queryChildren` traversal by overriding
   * `exportElement` with a trivial pass-through, which disables the direct
   * changed-element fast path.
   */
  class LegacyTraversalExporter extends IModelExporter {
    public override async exportElement(elementId: Id64String) {
      return super.exportElement(elementId);
    }
  }

  it("matches an explicit callback oracle on the legacy and direct paths", async () => {
    const runPath = async (useLegacy: boolean) => {
      const sourceDb = createSourceDb(
        `Parity${useLegacy ? "Legacy" : "Direct"}`
      );
      try {
        const ids = insertSubjectTree(sourceDb, mixedSpec);
        const handler = new RecordingHandler();
        const exporter = useLegacy
          ? new LegacyTraversalExporter(sourceDb)
          : new IModelExporter(sourceDb);
        exporter.registerHandler(handler);
        const changes = new ChangedInstanceIds(sourceDb);
        changes.model.updateIds.add(IModel.repositoryModelId);
        for (const name of ["A1a", "A2"]) {
          changes.element.insertIds.add(ids.get(name)!);
        }
        for (const name of ["A", "A1b", "B1"]) {
          changes.element.updateIds.add(ids.get(name)!);
        }
        handler.rejectedIds.add(ids.get("A1b")!);
        exporter.excludeElement(ids.get("A2")!);
        exporter.excludeElement(ids.get("B")!);
        exporter["_sourceDbChanges"] = changes;

        await exporter.exportModelContents(IModel.repositoryModelId);

        const idToName = new Map([...ids].map(([name, id]) => [id, name]));
        return handler.events.map(([kind, id, ...rest]) => [
          kind,
          idToName.get(id) ?? id,
          ...rest,
        ]);
      } finally {
        sourceDb.close();
      }
    };

    const expectedEvents = [
      ["should", "A"],
      ["pre", "A"],
      ["export", "A", true],
      ["should", "A1a"],
      ["pre", "A1a"],
      ["export", "A1a", false],
      ["should", "A1b"],
      ["skip", "A1b"],
      ["skip", "A2"],
      ["skip", "B"],
    ];
    expect(await runPath(true)).to.deep.equal(expectedEvents);
    expect(await runPath(false)).to.deep.equal(expectedEvents);
  });

  it("does not call queryChildren when exporting changes on the direct path", async () => {
    const { sourceDb, ids, handler, exporter, changes } =
      setupChangesMode("NoQueryChildren");
    try {
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.element.updateIds.add(ids.get("B")!);
      const queryChildrenSpy = vi.spyOn(sourceDb.elements, "queryChildren");

      await exporter.exportModelContents(IModel.repositoryModelId);
      await exporter.exportChildElements(ids.get("A")!);

      expect(handler.exportedIds).to.include(ids.get("A1a")!);
      expect(queryChildrenSpy).not.toHaveBeenCalled();
    } finally {
      sourceDb.close();
    }
  });

  it("builds one changed-element forest across models and scopes in exportChanges", async () => {
    const sourceDb = createSourceDb("OneForestPerExportChanges");
    try {
      const inserted = withEditTxn(
        sourceDb,
        "insert elements in multiple models",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "Category",
            {}
          );
          const modelAId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "ModelA"
          );
          const modelBId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "ModelB"
          );
          const insertObject = (modelId: Id64String) =>
            txn.insertElement({
              category: categoryId,
              classFullName: PhysicalObject.classFullName,
              code: Code.createEmpty(),
              model: modelId,
            } as PhysicalElementProps);
          return {
            modelAId,
            modelBId,
            objectAId: insertObject(modelAId),
            objectBId: insertObject(modelBId),
            repositoryElementId: Subject.insert(
              txn,
              IModel.rootSubjectId,
              "RepositoryElement"
            ),
          };
        }
      );
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      const changes = new ChangedInstanceIds(sourceDb);
      changes.model.updateIds.add(IModel.repositoryModelId);
      changes.model.updateIds.add(inserted.modelAId);
      changes.model.updateIds.add(inserted.modelBId);
      changes.element.insertIds.add(inserted.repositoryElementId);
      changes.element.insertIds.add(inserted.objectAId);
      changes.element.insertIds.add(inserted.objectBId);
      const isBriefcaseDb = vi
        .spyOn(sourceDb, "isBriefcaseDb")
        .mockReturnValue(true);
      const createQueryReader = vi.spyOn(sourceDb, "createQueryReader");
      try {
        await exporter.exportChanges({
          changedInstanceIds: changes,
          skipPropagateChangesToRootElements: true,
        });

        const hierarchyQueries = createQueryReader.mock.calls.filter(
          ([query]) =>
            String(query).includes("WITH RECURSIVE ChangedElementHierarchy")
        );
        expect(hierarchyQueries).to.have.lengthOf(1);
        expect(handler.exportedIds).to.include.members([
          inserted.repositoryElementId,
          inserted.objectAId,
          inserted.objectBId,
        ]);
      } finally {
        createQueryReader.mockRestore();
        isBriefcaseDb.mockRestore();
      }
    } finally {
      sourceDb.close();
    }
  });

  it("keeps the legacy path for exporter subclasses overriding element dispatch", async () => {
    const sourceDb = createSourceDb("SubclassLegacyPath");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      const exporter = new LegacyTraversalExporter(sourceDb);
      exporter.registerHandler(handler);
      const changes = new ChangedInstanceIds(sourceDb);
      changes.model.updateIds.add(IModel.repositoryModelId);
      changes.element.insertIds.add(ids.get("A1a")!);
      exporter["_sourceDbChanges"] = changes;
      const queryChildrenSpy = vi.spyOn(sourceDb.elements, "queryChildren");

      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.exportedIds).to.deep.equal([ids.get("A1a")!]);
      expect(queryChildrenSpy).toHaveBeenCalled();
    } finally {
      sourceDb.close();
    }
  });
});
