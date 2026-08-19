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
import {
  Code,
  IModel,
  PhysicalElementProps,
  QueryBinder,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { expect, vi } from "vitest";
import * as path from "node:path";
import {
  ChangedInstanceIds,
  IModelExporter,
  IModelExportHandler,
} from "../../IModelExporter";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";

/** Recorded traversal event: callback kind plus the element id it concerned. */
type TraversalEvent = [kind: "pre" | "export" | "skip", id: Id64String];

class RecordingHandler extends IModelExportHandler {
  public events: TraversalEvent[] = [];
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
    return !this.rejectedIds.has(element.id);
  }

  public override async preExportElement(element: Element) {
    this.events.push(["pre", element.id]);
  }

  public override async onExportElement(element: Element) {
    this.events.push(["export", element.id]);
  }

  public override async onSkipElement(elementId: Id64String) {
    this.events.push(["skip", elementId]);
  }
}

/** Depth-first reference traversal using the current per-element `queryChildren` contract:
 * top-level elements of the model in ECInstanceId order, then children recursively.
 */
async function referenceModelTraversal(
  db: IModelDb,
  modelId: Id64String
): Promise<Id64String[]> {
  const rootIds: Id64String[] = [];
  for await (const row of db.createQueryReader(
    "SELECT ECInstanceId FROM bis.Element WHERE Parent.Id IS NULL AND Model.Id=:modelId ORDER BY ECInstanceId",
    new QueryBinder().bindId("modelId", modelId)
  )) {
    rootIds.push(row.id);
  }
  const order: Id64String[] = [];
  const visit = (elementId: Id64String) => {
    order.push(elementId);
    for (const childId of db.elements.queryChildren(elementId)) visit(childId);
  };
  for (const rootId of rootIds) visit(rootId);
  return order;
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

describe("IModelExporter hierarchy traversal", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "ExporterHierarchyTraversal"
  );

  beforeAll(() => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir))
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    if (!IModelJsFs.existsSync(outputDir)) IModelJsFs.mkdirSync(outputDir);
  });

  function createSourceDb(testName: string): SnapshotDb {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "ExporterHierarchyTraversal",
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

  /** Forces the legacy per-element `queryChildren` traversal by overriding
   * `exportChildElements` with a pass-through, which disables the set-based path.
   */
  class LegacyTraversalExporter extends IModelExporter {
    public override async exportChildElements(elementId: Id64String) {
      return super.exportChildElements(elementId);
    }
  }

  it("preserves ECInstanceId order independent of unrelated inserts", async () => {
    const sourceDb = createSourceDb("QueryChildrenOrder");
    try {
      const fixture = withEditTxn(
        sourceDb,
        "insert interleaved subjects",
        (txn) => {
          const parentId = Subject.insert(txn, IModel.rootSubjectId, "parent");
          const childIds: Id64String[] = [];
          childIds.push(Subject.insert(txn, parentId, "third"));
          Subject.insert(txn, IModel.rootSubjectId, "unrelated-1");
          childIds.push(Subject.insert(txn, parentId, "first"));
          Subject.insert(txn, IModel.rootSubjectId, "unrelated-2");
          childIds.push(Subject.insert(txn, parentId, "second"));
          return { parentId, childIds };
        }
      );
      const expectedChildIds = [...fixture.childIds].sort((a, b) =>
        BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0
      );

      expect(sourceDb.elements.queryChildren(fixture.parentId)).to.deep.equal(
        expectedChildIds
      );

      const runTraversal = async (exporter: IModelExporter) => {
        const handler = new RecordingHandler();
        exporter.registerHandler(handler);
        await exporter.exportChildElements(fixture.parentId);
        return handler.exportedIds;
      };
      expect(await runTraversal(new IModelExporter(sourceDb))).to.deep.equal(
        expectedChildIds
      );
      expect(
        await runTraversal(new LegacyTraversalExporter(sourceDb))
      ).to.deep.equal(expectedChildIds);
    } finally {
      sourceDb.close();
    }
  });

  it("visits roots, siblings, and descendants in depth-first ECInstanceId order", async () => {
    const sourceDb = createSourceDb("DepthFirstOrder");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      await exporter.exportModelContents(IModel.repositoryModelId);

      const expectedOrder = await referenceModelTraversal(
        sourceDb,
        IModel.repositoryModelId
      );
      expect(handler.exportedIds).to.deep.equal(expectedOrder);

      // pin the parent-before-children and sibling-order relationships explicitly
      const exported = handler.exportedIds;
      const pos = (name: string) => exported.indexOf(ids.get(name)!);
      expect(pos("A")).to.be.greaterThanOrEqual(0);
      expect(pos("A1")).to.equal(pos("A") + 1); // first child follows parent
      expect(pos("A1a")).to.equal(pos("A1") + 1); // depth-first before uncle A2
      expect(pos("A1b")).to.equal(pos("A1a") + 1); // sibling id order
      expect(pos("A2")).to.equal(pos("A1b") + 1); // resumes after nephew subtree
      expect(pos("B")).to.be.greaterThan(pos("A2")); // root order
      expect(pos("B1")).to.equal(pos("B") + 1);

      // pin the per-element callback sequence: preExportElement immediately precedes onExportElement
      for (const [index, [kind, id]] of handler.events.entries()) {
        if (kind === "pre")
          expect(handler.events[index + 1]).to.deep.equal(["export", id]);
      }
    } finally {
      sourceDb.close();
    }
  });

  it("produces the reference order on deep and wide hierarchies", async () => {
    const sourceDb = createSourceDb("DeepAndWide");
    try {
      // deep spine of 40 subjects, each with one side child
      let deepSpec: TreeSpec = { name: "deep-39" };
      for (let i = 38; i >= 0; i--)
        deepSpec = {
          name: `deep-${i}`,
          children: [deepSpec, { name: `side-${i}` }],
        };
      // wide fan-out of 60 children
      const wideSpec: TreeSpec = {
        name: "wide",
        children: Array.from({ length: 60 }, (_, i) => ({
          name: `wide-${i}`,
        })),
      };
      insertSubjectTree(sourceDb, [deepSpec, wideSpec]);

      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.exportedIds).to.deep.equal(
        await referenceModelTraversal(sourceDb, IModel.repositoryModelId)
      );
    } finally {
      sourceDb.close();
    }
  });

  it("suppresses the whole subtree of an element rejected by the handler", async () => {
    const sourceDb = createSourceDb("SubtreeSuppression");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      handler.rejectedIds.add(ids.get("A1")!);
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.skippedIds).to.deep.equal([ids.get("A1")!]);
      const exported = new Set(handler.exportedIds);
      expect(exported.has(ids.get("A1")!)).to.be.false;
      expect(exported.has(ids.get("A1a")!)).to.be.false;
      expect(exported.has(ids.get("A1b")!)).to.be.false;
      // siblings and the rest of the tree are unaffected
      expect(exported.has(ids.get("A2")!)).to.be.true;
      expect(exported.has(ids.get("B1")!)).to.be.true;
      // A2 still directly follows its parent's remaining traversal
      const pos = (name: string) => handler.exportedIds.indexOf(ids.get(name)!);
      expect(pos("A2")).to.equal(pos("A") + 1);
    } finally {
      sourceDb.close();
    }
  });

  it("suppresses nested rejections independently", async () => {
    const sourceDb = createSourceDb("NestedSuppression");
    try {
      const ids = insertSubjectTree(sourceDb, [
        {
          name: "top",
          children: [
            {
              name: "mid",
              children: [{ name: "leaf1" }, { name: "leaf2" }],
            },
            { name: "mid2", children: [{ name: "leaf3" }] },
          ],
        },
      ]);
      const handler = new RecordingHandler();
      handler.rejectedIds.add(ids.get("mid")!);
      handler.rejectedIds.add(ids.get("leaf3")!);
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.skippedIds).to.deep.equal([
        ids.get("mid")!,
        ids.get("leaf3")!,
      ]);
      const exported = new Set(handler.exportedIds);
      expect(exported.has(ids.get("top")!)).to.be.true;
      expect(exported.has(ids.get("mid2")!)).to.be.true;
      expect(exported.has(ids.get("leaf1")!)).to.be.false;
      expect(exported.has(ids.get("leaf2")!)).to.be.false;
    } finally {
      sourceDb.close();
    }
  });

  it("skips excluded element ids without visiting their subtree", async () => {
    const sourceDb = createSourceDb("ExcludedIds");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      exporter.excludeElement(ids.get("A")!);
      await exporter.exportModelContents(IModel.repositoryModelId);

      expect(handler.skippedIds).to.include(ids.get("A")!);
      const exported = new Set(handler.exportedIds);
      for (const name of ["A", "A1", "A1a", "A1b", "A2"])
        expect(exported.has(ids.get(name)!), name).to.be.false;
      expect(exported.has(ids.get("B")!)).to.be.true;
    } finally {
      sourceDb.close();
    }
  });

  it("suppresses subtrees of elements excluded by category", async () => {
    const sourceDb = createSourceDb("CategoryExclusion");
    try {
      const fixture = withEditTxn(sourceDb, "insert physical tree", (txn) => {
        const excludedCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "ExcludedCategory",
          new SubCategoryAppearance()
        );
        const normalCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "NormalCategory",
          new SubCategoryAppearance()
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "PhysicalModel"
        );
        const makeProps = (
          categoryId: Id64String,
          parentId?: Id64String
        ): PhysicalElementProps => ({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: Code.createEmpty(),
          parent: parentId ? new ElementOwnsChildElements(parentId) : undefined,
        });
        const excludedParentId = txn.insertElement(
          makeProps(excludedCategoryId)
        );
        // child in a NON-excluded category is still suppressed with its parent
        const childOfExcludedId = txn.insertElement(
          makeProps(normalCategoryId, excludedParentId)
        );
        const normalRootId = txn.insertElement(makeProps(normalCategoryId));
        return {
          excludedCategoryId,
          modelId,
          excludedParentId,
          childOfExcludedId,
          normalRootId,
        };
      });

      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      exporter.excludeElementsInCategory(fixture.excludedCategoryId);
      await exporter.exportModelContents(fixture.modelId);

      const exported = new Set(handler.exportedIds);
      expect(exported.has(fixture.excludedParentId)).to.be.false;
      expect(exported.has(fixture.childOfExcludedId)).to.be.false;
      expect(exported.has(fixture.normalRootId)).to.be.true;
      expect(handler.skippedIds).to.include(fixture.excludedParentId);
    } finally {
      sourceDb.close();
    }
  });

  it("exports only descendants for explicit exportChildElements calls", async () => {
    const sourceDb = createSourceDb("ExplicitChildren");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      await exporter.exportChildElements(ids.get("A")!);

      const exported = handler.exportedIds;
      expect(exported).to.not.include(ids.get("A")!);
      expect(exported).to.deep.equal([
        ids.get("A1")!,
        ids.get("A1a")!,
        ids.get("A1b")!,
        ids.get("A2")!,
      ]);
    } finally {
      sourceDb.close();
    }
  });

  it("uses streamed traversal for exportAll across repository and submodel contents", async () => {
    const sourceDb = createSourceDb("ExportAllSubmodel");
    try {
      const fixture = withEditTxn(
        sourceDb,
        "insert exportAll hierarchy",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "ExportAllCategory",
            new SubCategoryAppearance()
          );
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "ExportAllPhysicalModel"
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
          };
          const modelRootId = txn.insertElement(physicalObjectProps);
          const modelChildId = txn.insertElement({
            ...physicalObjectProps,
            parent: new ElementOwnsChildElements(modelRootId),
          });
          const repositoryRootId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "ExportAllRepositoryRoot"
          );
          const repositoryChildId = Subject.insert(
            txn,
            repositoryRootId,
            "ExportAllRepositoryChild"
          );
          return {
            modelRootId,
            modelChildId,
            repositoryRootId,
            repositoryChildId,
          };
        }
      );

      const runExportAll = async (exporter: IModelExporter) => {
        const handler = new RecordingHandler();
        exporter.registerHandler(handler);
        const queryChildrenSpy = vi.spyOn(sourceDb.elements, "queryChildren");
        try {
          await exporter.exportAll();
          return {
            events: handler.events,
            queryChildrenCalls: queryChildrenSpy.mock.calls.length,
          };
        } finally {
          queryChildrenSpy.mockRestore();
        }
      };

      const streamed = await runExportAll(new IModelExporter(sourceDb));
      const legacy = await runExportAll(new LegacyTraversalExporter(sourceDb));
      expect(streamed.events).to.deep.equal(legacy.events);
      expect(streamed.queryChildrenCalls).to.equal(0);
      expect(legacy.queryChildrenCalls).to.be.greaterThan(0);

      const streamedExportedIds = streamed.events
        .filter(([kind]) => kind === "export")
        .map(([, id]) => id);
      for (const elementId of [
        fixture.modelRootId,
        fixture.modelChildId,
        fixture.repositoryRootId,
        fixture.repositoryChildId,
      ])
        expect(streamedExportedIds).to.include(elementId);
    } finally {
      vi.restoreAllMocks();
      sourceDb.close();
    }
  });

  it("preserves skipRootSubject behavior in streamed and legacy traversal", async () => {
    const sourceDb = createSourceDb("SkipRootSubject");
    try {
      insertSubjectTree(sourceDb, mixedSpec);

      const runTraversal = async (exporter: IModelExporter) => {
        const handler = new RecordingHandler();
        exporter.registerHandler(handler);
        await exporter.exportModelContents(
          IModel.repositoryModelId,
          Element.classFullName,
          true
        );
        return handler.events;
      };

      const streamed = await runTraversal(new IModelExporter(sourceDb));
      const legacy = await runTraversal(new LegacyTraversalExporter(sourceDb));
      expect(streamed).to.deep.equal(legacy);
      expect(streamed).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  it("skips traversal entirely when visitElements is false", async () => {
    const sourceDb = createSourceDb("VisitElementsFalse");
    try {
      insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      exporter.visitElements = false;
      await exporter.exportModelContents(IModel.repositoryModelId);
      await exporter.exportChildElements(IModel.rootSubjectId);
      expect(handler.events).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  it("yields to the event loop during model contents traversal", async () => {
    const sourceDb = createSourceDb("Yielding");
    try {
      insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      const allowYieldSpy = vi.spyOn(exporter["_yieldManager"], "allowYield");
      await exporter.exportModelContents(IModel.repositoryModelId);
      expect(allowYieldSpy).toHaveBeenCalled();
    } finally {
      vi.restoreAllMocks();
      sourceDb.close();
    }
  });

  it("yields while consuming descendants of a rejected subtree", async () => {
    const sourceDb = createSourceDb("YieldingRejectedSubtree");
    try {
      const childCount = 1_001;
      const parentId = withEditTxn(
        sourceDb,
        "insert rejected subtree",
        (txn) => {
          const rejectedParentId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "rejected-parent"
          );
          for (let index = 0; index < childCount; index++)
            Subject.insert(txn, rejectedParentId, `child-${index}`);
          return rejectedParentId;
        }
      );
      const modelElementIds: Id64String[] = [];
      for await (const row of sourceDb.createQueryReader(
        "SELECT ECInstanceId FROM bis.Element WHERE Model.Id=:modelId",
        new QueryBinder().bindId("modelId", IModel.repositoryModelId)
      ))
        modelElementIds.push(row.id);

      const handler = new RecordingHandler();
      handler.rejectedIds.add(parentId);
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      const allowYieldSpy = vi.spyOn(exporter["_yieldManager"], "allowYield");

      await exporter.exportModelContents(IModel.repositoryModelId);

      // The CTE emits every repository-model element, including descendants skipped
      // inside the rejected subtree.
      expect(modelElementIds.length).to.be.greaterThan(childCount);
      expect(allowYieldSpy).toHaveBeenCalledTimes(modelElementIds.length);
      expect(handler.skippedIds).to.deep.equal([parentId]);
    } finally {
      vi.restoreAllMocks();
      sourceDb.close();
    }
  });

  it("propagates handler failures and cleans up the export scope", async () => {
    const sourceDb = createSourceDb("FailureCleanup");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const failure = new Error("handler failure");
      class FailingHandler extends RecordingHandler {
        public override async onExportElement(element: Element) {
          if (element.id === ids.get("A1a")!) throw failure;
          await super.onExportElement(element);
        }
      }
      const handler = new FailingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);

      await expect(
        exporter.exportModelContents(IModel.repositoryModelId)
      ).rejects.toThrow(failure);
      // elements after the failure point were not exported
      expect(handler.exportedIds).to.not.include(ids.get("A1b")!);
      expect(handler.exportedIds).to.not.include(ids.get("B")!);
      // the aspect-owner scope was aborted, so a subsequent export starts cleanly
      expect(exporter.elementAspectExportCoordinator.isActive).to.be.false;

      handler.events = [];
      handler.rejectedIds.add(ids.get("A1a")!); // avoid re-throwing
      await exporter.exportModelContents(IModel.repositoryModelId);
      expect(handler.exportedIds).to.include(ids.get("B")!);
    } finally {
      sourceDb.close();
    }
  });

  it("honors subclass overrides of exportElement and exportChildElements", async () => {
    const sourceDb = createSourceDb("SubclassOverrides");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const exportElementCalls: Id64String[] = [];
      const childElementsCalls: Id64String[] = [];
      class OverridingExporter extends IModelExporter {
        public override async exportElement(elementId: Id64String) {
          exportElementCalls.push(elementId);
          return super.exportElement(elementId);
        }

        public override async exportChildElements(elementId: Id64String) {
          childElementsCalls.push(elementId);
          if (elementId === ids.get("A1")!) return; // consumer prunes this subtree
          return super.exportChildElements(elementId);
        }
      }
      const handler = new RecordingHandler();
      const exporter = new OverridingExporter(sourceDb);
      exporter.registerHandler(handler);
      await exporter.exportModelContents(IModel.repositoryModelId);

      // every visited element flows through the overridable methods
      expect(exportElementCalls).to.include(ids.get("A")!);
      expect(exportElementCalls).to.include(ids.get("A1")!);
      expect(childElementsCalls).to.include(ids.get("A1")!);
      // the consumer's pruning decision is respected
      const exported = new Set(handler.exportedIds);
      expect(exported.has(ids.get("A1")!)).to.be.true;
      expect(exported.has(ids.get("A1a")!)).to.be.false;
      expect(exported.has(ids.get("A1b")!)).to.be.false;
      expect(exported.has(ids.get("A2")!)).to.be.true;
    } finally {
      sourceDb.close();
    }
  });

  it("visits children of unchanged elements in changes mode", async () => {
    const sourceDb = createSourceDb("ChangesModeRecursion");
    try {
      const ids = insertSubjectTree(sourceDb, mixedSpec);
      const handler = new RecordingHandler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      // simulate changes mode where only a grandchild changed
      const changes = new ChangedInstanceIds(sourceDb);
      changes.element.insertIds.add(ids.get("A1a")!);
      changes.model.updateIds.add(IModel.repositoryModelId);
      exporter["_sourceDbChanges"] = changes;

      await exporter.exportModelContents(IModel.repositoryModelId);

      // unchanged ancestors are traversed but not exported; the changed leaf is exported
      expect(handler.exportedIds).to.deep.equal([ids.get("A1a")!]);
      expect(handler.skippedIds).to.deep.equal([]);
    } finally {
      sourceDb.close();
    }
  });

  it("produces identical event sequences on the streamed and legacy traversals", async () => {
    const sourceDb = createSourceDb("TraversalEquivalence");
    try {
      // mixed hierarchy: a deep spine with side children, plus a wide fan-out
      let spine: TreeSpec = { name: "spine-19" };
      for (let i = 18; i >= 0; i--)
        spine = {
          name: `spine-${i}`,
          children: [{ name: `limb-${i}` }, spine],
        };
      const wide: TreeSpec = {
        name: "fan",
        children: Array.from({ length: 40 }, (_, i) => ({
          name: `fan-${i}`,
          children: i % 3 === 0 ? [{ name: `fan-${i}-child` }] : undefined,
        })),
      };
      const ids = insertSubjectTree(sourceDb, [spine, wide]);
      const rejected = [ids.get("spine-5")!, ids.get("fan-9")!];

      const runTraversal = async (exporter: IModelExporter) => {
        const handler = new RecordingHandler();
        for (const id of rejected) handler.rejectedIds.add(id);
        exporter.registerHandler(handler);
        exporter.excludeElement(ids.get("fan-3")!);
        const queryChildrenSpy = vi.spyOn(sourceDb.elements, "queryChildren");
        await exporter.exportModelContents(IModel.repositoryModelId);
        const queryChildrenCalls = queryChildrenSpy.mock.calls.length;
        queryChildrenSpy.mockRestore();
        return { events: handler.events, queryChildrenCalls };
      };

      const streamed = await runTraversal(new IModelExporter(sourceDb));
      const legacy = await runTraversal(new LegacyTraversalExporter(sourceDb));

      expect(streamed.events).to.deep.equal(legacy.events);
      // the streamed traversal replaces per-element child lookups with one query
      expect(streamed.queryChildrenCalls).to.equal(0);
      expect(legacy.queryChildrenCalls).to.be.greaterThan(0);
    } finally {
      vi.restoreAllMocks();
      sourceDb.close();
    }
  });

  it("depends on SQLite ordered recursive-CTE queue semantics for depth-first order", async () => {
    // The set-based traversal relies on SQLite treating an ORDER BY inside a
    // recursive CTE as a priority queue: ordering by depth DESC then id ASC makes
    // the query emit rows in exact depth-first pre-order. This test documents that
    // dependency against the supported iTwin.js core range; if it fails, the
    // streamed traversal in IModelExporter can no longer assume pre-order rows.
    const sourceDb = createSourceDb("RecursiveCteDependency");
    try {
      insertSubjectTree(sourceDb, mixedSpec);
      const sql = `
        WITH RECURSIVE ElementTree (ECInstanceId, Depth) AS (
          SELECT e.ECInstanceId, 0 FROM bis.Element e
            WHERE e.Parent.Id IS NULL AND e.Model.Id=:modelId
          UNION ALL
          SELECT c.ECInstanceId, t.Depth + 1 FROM bis.Element c
            JOIN ElementTree t ON c.Parent.Id = t.ECInstanceId
          ORDER BY 2 DESC, 1 ASC
        )
        SELECT ECInstanceId, Depth FROM ElementTree`;
      const cteOrder: Id64String[] = [];
      for await (const row of sourceDb.createQueryReader(
        sql,
        new QueryBinder().bindId("modelId", IModel.repositoryModelId),
        { usePrimaryConn: true }
      )) {
        cteOrder.push(row[0]);
      }
      expect(cteOrder).to.deep.equal(
        await referenceModelTraversal(sourceDb, IModel.repositoryModelId)
      );
    } finally {
      sourceDb.close();
    }
  });
});
