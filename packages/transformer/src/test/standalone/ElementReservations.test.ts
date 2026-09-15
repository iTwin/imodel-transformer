/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseManager,
  DefinitionModel,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  ElementMultiAspect,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Guid, Id64, Logger } from "@itwin/core-bentley";
import {
  BriefcaseIdValue,
  Code,
  CodeScopeSpec,
  CodeSpec,
  ElementProps,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  ChangedInstanceIds,
  IModelExporter,
  IModelExportHandler,
} from "../../IModelExporter";
import { IModelTransformer } from "../../IModelTransformer";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import * as TestUtils from "../TestUtils";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { useLocalReservations } from "./ElementReservationTestUtils";

const outputFile = (name: string) =>
  IModelTransformerTestUtils.prepareOutputFile("ElementReservations", name);
const reservationError = (key: IModelTransformerError) => ({
  iTwinErrorId: { scope: IModelTransformerErrorScope, key },
});

function createScenario(name: string) {
  using resources = new DisposableStack();
  const source = resources.adopt(
    StandaloneDb.createEmpty(outputFile(`${name}-source.bim`), {
      rootSubject: { name },
    }),
    (db) => db.close()
  );
  const target = resources.adopt(
    StandaloneDb.createEmpty(outputFile(`${name}-target.bim`), {
      rootSubject: { name },
    }),
    (db) => {
      if (db.isOpen) db.close();
    }
  );
  const owned = resources.move();
  return {
    source,
    target,
    async start(
      transformerClass = IModelTransformer,
      options: ConstructorParameters<typeof IModelTransformer>[1] = {
        noProvenance: true,
      }
    ) {
      const reservations = owned.adopt(
        await useLocalReservations(target, outputFile(`${name}-sync.db`)),
        (adapter) => adapter.dispose()
      );
      const txn = owned.adopt(createStartedEditTxn(target), (edit) =>
        edit.end()
      );
      const transformer = owned.adopt(
        new transformerClass({ source, target: txn }, options),
        (instance) => instance.dispose()
      );
      return { reservations, txn, transformer };
    },
    [Symbol.dispose]: () => owned.dispose(),
  };
}

describe("automatic bulk reservations", () => {
  it("reserves a selected parent and 24 coded children once, before any insertion", async () => {
    using s = createScenario("parent");
    const name = "ParentCodes";
    const spec = withEditTxn(s.source, "source code spec", (txn) =>
      s.source.codeSpecs.insert(
        txn,
        CodeSpec.create(s.source, name, CodeScopeSpec.Type.ParentElement)
      )
    );
    const targetSpec = withEditTxn(s.target, "target code specs", (txn) => {
      s.target.codeSpecs.insert(
        txn,
        CodeSpec.create(s.target, "padding", CodeScopeSpec.Type.Repository)
      );
      return s.target.codeSpecs.insert(
        txn,
        CodeSpec.create(s.target, name, CodeScopeSpec.Type.ParentElement)
      );
    });
    const ids = withEditTxn(s.source, "hierarchy", (txn) => {
      const insertSubject = (scopeId: string, value: string) =>
        txn.insertElement({
          ...Subject.create(s.source, scopeId, value).toJSON(),
          federationGuid: Guid.createValue(),
          code: { spec, scope: scopeId, value },
        });
      const parent = insertSubject(IModel.rootSubjectId, "parent");
      return {
        parent,
        children: Array.from({ length: 24 }, (_, i) =>
          insertSubject(parent, `child-${i}`)
        ),
        excluded: insertSubject(parent, "excluded"),
      };
    });
    const { reservations, txn: targetTxn, transformer } = await s.start();
    transformer.context.remapCodeSpec(name, name);
    transformer.exporter.excludeElement(ids.excluded);
    const insert = vi.spyOn(targetTxn, "insertElement");
    const reserve = s.target.reservations.reserveElements.bind(
      s.target.reservations
    );
    const batch = vi
      .spyOn(s.target.reservations, "reserveElements")
      .mockImplementation(async (args) => {
        expect(insert).not.toHaveBeenCalled();
        expect([...args.elements]).toHaveLength(25);
        await reserve(args);
      });
    const finalize = vi.spyOn(transformer.importer, "finalize");
    const save = vi.spyOn(targetTxn, "saveChanges");
    await transformer.processElement(ids.parent);
    expect(batch).toHaveBeenCalledTimes(1);
    expect(insert).toHaveBeenCalledTimes(25);
    expect(spec).not.toBe(targetSpec);
    const parentId = transformer.context.findTargetElementId(ids.parent);
    for (const id of [ids.parent, ...ids.children]) {
      const targetId = transformer.context.findTargetElementId(id);
      expect(targetId).toBe(
        reservations.sync.findReservedElement(
          s.source.elements.getElement(id).federationGuid!
        )?.elementId
      );
      expect(targetId).not.toBe(id);
      expect(s.target.elements.getElementProps(targetId).code).toMatchObject({
        spec: targetSpec,
        scope: id === ids.parent ? IModel.rootSubjectId : parentId,
      });
    }
    expect(transformer.context.findTargetElementId(ids.excluded)).toBe(
      Id64.invalid
    );
    await transformer.processElement(ids.parent);
    expect(batch).toHaveBeenCalledTimes(1);
    expect(finalize).not.toHaveBeenCalled();
    expect(save).not.toHaveBeenCalled();
  });

  it("rethrows a real conflicting batch once, without writes, finalization, or losing prior reservations", async () => {
    using s = createScenario("batch-failure");
    const ids = withEditTxn(s.source, "source elements", (txn) =>
      ["fresh", "conflict"].map((name) =>
        Subject.insert(txn, IModel.rootSubjectId, name)
      )
    );
    const {
      reservations,
      txn: targetTxn,
      transformer,
    } = await s.start(IModelTransformer, {});
    const [fresh, conflict] = ids.map((id) =>
      s.source.elements.getElementProps(id)
    );
    await s.target.reservations.reserveElements({
      elements: [
        {
          ...conflict,
          federationGuid: conflict.federationGuid!,
          code: { ...conflict.code, value: "different" },
        },
      ],
    });
    const prior = reservations.sync.findReservedElement(
      conflict.federationGuid!
    )!;
    const mutations = [
      "insertElement",
      "updateElement",
      "deleteElement",
      "insertModel",
      "insertAspect",
      "insertRelationship",
      "saveChanges",
    ] as const;
    const writes = mutations.map((method) => vi.spyOn(targetTxn, method));
    const finalize = vi.spyOn(transformer.importer, "finalize");
    const log = vi.spyOn(Logger, "logError");
    const error = await transformer
      .process()
      .catch((caught: unknown) => caught);
    expect(error).toBeDefined();
    expect(error).toBe(reservations.lastError);
    expect(reservations.calls).toBe(2);
    for (const write of writes) expect(write).not.toHaveBeenCalled();
    expect(finalize).not.toHaveBeenCalled();
    expect(
      log.mock.calls.filter(
        ([, , metadata]) =>
          (metadata as { phase?: string })?.phase === "reserve-elements"
      )
    ).toEqual([
      [
        expect.any(String),
        error,
        expect.objectContaining({
          phase: "reserve-elements",
          batchSize: 2,
          error,
        }),
      ],
    ]);
    expect(
      reservations.sync.findReservedElement(fresh.federationGuid!)
    ).toBeUndefined();
    expect(
      reservations.sync.findReservedElement(conflict.federationGuid!)?.elementId
    ).toBe(prior.elementId);
    await s.target.reservations.reserveElements({
      elements: [{ ...fresh, federationGuid: fresh.federationGuid! }],
    });
    expect(
      reservations.sync.findReservedElement(fresh.federationGuid!)?.elementId
    ).toBe(
      Id64.fromLocalAndBriefcaseIds(
        Id64.getLocalId(prior.elementId) + 1,
        Id64.getBriefcaseId(prior.elementId)
      )
    );
  });

  it.each(["guid", "class", "code"])(
    "rejects importer identity drift at the native insertion boundary (%s)",
    async (mutation) => {
      using s = createScenario(`importer-${mutation}`);
      const id = withEditTxn(s.source, "source", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "source")
      );
      const { reservations, txn: targetTxn, transformer } = await s.start();
      const importer = transformer.importer as unknown as {
        onInsertElement(props: ElementProps): Promise<string>;
      };
      const onInsert = importer.onInsertElement.bind(importer);
      vi.spyOn(importer, "onInsertElement").mockImplementation(
        async (props) => {
          if (mutation === "guid") props.federationGuid = Guid.createValue();
          else if (mutation === "class")
            props.classFullName = "BisCore:Category";
          else props.code.value = "changed";
          return onInsert(props);
        }
      );
      const insert = vi.spyOn(targetTxn, "insertElement");
      await expect(transformer.processElement(id)).rejects.toMatchObject(
        reservationError(IModelTransformerError.ElementReservationPlanChanged)
      );
      expect(reservations.calls).toBe(1);
      expect(insert).not.toHaveBeenCalled();
    }
  );

  it("rejects duplicate pending identities before reservation", async () => {
    using s = createScenario("duplicates");
    const ids = withEditTxn(s.source, "source", (txn) =>
      ["first", "second"].map((name) =>
        Subject.insert(txn, IModel.rootSubjectId, name)
      )
    );
    const guid = Guid.createValue();
    class DuplicateTransformer extends IModelTransformer {
      public override async onTransformElement(
        element: Element
      ): Promise<ElementProps> {
        const props = await super.onTransformElement(element);
        if (ids.includes(element.id)) props.federationGuid = guid;
        return props;
      }
    }
    const {
      reservations,
      txn: targetTxn,
      transformer,
    } = await s.start(DuplicateTransformer);
    const insert = vi.spyOn(targetTxn, "insertElement");
    await expect(
      transformer.processChildElements(IModel.rootSubjectId)
    ).rejects.toMatchObject(
      reservationError(IModelTransformerError.ElementReservationUnsupported)
    );
    expect(reservations.calls).toBe(0);
    expect(insert).not.toHaveBeenCalled();
  });

  it.each([false, true])(
    "defers nested subject repairs and resets their count after success or failure (%s)",
    async (failReservation) => {
      using s = createScenario(`nested-${failReservation}`);
      const subjects = withEditTxn(s.source, "source subjects", (txn) =>
        ["first", "second"].map((name) => {
          const parent = Subject.insert(txn, IModel.rootSubjectId, name);
          return {
            parent,
            child: Subject.insert(txn, parent, `${name}-child`),
          };
        })
      );
      const destinations = withEditTxn(s.target, "target subjects", (txn) =>
        subjects.map((_, i) =>
          Subject.insert(txn, IModel.rootSubjectId, `destination-${i}`)
        )
      );
      const trace: string[] = [];
      class NestedTransformer extends IModelTransformer {
        private _visited = false;
        public override async initialize(): Promise<void> {
          await super.initialize();
          if (this._visited) return;
          this._visited = true;
          for (const [i, subject] of subjects.entries())
            await this.processSubject(subject.parent, destinations[i]);
          expect(trace).toEqual([]);
        }
        public override async onExportElement(element: Element): Promise<void> {
          trace.push("element");
          await super.onExportElement(element);
        }
        protected override async completePartiallyCommittedElements(): Promise<void> {
          trace.push("repair-elements");
          await super.completePartiallyCommittedElements();
        }
        protected override async completePartiallyCommittedAspects(): Promise<void> {
          trace.push("repair-aspects");
          await super.completePartiallyCommittedAspects();
        }
      }
      const {
        reservations,
        txn: targetTxn,
        transformer,
      } = await s.start(NestedTransformer);
      const finalize = vi.spyOn(transformer.importer, "finalize");
      const save = vi.spyOn(targetTxn, "saveChanges");
      const failure = new Error("reservation failure");
      if (failReservation) {
        vi.spyOn(
          s.target.reservations,
          "reserveElements"
        ).mockRejectedValueOnce(failure);
        await expect(
          transformer.processRelationships(ElementGroupsMembers.classFullName)
        ).rejects.toBe(failure);
        expect(trace).toEqual([]);
      } else {
        await transformer.processRelationships(
          ElementGroupsMembers.classFullName
        );
        expect(trace).toEqual([
          "element",
          "element",
          "repair-elements",
          "repair-aspects",
          "repair-elements",
          "repair-aspects",
        ]);
        for (const [i, subject] of subjects.entries())
          expect(
            s.target.elements.getElementProps(
              transformer.context.findTargetElementId(subject.child)
            )
          ).toMatchObject({
            parent: { id: destinations[i] },
            code: { scope: destinations[i] },
            model: IModel.repositoryModelId,
          });
      }
      const previous = [...trace];
      await transformer.processRelationships(
        ElementGroupsMembers.classFullName
      );
      expect(trace).toEqual(previous);
      expect(reservations.calls).toBe(failReservation ? 0 : 1);
      expect(finalize).not.toHaveBeenCalled();
      expect(save).not.toHaveBeenCalled();
    }
  );
});

async function createHubPair(name: string, targetNoLocks = true) {
  await using resources = new AsyncDisposableStack();
  // Close/delete briefcases before restoring the SchemaSync access hook.
  const adapterCleanup = resources.use(new DisposableStack());
  const accessToken = await HubWrappers.getAccessToken(
    TestUtils.TestUserType.Regular
  );
  const iTwinId = transformerTestHub.iTwinId;
  const open = async (side: string, noLocks: boolean) => {
    const iModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: `${name}-${side}`,
      ...(noLocks ? { noLocks: true } : {}),
    });
    resources.defer(async () =>
      transformerTestHub.deleteIModel({ accessToken, iTwinId, iModelId })
    );
    return resources.adopt(
      await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId,
      }),
      async (db) => HubWrappers.closeAndDeleteBriefcaseDb(accessToken, db)
    );
  };
  const source = await open("source", true);
  const target = await open("target", targetNoLocks);
  const reservations = adapterCleanup.adopt(
    await useLocalReservations(target, outputFile(`${name}-sync.db`)),
    (adapter) => adapter.dispose()
  );
  const owned = resources.move();
  return {
    source,
    target,
    accessToken,
    reservations,
    [Symbol.asyncDispose]: async () => owned.disposeAsync(),
  };
}

async function initializeHubTarget(
  pair: Awaited<ReturnType<typeof createHubPair>>
) {
  await pair.source.pushChanges({
    accessToken: pair.accessToken,
    description: "source baseline",
  });
  await withEditTxn(pair.target, "target baseline", async (txn) => {
    const transformer = new IModelTransformer({
      source: pair.source,
      target: txn,
    });
    try {
      await transformer.process();
    } finally {
      transformer.dispose();
    }
  });
  await pair.target.pushChanges({
    accessToken: pair.accessToken,
    description: "target baseline",
  });
}

describe("incremental reservation replay", () => {
  let ownsHub = false;
  let restoreDownload: (() => void) | undefined;
  beforeAll(() => {
    if (!transformerTestHub.isActive) {
      transformerTestHub.start(
        "ElementReservationsHub",
        KnownTestLocations.outputDir
      );
      ownsHub = true;
    }
    restoreDownload = installCheckpointDownload(transformerTestHub);
  });
  afterAll(() => {
    restoreDownload?.();
    if (ownsHub) transformerTestHub.stop();
  });

  it.each([false, true])(
    "replays changes with live metadata and reserves only missing targets (%s)",
    async (removeUpdateTarget) => {
      await using pair = await createHubPair(`changes-${removeUpdateTarget}`);
      const { source, target, reservations, accessToken } = pair;
      const ids = withEditTxn(source, "hierarchy", (txn) => {
        const parent = Subject.insert(txn, IModel.rootSubjectId, "parent");
        return {
          parent,
          child: Subject.insert(txn, parent, "child"),
          updated: Subject.insert(txn, IModel.rootSubjectId, "updated"),
          deleted: Subject.insert(txn, IModel.rootSubjectId, "deleted"),
        };
      });
      await initializeHubTarget(pair);
      const targetId = (id: string) =>
        target.elements.getIdFromFederationGuid(
          source.elements.getElement(id).federationGuid
        )!;
      const childId = targetId(ids.child);
      const updatedId = targetId(ids.updated);
      const deletedId = targetId(ids.deleted);
      const calls = reservations.calls;
      if (removeUpdateTarget)
        withEditTxn(target, "remove target match", (txn) =>
          txn.deleteElement(updatedId)
        );
      const insertedId = withEditTxn(source, "source changes", (txn) => {
        for (const id of [ids.child, ids.updated])
          txn.updateElement({
            ...source.elements.getElementProps(id),
            userLabel: "updated",
          });
        txn.deleteElement(ids.deleted);
        return Subject.insert(txn, ids.parent, "inserted");
      });
      await source.pushChanges({
        accessToken,
        description: "insert, update, delete",
      });
      const csFileProps = await transformerTestHub.downloadChangesets({
        accessToken,
        iModelId: source.iModelId,
        range: { first: source.changeset.index!, end: source.changeset.index! },
        targetDir: BriefcaseManager.getChangeSetsPath(source.iModelId),
      });
      const changes = (await ChangedInstanceIds.initialize({
        iModel: source,
        csFileProps,
      }))!;
      expect(changes.element.updateIds.has(ids.parent)).toBe(false);
      const visited: string[] = [];
      class MetadataTransformer extends IModelTransformer {
        public override async onExportElement(element: Element): Promise<void> {
          expect(this.exporter.sourceDbChanges).toBe(changes);
          visited.push(element.id);
          await super.onExportElement(element);
        }
      }
      await withEditTxn(target, "replay changes", async (txn) => {
        const transformer = new MetadataTransformer(
          { source, target: txn },
          {
            argsForProcessChanges: { changedInstanceIds: changes },
            skipPropagateChangesToRootElements: true,
          }
        );
        try {
          const insert = vi.spyOn(txn, "insertElement");
          await transformer.process();
          expect(reservations.calls).toBe(calls + 1);
          expect(insert).toHaveBeenCalledTimes(removeUpdateTarget ? 2 : 1);
          for (const id of [childId, updatedId])
            expect(target.elements.getElement(id).userLabel).toBe("updated");
          expect(transformer.context.findTargetElementId(insertedId)).not.toBe(
            Id64.invalid
          );
          expect(target.elements.tryGetElementProps(deletedId)).toBeUndefined();
          expect(visited).not.toContain(ids.parent);
          expect(transformer.exporter.sourceDbChanges).toBeUndefined();
          await transformer.process();
          expect(reservations.calls).toBe(calls + 1);
        } finally {
          transformer.dispose();
        }
      });
    }
  );

  it("replays aspect and relationship changes without an element reservation", async () => {
    await using pair = await createHubPair("aspect-relationship");
    const { source, target, reservations } = pair;
    const owner = withEditTxn(source, "owner", (txn) =>
      Subject.insert(txn, IModel.rootSubjectId, "owner")
    );
    await initializeHubTarget(pair);
    const calls = reservations.calls;
    const ids = withEditTxn(source, "aspect and relationship", (txn) => ({
      aspect: txn.insertAspect({
        classFullName: ExternalSourceAspect.classFullName,
        element: new ElementOwnsExternalSourceAspects(owner),
        scope: { id: IModel.rootSubjectId },
        identifier: "source-aspect",
        kind: ExternalSourceAspect.Kind.Element,
      } as ExternalSourceAspectProps),
      relationship: ElementGroupsMembers.create(
        source,
        IModel.rootSubjectId,
        owner
      ).insert(txn),
    }));
    await source.pushChanges({
      accessToken: pair.accessToken,
      description: "aspect and relationship",
    });
    const changes = (await ChangedInstanceIds.initialize({
      iModel: source,
      csFileProps: [],
    }))!;
    changes.addCustomAspectChange("Inserted", ids.aspect, owner);
    changes.relationship.insertIds.add(ids.relationship);
    await withEditTxn(target, "replay", async (txn) => {
      const transformer = new IModelTransformer(
        { source, target: txn },
        {
          includeSourceProvenance: true,
          argsForProcessChanges: { changedInstanceIds: changes },
        }
      );
      try {
        await transformer.process();
        const aspects = target.elements.getAspects(
          transformer.context.findTargetElementId(owner),
          ExternalSourceAspect.classFullName
        );
        expect(
          aspects.map((aspect) => (aspect as ExternalSourceAspect).identifier)
        ).toContain("source-aspect");
        let count = 0;
        for await (const _row of target.createQueryReader(
          `SELECT ECInstanceId FROM ${ElementGroupsMembers.classFullName}`
        ))
          count++;
        expect(count).toBe(1);
        expect(reservations.calls).toBe(calls);
      } finally {
        transformer.dispose();
      }
    });
  });

  it("deletes a definition model and its partition using replay's change metadata", async () => {
    await using pair = await createHubPair("definition-delete");
    const { source, target, reservations } = pair;
    const ids = withEditTxn(source, "definitions", (txn) => {
      const model = DefinitionModel.insert(
        txn,
        IModel.rootSubjectId,
        "definitions"
      );
      return {
        model,
        category: SpatialCategory.insert(
          txn,
          model,
          "category",
          new SubCategoryAppearance()
        ),
      };
    });
    await initializeHubTarget(pair);
    const modelId = target.elements.getIdFromFederationGuid(
      source.elements.getElement(ids.model).federationGuid
    )!;
    const categoryId = target.elements.getIdFromFederationGuid(
      source.elements.getElement(ids.category).federationGuid
    )!;
    withEditTxn(source, "delete model", (txn) => {
      expect(txn.deleteDefinitionElements([ids.category]).size).toBe(0);
      txn.deleteModel(ids.model);
    });
    await source.pushChanges({
      accessToken: pair.accessToken,
      description: "delete model",
    });
    const changes = (await ChangedInstanceIds.initialize({
      iModel: source,
      csFileProps: [],
    }))!;
    changes.model.deleteIds.add(ids.model);
    expect(changes.element.deleteIds.size).toBe(0);
    const calls = reservations.calls;
    await withEditTxn(target, "replay deletion", async (txn) => {
      const transformer = new IModelTransformer(
        { source, target: txn },
        { argsForProcessChanges: { changedInstanceIds: changes } }
      );
      try {
        await transformer.process();
      } finally {
        transformer.dispose();
      }
    });
    expect(changes.element.deleteIds).toContain(ids.model);
    expect(target.models.tryGetModelProps(modelId)).toBeUndefined();
    for (const id of [modelId, categoryId])
      expect(target.elements.tryGetElementProps(id)).toBeUndefined();
    expect(reservations.calls).toBe(calls);
  });

  it("keeps the insertion guard through the incremental save callback", async () => {
    await using pair = await createHubPair("save-guard");
    const { source, target } = pair;
    const id = withEditTxn(source, "source", (txn) =>
      Subject.insert(txn, IModel.rootSubjectId, "source")
    );
    await initializeHubTarget(pair);
    const extra = Subject.create(
      target,
      IModel.rootSubjectId,
      "unplanned"
    ).toJSON();
    extra.federationGuid = Guid.createValue();
    const changes = (await ChangedInstanceIds.initialize({
      iModel: source,
      csFileProps: [],
    }))!;
    changes.element.updateIds.add(id);
    const targetTxn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: targetTxn },
      {
        argsForProcessChanges: {
          changedInstanceIds: changes,
          saveTargetChanges: async (active) => {
            await active.importer.importElement(extra);
          },
        },
      }
    );
    try {
      await expect(transformer.process()).rejects.toMatchObject(
        reservationError(IModelTransformerError.ElementReservationPlanChanged)
      );
      expect(
        target.elements.getIdFromFederationGuid(extra.federationGuid)
      ).toBeUndefined();
    } finally {
      transformer.dispose();
      targetTxn.end("abandon");
    }
  });

  it("exports models, elements, aspects, and relationships whose source IDs are reserved", async () => {
    await using pair = await createHubPair("reserved-source");
    const { source, target } = pair;
    const ids = withEditTxn(source, "source model", (txn) => {
      const category = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "category",
        new SubCategoryAppearance()
      );
      const excluded = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "excluded",
        new SubCategoryAppearance()
      );
      const model = PhysicalModel.insert(txn, IModel.rootSubjectId, "model");
      const object = txn.insertElement({
        classFullName: PhysicalObject.classFullName,
        model,
        category,
        federationGuid: Guid.createValue(),
        code: Code.createEmpty().toJSON(),
      } as PhysicalElementProps);
      ElementGroupsMembers.create(source, IModel.rootSubjectId, object).insert(
        txn
      );
      return { model, object, excluded };
    });
    await initializeHubTarget(pair);
    const targetId = (id: string) =>
      target.elements.getIdFromFederationGuid(
        source.elements.getElement(id).federationGuid
      )!;
    const owner = targetId(ids.object);
    const modelId = targetId(ids.model);
    for (const id of [owner, modelId])
      expect(Id64.getBriefcaseId(id)).toBe(
        BriefcaseIdValue.SchemaSyncElementReserved
      );
    const aspectId = withEditTxn(target, "reserved owner aspect", (txn) =>
      txn.insertAspect({
        classFullName: ExternalSourceAspect.classFullName,
        element: new ElementOwnsExternalSourceAspects(owner),
        scope: { id: IModel.rootSubjectId },
        identifier: "reserved-owner",
        kind: ExternalSourceAspect.Kind.Element,
      } as ExternalSourceAspectProps)
    );
    const changes = (await ChangedInstanceIds.initialize({
      iModel: target,
      csFileProps: [],
    }))!;
    await changes.addCustomElementChange("Inserted", owner);
    class Capture extends IModelExportHandler {
      public elements: string[] = [];
      public models: string[] = [];
      public aspects: string[] = [];
      public relationships = 0;
      public override async onExportElement(element: Element) {
        this.elements.push(element.id);
      }
      public override async onExportModel(
        value: Parameters<IModelExportHandler["onExportModel"]>[0]
      ) {
        this.models.push(value.id);
      }
      public override async onExportElementMultiAspects(
        aspects: ElementMultiAspect[]
      ) {
        this.aspects.push(...aspects.map((aspect) => aspect.id));
      }
      public override async onExportRelationship() {
        this.relationships++;
      }
    }
    const handler = new Capture();
    const exporter = new IModelExporter(target);
    exporter.registerHandler(handler);
    exporter.excludeElementsInCategory(targetId(ids.excluded));
    await exporter.exportChanges({ changedInstanceIds: changes });
    expect(handler.elements).toContain(owner);
    expect(handler.models).toContain(modelId);
    expect(handler.aspects).toContain(aspectId);
    expect(handler.relationships).toBe(1);
  });

  it("bypasses reservations under the real target schema lock", async () => {
    await using pair = await createHubPair("schema-lock", false);
    const { source, target, reservations } = pair;
    const id = withEditTxn(source, "source", (txn) =>
      Subject.insert(txn, IModel.rootSubjectId, "source")
    );
    await source.pushChanges({
      accessToken: pair.accessToken,
      description: "source",
    });
    await target.acquireSchemaLock();
    expect(target.holdsSchemaLock).toBe(true);
    await withEditTxn(target, "locked target", async (txn) => {
      const transformer = new IModelTransformer(
        { source, target: txn },
        { noProvenance: true }
      );
      try {
        await transformer.processElement(id);
        expect(reservations.calls).toBe(0);
        expect(
          target.elements.getIdFromFederationGuid(
            source.elements.getElement(id).federationGuid
          )
        ).toBeDefined();
      } finally {
        transformer.dispose();
      }
    });
  });
});
