/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
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
import { Guid, Id64, ITwinError, Logger } from "@itwin/core-bentley";
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
  IModelCloneContext,
  PreparedElementIdentity,
} from "../../IModelCloneContext";
import {
  ChangedInstanceIds,
  IModelExporter,
  IModelExportHandler,
} from "../../IModelExporter";
import { IModelImporter } from "../../IModelImporter";
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

function createDb(name: string) {
  return StandaloneDb.createEmpty(
    IModelTransformerTestUtils.prepareOutputFile("ElementReservations", name),
    { rootSubject: { name } }
  );
}

let ownsReservationHub = false;
let restoreCheckpointDownload: (() => void) | undefined;
beforeAll(() => {
  if (!transformerTestHub.isActive) {
    transformerTestHub.start(
      "ElementReservationsHub",
      KnownTestLocations.outputDir
    );
    ownsReservationHub = true;
  }
  restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
});
afterAll(() => {
  restoreCheckpointDownload?.();
  if (ownsReservationHub) transformerTestHub.stop();
});

async function initializeHubTarget(
  pair: Awaited<ReturnType<typeof createHubPair>>,
  description: string
): Promise<void> {
  const targetTxn = createStartedEditTxn(pair.target);
  const transformer = new IModelTransformer({
    source: pair.source,
    target: targetTxn,
  });
  try {
    await transformer.process();
  } finally {
    transformer.dispose();
    targetTxn.end();
  }
  await pair.target.pushChanges({
    accessToken: pair.accessToken,
    description,
  });
}

async function createHubPair(
  name: string,
  noLocks = true,
  targetNoLocks = noLocks
): Promise<{
  accessToken: string;
  source: BriefcaseDb;
  target: BriefcaseDb;
  sourceIModelId: string;
  targetIModelId: string;
  dispose(): Promise<void>;
}> {
  const accessToken = await HubWrappers.getAccessToken(
    TestUtils.TestUserType.Regular
  );
  const iTwinId = transformerTestHub.iTwinId;
  const sourceIModelId = await HubWrappers.recreateIModel({
    accessToken,
    iTwinId,
    iModelName: `${name}-source`,
    ...(noLocks ? { noLocks: true } : {}),
  });
  const targetIModelId = await HubWrappers.recreateIModel({
    accessToken,
    iTwinId,
    iModelName: `${name}-target`,
    ...(targetNoLocks ? { noLocks: true } : {}),
  });
  const source = await HubWrappers.downloadAndOpenBriefcase({
    accessToken,
    iTwinId,
    iModelId: sourceIModelId,
  });
  const target = await HubWrappers.downloadAndOpenBriefcase({
    accessToken,
    iTwinId,
    iModelId: targetIModelId,
  });
  return {
    accessToken,
    source,
    target,
    sourceIModelId,
    targetIModelId,
    async dispose() {
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, source);
      await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, target);
      await transformerTestHub.deleteIModel({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      await transformerTestHub.deleteIModel({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });
    },
  };
}

describe("element reservation preparation", () => {
  it("prepares and reserves a cloned parent graph before inserting any of its elements", async () => {
    const source = createDb("parent-source.bim");
    const target = createDb("parent-target.bim");
    const specName = "ReservationParentScope";
    const sourceSpec = withEditTxn(source, "source code spec", (txn) =>
      source.codeSpecs.insert(
        txn,
        CodeSpec.create(source, specName, CodeScopeSpec.Type.ParentElement)
      )
    );
    const targetSpec = withEditTxn(target, "target code specs", (txn) => {
      target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, "padding", CodeScopeSpec.Type.Repository)
      );
      return target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, specName, CodeScopeSpec.Type.ParentElement)
      );
    });
    const parentGuid = Guid.createValue();
    const ids = withEditTxn(source, "source graph", (txn) => {
      const parent = Subject.create(
        source,
        IModel.rootSubjectId,
        "parent"
      ).toJSON();
      parent.federationGuid = parentGuid;
      parent.code = {
        spec: sourceSpec,
        scope: IModel.rootSubjectId,
        value: "parent",
      };
      const parentId = txn.insertElement(parent);
      return [
        parentId,
        ...Array.from({ length: 24 }, (_, index) => {
          const child = Subject.create(
            source,
            parentId,
            `child-${index}`
          ).toJSON();
          child.federationGuid = Guid.createValue();
          child.code = {
            spec: sourceSpec,
            scope: parentId,
            value: `child-${index}`,
          };
          return txn.insertElement(child);
        }),
      ];
    });
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "parent-sync.db"
      )
    );
    const targetTxn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    const context = transformer["_cloneContext"];
    try {
      await context.initialize();
      context.remapCodeSpec(specName, specName);
      context.remapElement(IModel.rootSubjectId, IModel.rootSubjectId);
      expect(sourceSpec).not.toBe(targetSpec);
      const prepared: PreparedElementIdentity[] = [];
      for (const id of ids) {
        const sourceElement = source.elements.getElement(id);
        const item = await context.prepareElementIdentity(
          sourceElement,
          id === ids[0]
            ? IModel.rootSubjectId
            : prepared[0].identity.federationGuid!,
          async () => transformer.onTransformElement(sourceElement)
        );
        const props = item.identity;
        expect(props.classFullName).toBe(sourceElement.classFullName);
        expect(props.federationGuid).toBe(sourceElement.federationGuid);
        expect(props.code).toEqual({
          spec: targetSpec,
          scope: id === ids[0] ? IModel.rootSubjectId : parentGuid,
          value: sourceElement.code.value,
        });
        prepared.push(item);
        expect(context.findTargetElementId(id)).toBe(Id64.invalid);
        expect(
          target.elements.getIdFromFederationGuid(sourceElement.federationGuid)
        ).toBeUndefined();
      }
      await target.reservations.reserveElements({
        elements: prepared.map(({ identity }) => ({
          ...identity,
          federationGuid: identity.federationGuid!,
        })),
      });
      expect(reservations.calls).toBe(1);
      for (const item of prepared) {
        const id = item.sourceId;
        const props = await context.transformPreparedElement(item, async () =>
          transformer.onTransformElement(source.elements.getElement(id))
        );
        props.id = undefined;
        const insertedId = targetTxn.insertElement(props);
        expect(insertedId).toBe(
          reservations.sync.findReservedElement(props.federationGuid!)
            ?.elementId
        );
        expect(insertedId).not.toBe(id);
        context.remapElement(id, insertedId);
        expect(target.elements.getElementProps(insertedId).code.scope).toBe(
          id === ids[0]
            ? IModel.rootSubjectId
            : context.findTargetElementId(ids[0])
        );
      }
    } finally {
      transformer.dispose();
      targetTxn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("automatically reserves a public processElement insert before ordinary insertion", async () => {
    const source = createDb("public-source.bim");
    const target = createDb("public-target.bim");
    const sourceId = withEditTxn(source, "public source element", (txn) => {
      const props = Subject.create(
        source,
        IModel.rootSubjectId,
        "public-child"
      ).toJSON();
      props.federationGuid = Guid.createValue();
      return txn.insertElement(props);
    });
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "public-sync.db"
      )
    );
    const targetTxn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    const insertElement = vi.spyOn(targetTxn, "insertElement");
    try {
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
      );
      const sourceCodeSpec = source.codeSpecs.getById(
        source.elements.getElement(sourceId).code.spec
      );
      transformer.context.remapCodeSpec(
        sourceCodeSpec.name,
        sourceCodeSpec.name
      );
      await transformer.processElement(sourceId);
      expect(reservations.calls).toBe(1);
      expect(insertElement).toHaveBeenCalledTimes(1);
      const targetId = transformer.context.findTargetElementId(sourceId);
      expect(targetId).not.toBe(Id64.invalid);
      expect(targetId).not.toBe(sourceId);
      expect(
        reservations.sync.findReservedElement(
          source.elements.getElement(sourceId).federationGuid!
        )?.elementId
      ).toBe(targetId);
    } finally {
      transformer.dispose();
      targetTxn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("reserves a public parent-and-24-child graph in one batch", async () => {
    const source = createDb("public-graph-source.bim");
    const target = createDb("public-graph-target.bim");
    const specName = "PublicParentScope";
    const sourceSpec = withEditTxn(source, "public source code spec", (txn) =>
      source.codeSpecs.insert(
        txn,
        CodeSpec.create(source, specName, CodeScopeSpec.Type.ParentElement)
      )
    );
    const targetSpec = withEditTxn(target, "public target code spec", (txn) => {
      target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, "public-padding", CodeScopeSpec.Type.Repository)
      );
      return target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, specName, CodeScopeSpec.Type.ParentElement)
      );
    });
    const parentGuid = Guid.createValue();
    const ids = withEditTxn(source, "public source graph", (txn) => {
      const parent = Subject.create(
        source,
        IModel.rootSubjectId,
        "public-parent"
      ).toJSON();
      parent.federationGuid = parentGuid;
      parent.code = {
        spec: sourceSpec,
        scope: IModel.rootSubjectId,
        value: "public-parent",
      };
      const parentId = txn.insertElement(parent);
      return [
        parentId,
        ...Array.from({ length: 24 }, (_, index) => {
          const child = Subject.create(
            source,
            parentId,
            `public-child-${index}`
          ).toJSON();
          child.federationGuid = Guid.createValue();
          child.code = {
            spec: sourceSpec,
            scope: parentId,
            value: `public-child-${index}`,
          };
          return txn.insertElement(child);
        }),
      ];
    });
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "public-graph-sync.db"
      )
    );
    const targetTxn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    const insertElement = vi.spyOn(targetTxn, "insertElement");
    try {
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
      );
      transformer.context.remapCodeSpec(specName, specName);
      await transformer.processElement(ids[0]);
      expect(reservations.calls).toBe(1);
      expect(insertElement).toHaveBeenCalledTimes(25);
      const targetParentId = transformer.context.findTargetElementId(ids[0]);
      expect(targetParentId).toBe(
        reservations.sync.findReservedElement(parentGuid)?.elementId
      );
      for (const childId of ids.slice(1)) {
        const targetChildId = transformer.context.findTargetElementId(childId);
        expect(targetChildId).toBe(
          reservations.sync.findReservedElement(
            source.elements.getElement(childId).federationGuid!
          )?.elementId
        );
        expect(target.elements.getElementProps(targetChildId).code.scope).toBe(
          targetParentId
        );
      }
      expect(targetSpec).not.toBe(sourceSpec);
    } finally {
      transformer.dispose();
      targetTxn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("exports changed elements, models, aspects, and relationships from a source containing real reserved IDs", async () => {
    const pair = await createHubPair("reserved-source-export");
    const sourceIds = withEditTxn(
      pair.source,
      "source model and categories",
      (txn) => {
        const category = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "primary",
          new SubCategoryAppearance()
        );
        const excludedCategory = SpatialCategory.insert(
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
        ElementGroupsMembers.create(
          pair.source,
          IModel.rootSubjectId,
          object
        ).insert(txn);
        return { model, object, excludedCategory };
      }
    );
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "source graph",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "reserved-source-export-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "reserve and clone graph");
      const targetId = (sourceId: string) =>
        pair.target.elements.getIdFromFederationGuid(
          pair.source.elements.getElement(sourceId).federationGuid
        )!;
      const ownerId = targetId(sourceIds.object);
      const modelId = targetId(sourceIds.model);
      expect(Id64.getBriefcaseId(ownerId)).toBe(
        BriefcaseIdValue.SchemaSyncElementReserved
      );
      expect(Id64.getBriefcaseId(modelId)).toBe(
        BriefcaseIdValue.SchemaSyncElementReserved
      );
      const aspectId = withEditTxn(
        pair.target,
        "aspect on reserved owner",
        (txn) =>
          txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(ownerId),
            scope: { id: IModel.rootSubjectId },
            identifier: "reserved-owner",
            kind: ExternalSourceAspect.Kind.Element,
          } as ExternalSourceAspectProps)
      );
      const changes = await ChangedInstanceIds.initialize({
        iModel: pair.target,
        csFileProps: [],
      });
      expect(changes).toBeDefined();
      await changes!.addCustomElementChange("Inserted", ownerId);
      class Capture extends IModelExportHandler {
        public elements = new Set<string>();
        public models = new Set<string>();
        public aspects = new Set<string>();
        public relationships = 0;
        public override async onExportElement(element: Element) {
          this.elements.add(element.id);
        }
        public override async onExportModel(
          model: Parameters<IModelExportHandler["onExportModel"]>[0]
        ) {
          this.models.add(model.id);
        }
        public override async onExportElementMultiAspects(
          aspects: ElementMultiAspect[]
        ) {
          for (const aspect of aspects) this.aspects.add(aspect.id);
        }
        public override async onExportRelationship() {
          this.relationships++;
        }
      }
      const handler = new Capture();
      const exporter = new IModelExporter(pair.target);
      exporter.registerHandler(handler);
      exporter.excludeElementsInCategory(targetId(sourceIds.excludedCategory));
      await exporter.exportChanges({ changedInstanceIds: changes! });
      expect(handler.elements).toContain(ownerId);
      expect(handler.models).toContain(modelId);
      expect(handler.aspects).toContain(aspectId);
      expect(handler.relationships).toBe(1);
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("replays a public processChanges relationship change without reserving elements", async () => {
    const pair = await createHubPair("process-relationship");
    const sourceIds = withEditTxn(
      pair.source,
      "insert relationship endpoints",
      (txn) => ({
        sourceId: Subject.insert(
          txn,
          IModel.rootSubjectId,
          "relationship-source"
        ),
        targetId: Subject.insert(
          txn,
          IModel.rootSubjectId,
          "relationship-target"
        ),
      })
    );
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "relationship endpoints",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "process-relationship-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "relationship baseline");
      const baselineReservationCalls = reservations.calls;
      const relationshipId = withEditTxn(
        pair.source,
        "insert source relationship",
        (txn) =>
          ElementGroupsMembers.create(
            pair.source,
            sourceIds.sourceId,
            sourceIds.targetId
          ).insert(txn)
      );
      await pair.source.pushChanges({
        accessToken: pair.accessToken,
        description: "source relationship change",
      });
      const changedInstanceIds = await ChangedInstanceIds.initialize({
        iModel: pair.source,
        csFileProps: [],
      });
      if (changedInstanceIds === undefined)
        throw new Error("Expected initialized changedInstanceIds");
      changedInstanceIds.relationship.insertIds.add(relationshipId);
      const targetTxn = createStartedEditTxn(pair.target);
      const transformer = new IModelTransformer(
        { source: pair.source, target: targetTxn },
        { argsForProcessChanges: { changedInstanceIds } }
      );
      try {
        await transformer.process();
        let relationshipCount = 0;
        for await (const _row of pair.target.createQueryReader(
          `SELECT ECInstanceId FROM ${ElementGroupsMembers.classFullName}`
        ))
          relationshipCount++;
        expect(relationshipCount).toBe(1);
        expect(reservations.calls).toBe(baselineReservationCalls);
      } finally {
        transformer.dispose();
        targetTxn.end();
      }
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("does not replay an unchanged parent used to reach a changed descendant", async () => {
    const pair = await createHubPair("unchanged-parent");
    const ids = withEditTxn(pair.source, "parent and child", (txn) => {
      const parent = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "unchanged-parent"
      );
      return { parent, child: Subject.insert(txn, parent, "changed-child") };
    });
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "hierarchy",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "unchanged-parent-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "hierarchy baseline");
      const calls = reservations.calls;
      withEditTxn(pair.source, "change only child", (txn) =>
        txn.updateElement({
          ...pair.source.elements.getElementProps(ids.child),
          userLabel: "changed",
        })
      );
      await pair.source.pushChanges({
        accessToken: pair.accessToken,
        description: "child update",
      });
      const changes = await ChangedInstanceIds.initialize({
        iModel: pair.source,
        csFileProps: [],
      });
      expect(changes).toBeDefined();
      await changes!.addCustomElementChange("Updated", ids.child);
      expect(changes!.element.updateIds.has(ids.parent)).toBe(false);
      const visited: string[] = [];
      class CaptureTransformer extends IModelTransformer {
        public override async onExportElement(element: Element): Promise<void> {
          visited.push(element.id);
          await super.onExportElement(element);
        }
      }
      await withEditTxn(pair.target, "child update replay", async (txn) => {
        const transformer = new CaptureTransformer(
          { source: pair.source, target: txn },
          {
            argsForProcessChanges: { changedInstanceIds: changes! },
            skipPropagateChangesToRootElements: true,
          }
        );
        try {
          await transformer.process();
          const childId = transformer.context.findTargetElementId(ids.child);
          expect(pair.target.elements.getElement(childId).userLabel).toBe(
            "changed"
          );
        } finally {
          transformer.dispose();
        }
      });
      expect(visited).toEqual([ids.child]);
      expect(reservations.calls).toBe(calls);
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("replays a public processChanges aspect change without reserving elements", async () => {
    const pair = await createHubPair("process-aspect");
    const ownerId = withEditTxn(pair.source, "insert aspect owner", (txn) =>
      Subject.insert(txn, IModel.rootSubjectId, "aspect-owner")
    );
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "aspect owner",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "process-aspect-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "aspect baseline");
      const baselineReservationCalls = reservations.calls;
      const aspectId = withEditTxn(pair.source, "insert source aspect", (txn) =>
        txn.insertAspect({
          classFullName: ExternalSourceAspect.classFullName,
          element: new ElementOwnsExternalSourceAspects(ownerId),
          scope: { id: IModel.rootSubjectId },
          identifier: "public-aspect",
          kind: ExternalSourceAspect.Kind.Element,
        } as ExternalSourceAspectProps)
      );
      await pair.source.pushChanges({
        accessToken: pair.accessToken,
        description: "source aspect change",
      });
      const changedInstanceIds = await ChangedInstanceIds.initialize({
        iModel: pair.source,
        csFileProps: [],
      });
      if (changedInstanceIds === undefined)
        throw new Error("Expected initialized changedInstanceIds");
      changedInstanceIds.addCustomAspectChange("Inserted", aspectId, ownerId);
      const targetTxn = createStartedEditTxn(pair.target);
      const transformer = new IModelTransformer(
        { source: pair.source, target: targetTxn },
        {
          includeSourceProvenance: true,
          argsForProcessChanges: { changedInstanceIds },
        }
      );
      try {
        await transformer.process();
        const targetOwnerId = transformer.context.findTargetElementId(ownerId);
        const targetAspects = pair.target.elements
          .getAspects(targetOwnerId, ExternalSourceAspect.classFullName)
          .filter(
            (aspect) =>
              (aspect as ExternalSourceAspect).kind ===
              ExternalSourceAspect.Kind.Element
          );
        expect(targetAspects).toHaveLength(1);
        expect((targetAspects[0] as ExternalSourceAspect).identifier).toBe(
          "public-aspect"
        );
        expect(reservations.calls).toBe(baselineReservationCalls);
      } finally {
        transformer.dispose();
        targetTxn.end();
      }
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("replays a public processChanges deletion without reserving elements", async () => {
    const pair = await createHubPair("process-delete");
    const sourceId = withEditTxn(
      pair.source,
      "insert deletable source element",
      (txn) => Subject.insert(txn, IModel.rootSubjectId, "deletable-source")
    );
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "deletable source",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "process-delete-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "delete baseline");
      const originalTargetId = pair.target.elements.getIdFromFederationGuid(
        pair.source.elements.getElement(sourceId).federationGuid
      );
      expect(originalTargetId).toBeDefined();
      const baselineReservationCalls = reservations.calls;
      withEditTxn(pair.source, "delete source element", (txn) =>
        txn.deleteElement(sourceId)
      );
      await pair.source.pushChanges({
        accessToken: pair.accessToken,
        description: "source delete",
      });
      const changesets = await transformerTestHub.downloadChangesets({
        accessToken: pair.accessToken,
        iModelId: pair.sourceIModelId,
        range: {
          first: pair.source.changeset.index!,
          end: pair.source.changeset.index!,
        },
        targetDir: BriefcaseManager.getChangeSetsPath(pair.sourceIModelId),
      });
      const changedInstanceIds = await ChangedInstanceIds.initialize({
        iModel: pair.source,
        csFileProps: changesets,
      });
      if (changedInstanceIds === undefined)
        throw new Error("Expected initialized changedInstanceIds");
      expect(changedInstanceIds.element.deleteIds).toContain(sourceId);
      const targetTxn = createStartedEditTxn(pair.target);
      const transformer = new IModelTransformer(
        { source: pair.source, target: targetTxn },
        { argsForProcessChanges: { changedInstanceIds } }
      );
      try {
        await transformer.process();
        expect(
          pair.target.elements.tryGetElementProps(originalTargetId!)
        ).toBeUndefined();
        expect(reservations.calls).toBe(baselineReservationCalls);
      } finally {
        transformer.dispose();
        targetTxn.end();
      }
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("replays model-only definition deletion with live change metadata and a deferred partition fallback", async () => {
    const pair = await createHubPair("definition-delete-fallback");
    const ids = withEditTxn(pair.source, "definition model contents", (txn) => {
      const model = DefinitionModel.insert(
        txn,
        IModel.rootSubjectId,
        "definitions"
      );
      const category = SpatialCategory.insert(
        txn,
        model,
        "definition-category",
        new SubCategoryAppearance()
      );
      return { model, category };
    });
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "definitions",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "definition-delete-fallback-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "definition baseline");
      const targetModelId = pair.target.elements.getIdFromFederationGuid(
        pair.source.elements.getElement(ids.model).federationGuid
      )!;
      const targetCategoryId = pair.target.elements.getIdFromFederationGuid(
        pair.source.elements.getElement(ids.category).federationGuid
      )!;
      expect(pair.target.models.tryGetModelProps(targetModelId)).toBeDefined();
      withEditTxn(pair.source, "remove source definition model", (txn) => {
        expect(txn.deleteDefinitionElements([ids.category]).size).toBe(0);
        txn.deleteModel(ids.model);
      });
      expect(pair.source.elements.tryGetElementProps(ids.model)).toBeDefined();
      await pair.source.pushChanges({
        accessToken: pair.accessToken,
        description: "delete definition model",
      });
      const changes = await ChangedInstanceIds.initialize({
        iModel: pair.source,
        csFileProps: [],
      });
      expect(changes).toBeDefined();
      changes!.model.deleteIds.add(ids.model);
      expect(changes!.element.deleteIds.size).toBe(0);
      const calls = reservations.calls;
      await withEditTxn(pair.target, "replay model deletion", async (txn) => {
        class MetadataTransformer extends IModelTransformer {
          public override async onDeleteModel(id: string): Promise<void> {
            expect(this.exporter.sourceDbChanges).toBe(changes);
            await super.onDeleteModel(id);
          }
        }
        const transformer = new MetadataTransformer(
          { source: pair.source, target: txn },
          { argsForProcessChanges: { changedInstanceIds: changes! } }
        );
        try {
          await transformer.process();
          expect(transformer.exporter.sourceDbChanges).toBeUndefined();
        } finally {
          transformer.dispose();
        }
      });
      expect(changes!.element.deleteIds).toContain(ids.model);
      expect(
        pair.target.models.tryGetModelProps(targetModelId)
      ).toBeUndefined();
      expect(
        pair.target.elements.tryGetElementProps(targetModelId)
      ).toBeUndefined();
      expect(
        pair.target.elements.tryGetElementProps(targetCategoryId)
      ).toBeUndefined();
      expect(reservations.calls).toBe(calls);
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("keeps the insertion guard active through the incremental save callback", async () => {
    const pair = await createHubPair("save-callback-guard");
    const sourceId = withEditTxn(pair.source, "save guard source", (txn) =>
      Subject.insert(txn, IModel.rootSubjectId, "source")
    );
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "source",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "save-callback-guard-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "save guard baseline");
      const extra = Subject.create(
        pair.target,
        IModel.rootSubjectId,
        "unplanned-save-insert"
      ).toJSON();
      extra.federationGuid = Guid.createValue();
      await pair.target.reservations.reserveElements({
        elements: [{ ...extra, federationGuid: extra.federationGuid }],
      });
      const changes = await ChangedInstanceIds.initialize({
        iModel: pair.source,
        csFileProps: [],
      });
      expect(changes).toBeDefined();
      changes!.element.updateIds.add(sourceId);
      const txn = createStartedEditTxn(pair.target);
      const transformer = new IModelTransformer(
        { source: pair.source, target: txn },
        {
          argsForProcessChanges: {
            changedInstanceIds: changes!,
            saveTargetChanges: async (active) => {
              await active.importer.importElement(extra);
            },
          },
        }
      );
      try {
        const error = await transformer
          .process()
          .catch((caught: unknown) => caught);
        expect(error).toMatchObject({
          iTwinErrorId: {
            scope: IModelTransformerErrorScope,
            key: IModelTransformerError.ElementReservationPlanChanged,
          },
        });
        expect(
          pair.target.elements.getIdFromFederationGuid(extra.federationGuid)
        ).toBeUndefined();
        expect(
          reservations.sync.findReservedElement(extra.federationGuid)
        ).toBeDefined();
      } finally {
        transformer.dispose();
        txn.end("abandon");
      }
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("does not reserve a changed insert that already matches the target", async () => {
    const pair = await createHubPair("process-matched-insert");
    withEditTxn(pair.source, "insert matched baseline source", (txn) =>
      Subject.insert(txn, IModel.rootSubjectId, "matched-baseline")
    );
    await pair.source.pushChanges({
      accessToken: pair.accessToken,
      description: "matched baseline source",
    });
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "process-matched-insert-sync.db"
      )
    );
    try {
      await initializeHubTarget(pair, "matched insert baseline");
      const insertedSourceId = withEditTxn(
        pair.source,
        "insert source element with target match",
        (txn) => {
          const props = Subject.create(
            pair.source,
            IModel.rootSubjectId,
            "matched-insert"
          ).toJSON();
          props.federationGuid = Guid.createValue();
          props.userLabel = "source-value";
          return txn.insertElement(props);
        }
      );
      const matchingSourceProps =
        pair.source.elements.getElementProps(insertedSourceId);
      await pair.target.reservations.reserveElements({
        elements: [
          {
            classFullName: matchingSourceProps.classFullName,
            federationGuid: matchingSourceProps.federationGuid!,
            code: matchingSourceProps.code,
          },
        ],
      });
      withEditTxn(pair.target, "insert matching target element", (txn) =>
        txn.insertElement({
          ...matchingSourceProps,
          id: undefined,
          userLabel: "target-value",
        })
      );
      await pair.source.pushChanges({
        accessToken: pair.accessToken,
        description: "source matched insert",
      });
      await pair.target.pushChanges({
        accessToken: pair.accessToken,
        description: "target matched insert",
      });
      const changedInstanceIds = await ChangedInstanceIds.initialize({
        iModel: pair.source,
        csFileProps: [],
      });
      if (changedInstanceIds === undefined)
        throw new Error("Expected initialized changedInstanceIds");
      changedInstanceIds.element.insertIds.add(insertedSourceId);
      const baselineReservationCalls = reservations.calls;
      const targetTxn = createStartedEditTxn(pair.target);
      const transformer = new IModelTransformer(
        { source: pair.source, target: targetTxn },
        { argsForProcessChanges: { changedInstanceIds } }
      );
      const sourceCodeSpec = pair.source.codeSpecs.getById(
        matchingSourceProps.code.spec
      );
      transformer.context.remapCodeSpec(
        sourceCodeSpec.name,
        sourceCodeSpec.name
      );
      try {
        await transformer.process();
        expect(reservations.calls).toBe(baselineReservationCalls);
        const targetId =
          transformer.context.findTargetElementId(insertedSourceId);
        expect(targetId).not.toBe(Id64.invalid);
        expect(pair.target.elements.getElementProps(targetId).userLabel).toBe(
          "source-value"
        );
      } finally {
        transformer.dispose();
        targetTxn.end();
      }
    } finally {
      await pair.dispose();
      reservations.dispose();
    }
  });

  it.each([false, true])(
    "processes initialized changedInstanceIds through public process (update target absent: %s)",
    async (removeUpdateTarget) => {
      const pair = await createHubPair(
        `process-changes-public-${removeUpdateTarget}`
      );
      const reservations = await useLocalReservations(
        pair.target,
        IModelTransformerTestUtils.prepareOutputFile(
          "ElementReservations",
          `process-changes-public-${removeUpdateTarget}-sync.db`
        )
      );
      let targetTxn: ReturnType<typeof createStartedEditTxn> | undefined;
      let transformer: IModelTransformer | undefined;
      try {
        const initialSourceId = withEditTxn(
          pair.source,
          "insert process changes source element",
          (txn) => {
            const props = Subject.create(
              pair.source,
              IModel.rootSubjectId,
              "process-changes-existing"
            ).toJSON();
            props.federationGuid = Guid.createValue();
            props.userLabel = "initial";
            return txn.insertElement(props);
          }
        );
        await pair.source.pushChanges({
          accessToken: pair.accessToken,
          description: "initial process changes source",
        });

        const initialTargetTxn = createStartedEditTxn(pair.target);
        const initialTransformer = new IModelTransformer({
          source: pair.source,
          target: initialTargetTxn,
        });
        await initialTransformer.process();
        const initialTargetId =
          initialTransformer.context.findTargetElementId(initialSourceId);
        initialTransformer.dispose();
        initialTargetTxn.end();
        await pair.target.pushChanges({
          accessToken: pair.accessToken,
          description: "initial process changes target",
        });
        expect(initialTargetId).not.toBe(Id64.invalid);
        const initialReservationCalls = reservations.calls;
        if (removeUpdateTarget) {
          withEditTxn(pair.target, "remove update's target match", (txn) =>
            txn.deleteElement(initialTargetId)
          );
          expect(
            pair.target.elements.tryGetElementProps(initialTargetId)
          ).toBeUndefined();
        }

        const insertedSourceId = withEditTxn(
          pair.source,
          "update and insert process changes source elements",
          (txn) => {
            const existing = pair.source.elements.getElement(initialSourceId);
            existing.userLabel = "updated";
            existing.update(txn);
            const inserted = Subject.create(
              pair.source,
              IModel.rootSubjectId,
              "process-changes-inserted"
            ).toJSON();
            inserted.federationGuid = Guid.createValue();
            inserted.userLabel = "inserted";
            return txn.insertElement(inserted);
          }
        );
        await pair.source.pushChanges({
          accessToken: pair.accessToken,
          description: "updated process changes source",
        });

        const changedInstanceIds = await ChangedInstanceIds.initialize({
          iModel: pair.source,
          csFileProps: [],
        });
        if (changedInstanceIds === undefined)
          throw new Error("Expected initialized changedInstanceIds");
        changedInstanceIds.element.updateIds.add(initialSourceId);
        changedInstanceIds.element.insertIds.add(insertedSourceId);
        class MetadataTransformer extends IModelTransformer {
          public override async onExportElement(
            element: Element
          ): Promise<void> {
            expect(this.exporter.sourceDbChanges).toBe(changedInstanceIds);
            await super.onExportElement(element);
          }
        }

        targetTxn = createStartedEditTxn(pair.target);
        transformer = new MetadataTransformer(
          { source: pair.source, target: targetTxn },
          {
            argsForProcessChanges: { changedInstanceIds },
            skipPropagateChangesToRootElements: true,
          }
        );
        const sourceCodeSpec = pair.source.codeSpecs.getById(
          pair.source.elements.getElement(initialSourceId).code.spec
        );
        transformer.context.remapCodeSpec(
          sourceCodeSpec.name,
          sourceCodeSpec.name
        );
        const inserts = vi.spyOn(targetTxn, "insertElement");
        await transformer.process();
        const insertedTargetId =
          transformer.context.findTargetElementId(insertedSourceId);
        expect(reservations.calls).toBe(initialReservationCalls + 1);
        expect(inserts).toHaveBeenCalledTimes(removeUpdateTarget ? 2 : 1);
        expect(pair.target.elements.getElement(initialTargetId).userLabel).toBe(
          "updated"
        );
        expect(insertedTargetId).not.toBe(Id64.invalid);
        expect(
          pair.target.elements.getElement(insertedTargetId).userLabel
        ).toBe("inserted");
        expect(transformer.exporter.sourceDbChanges).toBeUndefined();
        await transformer.process();
        expect(reservations.calls).toBe(initialReservationCalls + 1);
        expect(transformer.exporter.sourceDbChanges).toBeUndefined();
      } finally {
        transformer?.dispose();
        targetTxn?.end();
        await pair.dispose();
        reservations.dispose();
      }
    }
  );

  it("rejects duplicate pending federation identities before insertion", async () => {
    const source = createDb("duplicate-pending-source.bim");
    const target = createDb("duplicate-pending-target.bim");
    const duplicateGuid = Guid.createValue();
    const sourceIds = withEditTxn(source, "duplicate pending source", (txn) =>
      ["duplicate-a", "duplicate-b"].map((label) => {
        const props = Subject.create(
          source,
          IModel.rootSubjectId,
          label
        ).toJSON();
        props.federationGuid = Guid.createValue();
        return txn.insertElement(props);
      })
    );
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "duplicate-pending-sync.db"
      )
    );
    const targetTxn = createStartedEditTxn(target);
    class DuplicateIdentityTransformer extends IModelTransformer {
      public override async onTransformElement(
        sourceElement: Element
      ): Promise<ElementProps> {
        const props = await super.onTransformElement(sourceElement);
        if (sourceIds.includes(sourceElement.id))
          props.federationGuid = duplicateGuid;
        return props;
      }
    }
    const transformer = new DuplicateIdentityTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    const insert = vi.spyOn(targetTxn, "insertElement");
    try {
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
      );
      const sourceCodeSpec = source.codeSpecs.getById(
        source.elements.getElement(sourceIds[0]).code.spec
      );
      transformer.context.remapCodeSpec(
        sourceCodeSpec.name,
        sourceCodeSpec.name
      );
      const error = await transformer
        .processChildElements(IModel.rootSubjectId)
        .catch((caught: unknown) => caught);
      expect(
        ITwinError.isError(
          error,
          IModelTransformerErrorScope,
          IModelTransformerError.ElementReservationUnsupported
        )
      ).toBe(true);
      expect(reservations.calls).toBe(0);
      expect(insert).not.toHaveBeenCalled();
    } finally {
      transformer.dispose();
      targetTxn.end();
      source.close();
      reservations.dispose();
    }
  });

  it.each(["guid", "class", "code"] as const)(
    "rejects a custom importer that changes %s at the insertion boundary",
    async (mutation) => {
      const source = createDb(`importer-drift-${mutation}-source.bim`);
      const target = createDb(`importer-drift-${mutation}-target.bim`);
      const sourceId = withEditTxn(
        source,
        `importer drift ${mutation}`,
        (txn) => {
          const props = Subject.create(
            source,
            IModel.rootSubjectId,
            `importer-drift-${mutation}`
          ).toJSON();
          props.federationGuid = Guid.createValue();
          return txn.insertElement(props);
        }
      );
      const reservations = await useLocalReservations(
        target,
        IModelTransformerTestUtils.prepareOutputFile(
          "ElementReservations",
          `importer-drift-${mutation}-sync.db`
        )
      );
      const targetTxn = createStartedEditTxn(target);
      class MutatingImporter extends IModelImporter {
        protected override async onInsertElement(
          props: ElementProps
        ): Promise<string> {
          if (mutation === "guid") props.federationGuid = Guid.createValue();
          else if (mutation === "class")
            props.classFullName = "BisCore:Category";
          else props.code.value = `${props.code.value}:changed`;
          return super.onInsertElement(props);
        }
      }
      const importer = new MutatingImporter(targetTxn);
      const insert = vi.spyOn(targetTxn, "insertElement");
      const transformer = new IModelTransformer(
        { source, target: importer },
        { noProvenance: true }
      );
      try {
        transformer.context.remapElement(
          IModel.rootSubjectId,
          IModel.rootSubjectId
        );
        const sourceCodeSpec = source.codeSpecs.getById(
          source.elements.getElement(sourceId).code.spec
        );
        transformer.context.remapCodeSpec(
          sourceCodeSpec.name,
          sourceCodeSpec.name
        );
        const error = await transformer
          .processElement(sourceId)
          .catch((caught: unknown) => caught);
        expect(
          ITwinError.isError(
            error,
            IModelTransformerErrorScope,
            IModelTransformerError.ElementReservationPlanChanged
          )
        ).toBe(true);
        expect(reservations.calls).toBe(1);
        expect(insert).not.toHaveBeenCalled();
      } finally {
        transformer.dispose();
        targetTxn.end();
        source.close();
        reservations.dispose();
      }
    }
  );

  it("rethrows one failed reservation batch before any public element mutation", async () => {
    const source = createDb("failed-public-source.bim");
    const target = createDb("failed-public-target.bim");
    const specName = "FailedPublicScope";
    const sourceSpec = withEditTxn(source, "failed source code spec", (txn) =>
      source.codeSpecs.insert(
        txn,
        CodeSpec.create(source, specName, CodeScopeSpec.Type.Repository)
      )
    );
    const targetSpec = withEditTxn(target, "failed target code spec", (txn) => {
      target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, "failed-padding", CodeScopeSpec.Type.Repository)
      );
      return target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, specName, CodeScopeSpec.Type.Repository)
      );
    });
    const sourceId = withEditTxn(source, "failed source element", (txn) =>
      txn.insertElement({
        classFullName: Subject.classFullName,
        model: IModel.repositoryModelId,
        parent: {
          id: IModel.rootSubjectId,
          relClassName: "BisCore:SubjectOwnsSubjects",
        },
        federationGuid: Guid.createValue(),
        code: {
          spec: sourceSpec,
          scope: IModel.rootSubjectId,
          value: "failed",
        },
      })
    );
    const sourceGuid = source.elements.getElement(sourceId).federationGuid!;
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "failed-public-sync.db"
      )
    );
    await target.reservations.reserveElements({
      elements: [
        {
          federationGuid: sourceGuid,
          classFullName: Subject.classFullName,
          code: {
            spec: targetSpec,
            scope: IModel.rootSubjectId,
            value: "different-code",
          },
        },
      ],
    });
    const targetTxn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    const insert = vi.spyOn(targetTxn, "insertElement");
    const logError = vi.spyOn(Logger, "logError");
    try {
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
      );
      transformer.context.remapCodeSpec(specName, specName);
      const error = await transformer
        .processElement(sourceId)
        .catch((caught: unknown) => caught);
      expect(error).toBe(reservations.lastError);
      expect(insert).not.toHaveBeenCalled();
      expect(
        target.elements.getIdFromFederationGuid(sourceGuid)
      ).toBeUndefined();
      expect(reservations.calls).toBe(2);
      expect(
        logError.mock.calls.filter(
          ([, , metadata]) =>
            (metadata as { phase?: string } | undefined)?.phase ===
            "reserve-elements"
        )
      ).toHaveLength(1);
    } finally {
      logError.mockRestore();
      transformer.dispose();
      targetTxn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("does not reserve or insert during an in-place public processElement", async () => {
    const db = createDb("in-place-source-target.bim");
    const sourceId = withEditTxn(db, "in-place source element", (txn) => {
      const props = Subject.create(
        db,
        IModel.rootSubjectId,
        "in-place-element"
      ).toJSON();
      props.federationGuid = Guid.createValue();
      props.userLabel = "in-place-element";
      return txn.insertElement(props);
    });
    const reservations = await useLocalReservations(
      db,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "in-place-sync.db"
      )
    );
    const targetTxn = createStartedEditTxn(db);
    const transformer = new IModelTransformer(
      { source: db, target: targetTxn },
      { noProvenance: true }
    );
    const insert = vi.spyOn(targetTxn, "insertElement");
    try {
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
      );
      transformer.context.remapElement(sourceId, sourceId);
      await transformer.processElement(sourceId);
      expect(reservations.calls).toBe(0);
      expect(insert).not.toHaveBeenCalled();
      expect(db.elements.getElement(sourceId).userLabel).toBe(
        "in-place-element"
      );
    } finally {
      transformer.dispose();
      targetTxn.end();
      reservations.dispose();
    }
  });

  it("retains copied-target no-insert behavior", async () => {
    const source = createDb("copied-source.bim");
    const target = createDb("copied-target.bim");
    const sourceId = withEditTxn(source, "source child", (setupTxn) =>
      Subject.insert(setupTxn, IModel.rootSubjectId, "copied-child")
    );
    const props = source.elements.getElementProps(sourceId);
    const targetId = withEditTxn(target, "copy child", (setupTxn) =>
      setupTxn.insertElement({ ...props, id: undefined })
    );
    expect(targetId).toBe(sourceId);
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "copied-sync.db"
      )
    );
    const txn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: txn },
      { noProvenance: true, wasSourceIModelCopiedToTarget: true }
    );
    try {
      const insert = vi.spyOn(txn, "insertElement");
      const update = vi.spyOn(txn, "updateElement");
      await transformer.processElement(sourceId);
      expect(reservations.calls).toBe(0);
      expect(insert).not.toHaveBeenCalled();
      expect(update).not.toHaveBeenCalled();
      expect(transformer.context.findTargetElementId(sourceId)).toBe(targetId);
    } finally {
      transformer.dispose();
      txn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("uses the remapped root Subject for parent codes without remapping the repository model", async () => {
    const source = createDb("root-scope-source.bim");
    const target = createDb("root-scope-target.bim");
    const sourceId = withEditTxn(source, "root child", (setupTxn) =>
      Subject.insert(setupTxn, IModel.rootSubjectId, "root-child")
    );
    const destination = withEditTxn(target, "destination Subject", (setupTxn) =>
      Subject.insert(setupTxn, IModel.rootSubjectId, "destination")
    );
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "root-scope-sync.db"
      )
    );
    const txn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: txn },
      { noProvenance: true }
    );
    try {
      await transformer.processCodeSpecs();
      await transformer.processSubject(IModel.rootSubjectId, destination);
      const props = target.elements.getElementProps(
        transformer.context.findTargetElementId(sourceId)
      );
      expect(reservations.calls).toBe(1);
      expect(props.code.scope).toBe(destination);
      expect(props.parent?.id).toBe(destination);
      expect(props.model).toBe(IModel.repositoryModelId);
    } finally {
      transformer.dispose();
      txn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("keeps public subset selection inside one reservation operation", async () => {
    const source = createDb("subset-source.bim");
    const target = createDb("subset-target.bim");
    const ids = withEditTxn(source, "subset source hierarchy", (txn) => {
      const subject = Subject.create(
        source,
        IModel.rootSubjectId,
        "subset-subject"
      ).toJSON();
      subject.federationGuid = Guid.createValue();
      const subjectId = txn.insertElement(subject);
      const childIds = [0, 1].map((index) => {
        const child = Subject.create(
          source,
          subjectId,
          `subset-child-${index}`
        ).toJSON();
        child.federationGuid = Guid.createValue();
        return txn.insertElement(child);
      });
      return { subjectId, childIds };
    });
    const reservations = await useLocalReservations(
      target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "subset-sync.db"
      )
    );
    const targetTxn = createStartedEditTxn(target);
    const exported: string[] = [];
    class SelectionTransformer extends IModelTransformer {
      public override async onExportElement(sourceElement: Element) {
        exported.push(sourceElement.id);
        return super.onExportElement(sourceElement);
      }
    }
    const transformer = new SelectionTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    try {
      const sourceCodeSpec = source.codeSpecs.getById(
        source.elements.getElement(ids.childIds[0]).code.spec
      );
      transformer.context.remapCodeSpec(
        sourceCodeSpec.name,
        sourceCodeSpec.name
      );
      await transformer.processSubject(ids.subjectId, IModel.rootSubjectId);
      expect(reservations.calls).toBe(1);
      expect(exported).toEqual(expect.arrayContaining(ids.childIds));
      expect(exported).not.toContain(ids.subjectId);
      expect(
        ids.childIds.map((id) => transformer.context.findTargetElementId(id))
      ).not.toContain(Id64.invalid);
    } finally {
      transformer.dispose();
      targetTxn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("keeps the ordinary path when the target is not server-based", async () => {
    const source = createDb("noop-source.bim");
    const target = createDb("noop-target.bim");
    const sourceId = withEditTxn(source, "noop source element", (txn) => {
      const props = Subject.create(
        source,
        IModel.rootSubjectId,
        "noop-source"
      ).toJSON();
      props.federationGuid = Guid.createValue();
      return txn.insertElement(props);
    });
    const reserveElements = vi.spyOn(target.reservations, "reserveElements");
    const targetTxn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    try {
      expect(target.reservations.isServerBased).toBe(false);
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
      );
      await transformer.processElement(sourceId);
      expect(reserveElements).not.toHaveBeenCalled();
      expect(
        target.elements.getIdFromFederationGuid(
          source.elements.getElement(sourceId).federationGuid
        )
      ).toBeDefined();
    } finally {
      transformer.dispose();
      targetTxn.end();
      source.close();
      target.close();
    }
  });

  it("bypasses reservations when the real target schema lock is held", async () => {
    const pair = await createHubPair("schema-lock-public", true, false);
    const reservations = await useLocalReservations(
      pair.target,
      IModelTransformerTestUtils.prepareOutputFile(
        "ElementReservations",
        "schema-lock-sync.db"
      )
    );
    let targetTxn: ReturnType<typeof createStartedEditTxn> | undefined;
    let transformer: IModelTransformer | undefined;
    try {
      const sourceId = withEditTxn(
        pair.source,
        "schema lock source element",
        (txn) => {
          const props = Subject.create(
            pair.source,
            IModel.rootSubjectId,
            "schema-lock-source"
          ).toJSON();
          props.federationGuid = Guid.createValue();
          return txn.insertElement(props);
        }
      );
      await pair.source.pushChanges({
        accessToken: pair.accessToken,
        description: "schema lock source element",
      });
      await pair.target.acquireSchemaLock();
      expect(pair.target.holdsSchemaLock).toBe(true);
      targetTxn = createStartedEditTxn(pair.target);
      transformer = new IModelTransformer(
        { source: pair.source, target: targetTxn },
        { noProvenance: true }
      );
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
      );
      await transformer.processElement(sourceId);
      expect(reservations.calls).toBe(0);
      expect(
        pair.target.elements.getIdFromFederationGuid(
          pair.source.elements.getElement(sourceId).federationGuid
        )
      ).toBeDefined();
    } finally {
      transformer?.dispose();
      targetTxn?.end();
      await pair.dispose();
      reservations.dispose();
    }
  });

  it("records R18 preparation diagnostics for 25 and 250 candidates", async () => {
    const candidateCounts = [25, 250];
    const metrics: Array<{
      candidates: number;
      batches: number;
      preparationMs: number;
      wholeOperationMs: number;
      heapDeltaBytes: number;
    }> = [];

    for (const candidateCount of candidateCounts) {
      const source = createDb(`diagnostic-${candidateCount}-source.bim`);
      const target = createDb(`diagnostic-${candidateCount}-target.bim`);
      const specName = `DiagnosticParentScope${candidateCount}`;
      const sourceSpec = withEditTxn(
        source,
        "diagnostic source code spec",
        (txn) =>
          source.codeSpecs.insert(
            txn,
            CodeSpec.create(source, specName, CodeScopeSpec.Type.ParentElement)
          )
      );
      withEditTxn(target, "diagnostic target code spec", (txn) => {
        target.codeSpecs.insert(
          txn,
          CodeSpec.create(
            target,
            `diagnostic-padding-${candidateCount}`,
            CodeScopeSpec.Type.Repository
          )
        );
        target.codeSpecs.insert(
          txn,
          CodeSpec.create(target, specName, CodeScopeSpec.Type.ParentElement)
        );
      });
      const ids = withEditTxn(source, "diagnostic source graph", (txn) => {
        const parent = Subject.create(
          source,
          IModel.rootSubjectId,
          `diagnostic-parent-${candidateCount}`
        ).toJSON();
        parent.federationGuid = Guid.createValue();
        parent.code = {
          spec: sourceSpec,
          scope: IModel.rootSubjectId,
          value: `diagnostic-parent-${candidateCount}`,
        };
        const parentId = txn.insertElement(parent);
        const childIds = Array.from(
          { length: candidateCount - 1 },
          (_, index) => {
            const child = Subject.create(
              source,
              parentId,
              `diagnostic-child-${candidateCount}-${index}`
            ).toJSON();
            child.federationGuid = Guid.createValue();
            child.code = {
              spec: sourceSpec,
              scope: parentId,
              value: `diagnostic-child-${candidateCount}-${index}`,
            };
            return txn.insertElement(child);
          }
        );
        return [parentId, ...childIds];
      });

      const reservations = await useLocalReservations(
        target,
        IModelTransformerTestUtils.prepareOutputFile(
          "ElementReservations",
          `diagnostic-${candidateCount}-sync.db`
        )
      );
      const targetTxn = createStartedEditTxn(target);
      const transformer = new IModelTransformer(
        { source, target: targetTxn },
        { noProvenance: true }
      );
      const reservationApi = target.reservations;
      const reserveElements =
        reservationApi.reserveElements.bind(reservationApi);
      const startedAt = performance.now();
      const heapBefore = process.memoryUsage().heapUsed;
      let preparationMs = 0;
      const reserveSpy = vi
        .spyOn(reservationApi, "reserveElements")
        .mockImplementation(async (args) => {
          preparationMs = performance.now() - startedAt;
          return reserveElements(args);
        });
      try {
        transformer.context.remapElement(
          IModel.rootSubjectId,
          IModel.rootSubjectId
        );
        transformer.context.remapCodeSpec(specName, specName);
        await transformer.processElement(ids[0]);
        const wholeOperationMs = performance.now() - startedAt;
        const heapDeltaBytes = process.memoryUsage().heapUsed - heapBefore;
        metrics.push({
          candidates: candidateCount,
          batches: reservations.calls,
          preparationMs,
          wholeOperationMs,
          heapDeltaBytes,
        });
        expect(reserveSpy).toHaveBeenCalledTimes(1);
        expect(reservations.calls).toBe(1);
      } finally {
        reserveSpy.mockRestore();
        transformer.dispose();
        targetTxn.end();
        source.close();
        reservations.dispose();
      }
    }

    // Diagnostic only: these observations intentionally have no CI thresholds.
    process.stderr.write(
      `R18 element-reservation diagnostics ${JSON.stringify(metrics)}\n`
    );
    expect(metrics).toHaveLength(2);
    for (const metric of metrics) {
      expect(metric.batches).toBe(1);
      expect(metric.preparationMs).toBeGreaterThanOrEqual(0);
      expect(metric.wholeOperationMs).toBeGreaterThanOrEqual(
        metric.preparationMs
      );
    }
  });

  it("still rejects ordinary native cloning without a target model mapping", async () => {
    const source = createDb("model-source.bim");
    const target = createDb("model-target.bim");
    const specName = "ReservationModelScope";
    const sourceSpec = withEditTxn(source, "source model code spec", (txn) =>
      source.codeSpecs.insert(
        txn,
        CodeSpec.create(source, specName, CodeScopeSpec.Type.Model)
      )
    );
    const targetSpec = withEditTxn(target, "target model code spec", (txn) =>
      target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, specName, CodeScopeSpec.Type.Model)
      )
    );
    const { modelId, objectId } = withEditTxn(source, "source model", (txn) => {
      const insertedModelId = PhysicalModel.insert(
        txn,
        IModel.rootSubjectId,
        "model"
      );
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "category",
        new SubCategoryAppearance()
      );
      const objectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: insertedModelId,
        category: categoryId,
        federationGuid: Guid.createValue(),
        code: { spec: sourceSpec, scope: insertedModelId, value: "object" },
      };
      return {
        modelId: insertedModelId,
        objectId: txn.insertElement(objectProps),
      };
    });
    const modelGuid = source.elements.getElement(modelId).federationGuid!;
    const context = new IModelCloneContext(source, target);
    try {
      await context.initialize();
      context.remapCodeSpec(specName, specName);
      const sourceElement = source.elements.getElement(objectId);
      await expect(context.cloneElement(sourceElement)).rejects.toThrow(
        "Invalid target model"
      );
      expect(context.findTargetElementId(modelId)).toBe(Id64.invalid);
      expect(
        target.elements.getIdFromFederationGuid(modelGuid)
      ).toBeUndefined();

      // G1 stop/go control: an actual model, not a provisional ID, unblocks
      // native cloning. Inserting it during preparation would break the gate.
      const targetModelId = withEditTxn(target, "target model control", (txn) =>
        PhysicalModel.insert(txn, IModel.rootSubjectId, "target-model")
      );
      context.remapElement(modelId, targetModelId);
      const props = await context.cloneElement(sourceElement);
      expect(props.model).toBe(targetModelId);
      expect(props.federationGuid).toBe(sourceElement.federationGuid);
      expect(props.code).toEqual({
        spec: targetSpec,
        scope: targetModelId,
        value: "object",
      });
    } finally {
      context[Symbol.dispose]();
      source.close();
      target.close();
    }
  });
});
