/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSourceAspect,
  IModelElementCloneContext,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  StandaloneDb,
  SubCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Guid, Id64, ITwinError } from "@itwin/core-bentley";
import {
  Code,
  CodeScopeSpec,
  CodeSpec,
  ElementProps,
  ElementReservationError,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { describe, expect, it, vi } from "vitest";
import { Point3d } from "@itwin/core-geometry";
import { IModelTransformer } from "../../IModelTransformer";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";
import { PreparedElementIdentity } from "../../IModelCloneContext";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import { useLocalReservations } from "./ElementReservationTestUtils";

const outputFile = (name: string) =>
  IModelTransformerTestUtils.prepareOutputFile(
    "ElementReservationIdentity",
    name
  );

async function createScenario(
  name: string,
  transformerClass: typeof IModelTransformer = IModelTransformer,
  transformerOptions: ConstructorParameters<typeof IModelTransformer>[1] = {
    noProvenance: true,
  }
) {
  const source = StandaloneDb.createEmpty(outputFile(`${name}-source.bim`), {
    rootSubject: { name },
  });
  const target = StandaloneDb.createEmpty(outputFile(`${name}-target.bim`), {
    rootSubject: { name },
  });
  try {
    await target.importSchemaStrings([
      `<ECSchema schemaName="ReservationIdentity" alias="ri" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
      <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
      <ECEntityClass typeName="Remapped" modifier="Sealed"><BaseClass>bis:PhysicalElement</BaseClass></ECEntityClass>
      <ECEntityClass typeName="Transformed" modifier="Sealed"><BaseClass>bis:PhysicalElement</BaseClass></ECEntityClass>
    </ECSchema>`,
    ]);
    const specName = "IdentityPreparation";
    const sourceIds = withEditTxn(source, "source graph", (txn) => {
      const spec = source.codeSpecs.insert(
        txn,
        CodeSpec.create(source, specName, CodeScopeSpec.Type.Model)
      );
      const category = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "category",
        new SubCategoryAppearance()
      );
      const model = PhysicalModel.insert(txn, IModel.rootSubjectId, "model");
      const props: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model,
        category,
        federationGuid: Guid.createValue(),
        code: { spec, scope: model, value: "object" },
        geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 2, 3)),
        placement: {
          origin: { x: 0, y: 0, z: 0 },
          angles: { yaw: 0, pitch: 0, roll: 0 },
        },
      };
      return { spec, category, model, object: txn.insertElement(props) };
    });
    const targetIds = withEditTxn(
      target,
      "existing target prerequisites",
      (txn) => {
        target.codeSpecs.insert(
          txn,
          CodeSpec.create(target, "padding", CodeScopeSpec.Type.Repository)
        );
        const spec = target.codeSpecs.insert(
          txn,
          CodeSpec.create(target, specName, CodeScopeSpec.Type.Model)
        );
        const category = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "different-category",
          new SubCategoryAppearance()
        );
        const existingModel = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "existing-model"
        );
        return { spec, category, existingModel };
      }
    );
    const reservations = await useLocalReservations(
      target,
      outputFile(`${name}-sync.db`)
    );
    const targetTxn = createStartedEditTxn(target);
    const transformer = new transformerClass(
      { source, target: targetTxn },
      transformerOptions
    );
    try {
      const context = transformer["_cloneContext"];
      await context.initialize();
      context.remapElement(IModel.rootSubjectId, IModel.rootSubjectId);
      context.remapElement(sourceIds.category, targetIds.category);
      context.remapCodeSpec(specName, specName);
      const partition = source.elements.getElement(sourceIds.model);
      context.importCodeSpec(partition.code.spec);
      const object = source.elements.getElement(sourceIds.object);
      return {
        source,
        target,
        sourceIds,
        targetIds,
        context,
        transformer,
        txn: targetTxn,
        reservations,
        partition,
        object,
        async prepare() {
          const model = await context.prepareElementIdentity(
            partition,
            IModel.rootSubjectId,
            async () => transformer.onTransformElement(partition)
          );
          const element = await context.prepareElementIdentity(
            object,
            model.identity.federationGuid!,
            async () => transformer.onTransformElement(object)
          );
          return { model, element };
        },
        async reserve(prepared: PreparedElementIdentity[]) {
          await target.reservations.reserveElements({
            elements: prepared.map(({ identity }) => ({
              ...identity,
              federationGuid: identity.federationGuid!,
            })),
          });
        },
        async insertModel(prepared: PreparedElementIdentity) {
          const props = await context.transformPreparedElement(
            prepared,
            async () => transformer.onTransformElement(partition)
          );
          props.id = undefined;
          const id = targetTxn.insertElement(props);
          context.remapElement(sourceIds.model, id);
          targetTxn.insertModel(
            transformer.onTransformModel(
              source.models.getModel(sourceIds.model),
              id
            )
          );
          return id;
        },
        [Symbol.dispose]() {
          try {
            transformer.dispose();
            targetTxn.end();
          } finally {
            try {
              reservations.dispose();
            } finally {
              source.close();
              vi.restoreAllMocks();
            }
          }
        },
      };
    } catch (error) {
      try {
        transformer.dispose();
        targetTxn.end();
      } finally {
        reservations.dispose();
      }
      throw error;
    }
  } catch (error) {
    source.close();
    if (target.isOpen) target.close();
    throw error;
  }
}

describe("TypeScript reservation identity preparation", () => {
  it("reserves a new model through the public processModel entry point", async () => {
    using scenario = await createScenario("public-new-model");
    const insertElement = vi.spyOn(scenario.txn, "insertElement");
    const insertModel = vi.spyOn(scenario.txn, "insertModel");
    await scenario.transformer.processModel(scenario.sourceIds.model);
    const modelId = scenario.context.findTargetElementId(
      scenario.sourceIds.model
    );
    const objectId = scenario.context.findTargetElementId(
      scenario.sourceIds.object
    );
    expect(scenario.reservations.calls).toBe(1);
    expect(insertElement).toHaveBeenCalledTimes(2);
    expect(insertModel).toHaveBeenCalledTimes(1);
    expect(modelId).not.toBe(Id64.invalid);
    expect(objectId).not.toBe(Id64.invalid);
    expect(
      scenario.reservations.sync.findReservedElement(
        scenario.partition.federationGuid!
      )?.elementId
    ).toBe(modelId);
    expect(
      scenario.reservations.sync.findReservedElement(
        scenario.object.federationGuid!
      )?.elementId
    ).toBe(objectId);
    expect(scenario.target.elements.getElementProps(objectId).model).toBe(
      modelId
    );
  });

  it("reserves only the selected contents through public processModelContents", async () => {
    using scenario = await createScenario("public-model-contents");
    await scenario.transformer.processModelContents(
      scenario.sourceIds.model,
      scenario.targetIds.existingModel,
      PhysicalObject.classFullName
    );
    const objectId = scenario.context.findTargetElementId(
      scenario.sourceIds.object
    );
    expect(scenario.reservations.calls).toBe(1);
    expect(objectId).not.toBe(Id64.invalid);
    expect(scenario.target.elements.getElementProps(objectId).model).toBe(
      scenario.targetIds.existingModel
    );
  });

  it("lets category insertion own its default subcategory and preserves appearances", async () => {
    using scenario = await createScenario("new-category");
    const sourceCategory = scenario.source.elements.getElement<SpatialCategory>(
      scenario.sourceIds.category
    );
    const sourceDefaultId = sourceCategory.myDefaultSubCategoryId();
    scenario.context.removeElement(scenario.sourceIds.category);
    scenario.context.removeElement(sourceDefaultId);
    const sourceExtraId = withEditTxn(
      scenario.source,
      "subcategory appearances",
      (txn) => {
        sourceCategory.setDefaultAppearance(txn, { transp: 0.4 });
        return SubCategory.insert(txn, sourceCategory.id, "extra", {
          transp: 0.6,
        });
      }
    );
    const reserve = vi.spyOn(scenario.target.reservations, "reserveElements");
    await scenario.transformer.process();
    expect(reserve).toHaveBeenCalledTimes(1);
    const sourceDefaultGuid =
      scenario.source.elements.getElement(sourceDefaultId).federationGuid;
    expect(
      [...reserve.mock.calls[0][0].elements].map(
        (entry) => entry.federationGuid
      )
    ).not.toContain(sourceDefaultGuid);
    const targetCategoryId = scenario.context.findTargetElementId(
      sourceCategory.id
    );
    const targetDefaultId = scenario.target.elements
      .getElement<SpatialCategory>(targetCategoryId)
      .myDefaultSubCategoryId();
    expect(scenario.context.findTargetElementId(sourceDefaultId)).toBe(
      targetDefaultId
    );
    expect(
      scenario.target.elements.getElement<SubCategory>(targetDefaultId)
        .appearance.transparency
    ).toBe(0.4);
    const extra = scenario.target.elements.getElement<SubCategory>(
      scenario.context.findTargetElementId(sourceExtraId)
    );
    expect(extra.getCategoryId()).toBe(targetCategoryId);
    expect(extra.appearance.transparency).toBe(0.6);
    expect(
      scenario.reservations.sync.findReservedElement(
        sourceCategory.federationGuid!
      )?.elementId
    ).toBe(targetCategoryId);
  });

  it("defers implicit scope provenance until the reservation succeeds", async () => {
    using scenario = await createScenario(
      "implicit-provenance",
      IModelTransformer,
      {}
    );
    await scenario.transformer.processModel(scenario.sourceIds.model);
    const scopeAspects = scenario.target.elements.getAspects(
      IModel.rootSubjectId,
      ExternalSourceAspect.classFullName
    );
    expect(scopeAspects).toHaveLength(1);
    expect((scopeAspects[0] as ExternalSourceAspect).kind).toBe(
      ExternalSourceAspect.Kind.Scope
    );
  });

  it("defers legacy scope migration and refreshes its cached synchronization version", async () => {
    class VersionTransformer extends IModelTransformer {
      public async readVersion() {
        return this.getSynchronizationVersion();
      }
    }
    using scenario = await createScenario(
      "deferred-migration",
      VersionTransformer,
      { branchRelationshipDataBehavior: "unsafe-migrate" }
    );
    const transformer = scenario.transformer as VersionTransformer;
    const scopeId = scenario.txn.insertAspect({
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: IModel.rootSubjectId },
      scope: { id: IModel.rootSubjectId },
      identifier: scenario.source.iModelId,
      kind: ExternalSourceAspect.Kind.Scope,
      version: "older;3",
      jsonProperties: JSON.stringify({
        reverseSyncVersion: "",
        pendingSyncChangesetIndices: [],
        pendingReverseSyncChangesetIndices: [],
      }),
    } as ExternalSourceAspectProps);
    expect(await transformer.readVersion()).toEqual({ id: "older", index: 3 });
    scenario.txn.updateAspect({
      ...scenario.target.elements.getAspect(scopeId).toJSON(),
      version: "",
      jsonProperties: "{}",
    } as ExternalSourceAspectProps);
    const failure = new Error(
      "reservation failure after migration preparation"
    );
    vi.spyOn(
      scenario.target.reservations,
      "reserveElements"
    ).mockImplementationOnce(async () => {
      expect(await transformer.readVersion()).toEqual({ id: "", index: -1 });
      expect(
        (scenario.target.elements.getAspect(scopeId) as ExternalSourceAspect)
          .jsonProperties
      ).toBe("{}");
      throw failure;
    });
    await expect(
      transformer.processModel(scenario.sourceIds.model)
    ).rejects.toBe(failure);
    expect(
      (scenario.target.elements.getAspect(scopeId) as ExternalSourceAspect)
        .jsonProperties
    ).toBe("{}");
    expect(await transformer.readVersion()).toEqual({ id: "older", index: 3 });
    await transformer.processModel(scenario.sourceIds.model);
    expect(
      JSON.parse(
        (scenario.target.elements.getAspect(scopeId) as ExternalSourceAspect)
          .jsonProperties ?? "{}"
      )
    ).toEqual({
      reverseSyncVersion: "",
      pendingSyncChangesetIndices: [],
      pendingReverseSyncChangesetIndices: [],
    });
    expect(await transformer.readVersion()).toEqual({ id: "", index: -1 });
  });

  it("retries implicit initialization after reservation failure without losing scope provenance", async () => {
    using scenario = await createScenario(
      "implicit-provenance-retry",
      IModelTransformer,
      {}
    );
    const failure = new Error("reservation service unavailable");
    vi.spyOn(
      scenario.target.reservations,
      "reserveElements"
    ).mockRejectedValueOnce(failure);
    const insertAspect = vi.spyOn(scenario.txn, "insertAspect");
    await expect(
      scenario.transformer.processModel(scenario.sourceIds.model)
    ).rejects.toBe(failure);
    expect(insertAspect).not.toHaveBeenCalled();
    expect(
      scenario.target.elements.getAspects(
        IModel.rootSubjectId,
        ExternalSourceAspect.classFullName
      )
    ).toHaveLength(0);
    await scenario.transformer.processModel(scenario.sourceIds.model);
    const scopes = scenario.target.elements.getAspects(
      IModel.rootSubjectId,
      ExternalSourceAspect.classFullName
    );
    expect(scopes).toHaveLength(1);
    expect((scopes[0] as ExternalSourceAspect).kind).toBe(
      ExternalSourceAspect.Kind.Scope
    );
  });

  it.each([false, true])(
    "preserves conflict-code clearing without masking hook drift (%s)",
    async (drift) => {
      class CodeTransformer extends IModelTransformer {
        public drift = false;
        public override async onTransformElement(
          element: Element
        ): Promise<ElementProps> {
          const props = await super.onTransformElement(element);
          if (this.drift) props.code.value = "changed-after-reservation";
          return props;
        }
      }
      using scenario = await createScenario(
        `conflict-code-${drift}`,
        CodeTransformer
      );
      const conflictingIdentity = {
        classFullName: "ReservationIdentity:Remapped",
        federationGuid: Guid.createValue(),
        code: {
          spec: scenario.targetIds.spec,
          scope: scenario.targetIds.existingModel,
          value: "object",
        },
      };
      await scenario.target.reservations.reserveElements({
        elements: [conflictingIdentity],
      });
      const conflictingId = scenario.txn.insertElement({
        ...conflictingIdentity,
        model: scenario.targetIds.existingModel,
        category: scenario.targetIds.category,
      } as PhysicalElementProps);
      scenario.context.remapElement(
        scenario.sourceIds.model,
        scenario.targetIds.existingModel
      );
      const reserve = scenario.target.reservations.reserveElements.bind(
        scenario.target.reservations
      );
      vi.spyOn(
        scenario.target.reservations,
        "reserveElements"
      ).mockImplementation(async (args) => {
        await reserve(args);
        (scenario.transformer as CodeTransformer).drift = drift;
      });
      const insert = vi.spyOn(scenario.txn, "insertElement");
      const processing = scenario.transformer.processElement(
        scenario.sourceIds.object
      );
      if (drift) {
        const error = await processing.catch((caught: unknown) => caught);
        expect(
          ITwinError.isError(
            error,
            IModelTransformerErrorScope,
            IModelTransformerError.ElementReservationPlanChanged
          )
        ).toBe(true);
        expect(insert).not.toHaveBeenCalled();
      } else {
        await processing;
        expect(insert).toHaveBeenCalledTimes(1);
        const id = scenario.context.findTargetElementId(
          scenario.sourceIds.object
        );
        expect(scenario.target.elements.getElementProps(id).code).toEqual(
          Code.createEmpty().toJSON()
        );
      }
      expect(
        scenario.target.elements.getElementProps(conflictingId).code.value
      ).toBe("object");
    }
  );

  it("rejects a related-code-scope cycle before recursive export or target writes", async () => {
    using scenario = await createScenario("code-cycle");
    const specName = "CyclicRelatedCodes";
    withEditTxn(scenario.source, "related scope cycle", (txn) => {
      const spec = scenario.source.codeSpecs.insert(
        txn,
        CodeSpec.create(
          scenario.source,
          specName,
          CodeScopeSpec.Type.RelatedElement
        )
      );
      const other = txn.insertElement({
        ...scenario.object.toJSON(),
        id: undefined,
        federationGuid: Guid.createValue(),
        code: Code.createEmpty().toJSON(),
      });
      txn.updateElement({
        ...scenario.object.toJSON(),
        code: { spec, scope: other, value: "first" },
      });
      txn.updateElement({
        ...scenario.source.elements.getElementProps(other),
        code: { spec, scope: scenario.sourceIds.object, value: "second" },
      });
    });
    scenario.target.codeSpecs.insert(
      scenario.txn,
      CodeSpec.create(
        scenario.target,
        specName,
        CodeScopeSpec.Type.RelatedElement
      )
    );
    scenario.context.remapCodeSpec(specName, specName);
    scenario.context.remapElement(
      scenario.sourceIds.model,
      scenario.targetIds.existingModel
    );
    const insert = vi.spyOn(scenario.txn, "insertElement");
    const error = await scenario.transformer
      .processElement(scenario.sourceIds.object)
      .catch((caught: unknown) => caught);
    expect(
      ITwinError.isError(
        error,
        IModelTransformerErrorScope,
        IModelTransformerError.ElementReservationUnsupported
      )
    ).toBe(true);
    expect((error as Error).message).toContain("cycle");
    expect(scenario.reservations.calls).toBe(0);
    expect(insert).not.toHaveBeenCalled();
  });

  it("prepares a new model and its contained element before native cloning is possible", async () => {
    using scenario = await createScenario("new-model");
    const {
      context,
      transformer,
      object,
      target,
      sourceIds,
      targetIds,
      txn,
      reservations,
    } = scenario;
    const insertElement = vi.spyOn(txn, "insertElement");
    const insertModel = vi.spyOn(txn, "insertModel");
    await expect(transformer.onTransformElement(object)).rejects.toThrow(
      "Invalid target model"
    );

    const prepared = await scenario.prepare();
    expect(insertElement).not.toHaveBeenCalled();
    expect(insertModel).not.toHaveBeenCalled();
    expect(reservations.calls).toBe(0);
    expect(context.findTargetElementId(sourceIds.model)).toBe(Id64.invalid);
    expect(prepared.element.identity).toEqual({
      classFullName: object.classFullName,
      federationGuid: object.federationGuid,
      code: {
        spec: targetIds.spec,
        scope: prepared.model.identity.federationGuid,
        value: "object",
      },
    });
    expect(sourceIds.spec).not.toBe(targetIds.spec);
    expect(Object.keys(prepared.element).sort()).toEqual([
      "codeBeforeHooks",
      "identity",
      "sourceId",
    ]);
    expect(Object.keys(prepared.element.identity).sort()).toEqual([
      "classFullName",
      "code",
      "federationGuid",
    ]);

    await scenario.reserve([prepared.model, prepared.element]);
    expect(reservations.calls).toBe(1);
    const modelId = await scenario.insertModel(prepared.model);
    const props = await context.transformPreparedElement(
      prepared.element,
      async () => transformer.onTransformElement(object)
    );
    expect(props.model).toBe(modelId);
    expect((props as PhysicalElementProps).category).toBe(targetIds.category);
    props.id = undefined;
    const objectId = txn.insertElement(props);
    expect(modelId).toBe(
      reservations.sync.findReservedElement(
        prepared.model.identity.federationGuid!
      )?.elementId
    );
    expect(objectId).toBe(
      reservations.sync.findReservedElement(
        prepared.element.identity.federationGuid!
      )?.elementId
    );
    expect(objectId).not.toBe(sourceIds.object);
    expect(target.elements.getElementProps(objectId).code.scope).toBe(modelId);
    const sourceGeometry = scenario.source.elements.getElement<PhysicalObject>({
      id: sourceIds.object,
      wantGeometry: true,
    }).geom;
    expect(sourceGeometry).toBeDefined();
    expect(
      target.elements.getElement<PhysicalObject>({
        id: objectId,
        wantGeometry: true,
      }).geom
    ).toEqual(sourceGeometry);
  });

  it.each([false, true])(
    "inserts a GUID-less element without reserving (preserve IDs: %s)",
    async (preserveElementIdsForFiltering) => {
      using scenario = await createScenario(
        `unreserved-${preserveElementIdsForFiltering}`,
        IModelTransformer,
        { noProvenance: true, preserveElementIdsForFiltering }
      );
      withEditTxn(scenario.source, "clear source GUID", (txn) =>
        txn.updateElement({
          ...scenario.object.toJSON(),
          federationGuid: Guid.empty,
        })
      );
      expect(
        scenario.source.elements.getElement(scenario.sourceIds.object)
          .federationGuid
      ).toBeUndefined();
      expect(
        scenario.target.elements.tryGetElementProps(scenario.sourceIds.object)
      ).toBeUndefined();
      const insert = vi.spyOn(scenario.txn, "insertElement");
      await scenario.transformer.processModelContents(
        scenario.sourceIds.model,
        scenario.targetIds.existingModel
      );
      expect(scenario.reservations.calls).toBe(0);
      expect(insert).toHaveBeenCalledTimes(1);
      expect(insert.mock.calls[0][1]?.forceUseId).toBe(
        preserveElementIdsForFiltering
      );
      const targetId = scenario.context.findTargetElementId(
        scenario.sourceIds.object
      );
      expect(scenario.target.elements.getElementProps(targetId).model).toBe(
        scenario.targetIds.existingModel
      );
      if (preserveElementIdsForFiltering)
        expect(targetId).toBe(scenario.sourceIds.object);
    }
  );

  it("rejects preserveElementIdsForFiltering before reserving a new public insert", async () => {
    using scenario = await createScenario(
      "preserve-id-public",
      IModelTransformer,
      {
        noProvenance: true,
        preserveElementIdsForFiltering: true,
      }
    );
    const insert = vi.spyOn(scenario.txn, "insertElement");
    const error = await scenario.transformer
      .processElement(scenario.partition.id)
      .catch((caught: unknown) => caught);
    expect(
      ITwinError.isError(
        error,
        IModelTransformerErrorScope,
        IModelTransformerError.ElementReservationUnsupported
      )
    ).toBe(true);
    expect(scenario.reservations.calls).toBe(0);
    expect(insert).not.toHaveBeenCalled();
  });

  it("preserves backend validation of a malformed transformed GUID before inserts", async () => {
    class MalformedGuidTransformer extends IModelTransformer {
      public override async onTransformElement(
        element: Element
      ): Promise<ElementProps> {
        const props = await super.onTransformElement(element);
        if (element instanceof PhysicalObject)
          props.federationGuid = "not-a-guid";
        return props;
      }
    }
    using scenario = await createScenario(
      "malformed-guid",
      MalformedGuidTransformer
    );
    const reserve = scenario.target.reservations.reserveElements.bind(
      scenario.target.reservations
    );
    let backendError: unknown;
    vi.spyOn(
      scenario.target.reservations,
      "reserveElements"
    ).mockImplementation(async (args) => {
      try {
        await reserve(args);
      } catch (caught) {
        backendError = caught;
        throw caught;
      }
    });
    const insert = vi.spyOn(scenario.txn, "insertElement");
    const error = await scenario.transformer
      .processModel(scenario.sourceIds.model)
      .catch((caught: unknown) => caught);
    expect(
      ITwinError.isError(
        error,
        ElementReservationError.scope,
        "invalid-reservation"
      )
    ).toBe(true);
    expect(error).toBe(backendError);
    expect(scenario.reservations.calls).toBe(0);
    expect(insert).not.toHaveBeenCalled();
  });

  it("rejects a public new-model reservation when the modeled element has no GUID", async () => {
    using scenario = await createScenario("missing-model-guid");
    withEditTxn(scenario.source, "clear model federation GUID", (txn) =>
      txn.updateElement({
        ...scenario.partition.toJSON(),
        federationGuid: Guid.empty,
      })
    );
    const error = await scenario.transformer
      .processModel(scenario.sourceIds.model)
      .catch((caught: unknown) => caught);
    expect(
      ITwinError.isError(
        error,
        IModelTransformerErrorScope,
        IModelTransformerError.ElementReservationUnsupported
      )
    ).toBe(true);
    expect(scenario.reservations.calls).toBe(0);
  });

  it("rejects a dependent parent-scope insert when its scope has no GUID", async () => {
    const source = StandaloneDb.createEmpty(
      outputFile("missing-scope-source"),
      {
        rootSubject: { name: "missing-scope" },
      }
    );
    const target = StandaloneDb.createEmpty(
      outputFile("missing-scope-target"),
      {
        rootSubject: { name: "missing-scope" },
      }
    );
    const specName = "MissingScopeParent";
    const sourceSpec = withEditTxn(source, "missing scope code spec", (txn) =>
      source.codeSpecs.insert(
        txn,
        CodeSpec.create(source, specName, CodeScopeSpec.Type.ParentElement)
      )
    );
    withEditTxn(target, "missing scope target code spec", (txn) =>
      target.codeSpecs.insert(
        txn,
        CodeSpec.create(target, specName, CodeScopeSpec.Type.ParentElement)
      )
    );
    const ids = withEditTxn(source, "missing scope hierarchy", (txn) => {
      const parent = Subject.create(
        source,
        IModel.rootSubjectId,
        "missing-parent"
      ).toJSON();
      parent.federationGuid = Guid.empty;
      const parentId = txn.insertElement(parent);
      const child = Subject.create(
        source,
        parentId,
        "dependent-child"
      ).toJSON();
      child.federationGuid = Guid.createValue();
      child.code = {
        spec: sourceSpec,
        scope: parentId,
        value: "dependent-child",
      };
      return { parentId, childId: txn.insertElement(child) };
    });
    const reservations = await useLocalReservations(
      target,
      outputFile("missing-scope-sync")
    );
    const targetTxn = createStartedEditTxn(target);
    const transformer = new IModelTransformer(
      { source, target: targetTxn },
      { noProvenance: true }
    );
    try {
      transformer.context.remapCodeSpec(specName, specName);
      transformer.context.remapElement(
        IModel.rootSubjectId,
        IModel.rootSubjectId
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
      expect(transformer.context.findTargetElementId(ids.childId)).toBe(
        Id64.invalid
      );
    } finally {
      transformer.dispose();
      targetTxn.end();
      source.close();
      reservations.dispose();
    }
  });

  it("uses a public processElement call for a code-matched existing target", async () => {
    using scenario = await createScenario("public-code-match");
    withEditTxn(scenario.source, "match public existing model", (txn) => {
      txn.updateElement({
        ...scenario.partition.toJSON(),
        code: { ...scenario.partition.code.toJSON(), value: "existing-model" },
      });
    });
    scenario.context.remapCodeSpec(
      scenario.source.codeSpecs.getById(scenario.partition.code.spec).name,
      scenario.source.codeSpecs.getById(scenario.partition.code.spec).name
    );
    scenario.context.remapElement(IModel.rootSubjectId, IModel.rootSubjectId);
    await scenario.transformer.processElement(scenario.partition.id);
    expect(scenario.reservations.calls).toBe(0);
    expect(scenario.context.findTargetElementId(scenario.partition.id)).toBe(
      scenario.targetIds.existingModel
    );
  });

  it("uses the real target identity of a code-matched scope with a different GUID", async () => {
    using scenario = await createScenario("code-matched-scope");
    const existingGuid = scenario.target.elements.getElement(
      scenario.targetIds.existingModel
    ).federationGuid;
    expect(existingGuid).not.toBe(scenario.partition.federationGuid);
    withEditTxn(scenario.source, "match existing model code", (txn) => {
      txn.updateElement({
        ...scenario.partition.toJSON(),
        code: { ...scenario.partition.code.toJSON(), value: "existing-model" },
      });
    });
    const partition = scenario.source.elements.getElement(
      scenario.partition.id
    );
    const insert = vi.spyOn(scenario.txn, "insertElement");
    const update = vi.spyOn(scenario.txn, "updateElement");
    const preparedModel = await scenario.context.prepareElementIdentity(
      partition,
      IModel.rootSubjectId,
      async () => scenario.transformer.onTransformElement(partition)
    );
    const matchedId = await scenario.transformer["findTargetElementMatch"](
      partition,
      preparedModel.identity
    );
    expect(matchedId).toBe(scenario.targetIds.existingModel);
    expect(scenario.context.findTargetElementId(partition.id)).toBe(matchedId);
    const element = await scenario.context.prepareElementIdentity(
      scenario.object,
      matchedId,
      async () => scenario.transformer.onTransformElement(scenario.object)
    );
    expect(element.identity.code.scope).toBe(matchedId);
    expect(insert).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(scenario.reservations.calls).toBe(0);
    await scenario.reserve([element]);
    expect(scenario.reservations.calls).toBe(1);
    const props = await scenario.context.transformPreparedElement(
      element,
      async () => scenario.transformer.onTransformElement(scenario.object)
    );
    props.id = undefined;
    const id = scenario.txn.insertElement(props);
    expect(scenario.target.elements.getElementProps(id).code.scope).toBe(
      matchedId
    );
    expect(scenario.target.elements.getElement(matchedId).federationGuid).toBe(
      existingGuid
    );
    expect(
      scenario.reservations.sync.findReservedElement(partition.federationGuid!)
    ).toBeUndefined();
  });

  it("honors class remapping, onCloned and onTransformElement identity changes exactly once per pass", async () => {
    const transformedGuid = Guid.createValue();
    class CustomTransformer extends IModelTransformer {
      public override async onTransformElement(
        source: Element
      ): Promise<ElementProps> {
        const props = await super.onTransformElement(source);
        if (source instanceof PhysicalObject) {
          props.classFullName = "ReservationIdentity:Transformed";
          props.federationGuid = transformedGuid;
          props.code.value = `${props.code.value}:transform`;
        }
        return props;
      }
    }
    using scenario = await createScenario("customization", CustomTransformer);
    const seenClasses: string[] = [];
    vi.spyOn(
      PhysicalObject as unknown as {
        onCloned(
          context: IModelElementCloneContext,
          source: ElementProps,
          target: ElementProps
        ): Promise<void>;
      },
      "onCloned"
    ).mockImplementation(async (_context, _source, props) => {
      seenClasses.push(props.classFullName);
      expect("code" in props).toBe(true);
      expect(
        Object.getOwnPropertyDescriptor(props, "classFullName")?.value
      ).toBe("ReservationIdentity:Remapped");
      props.code.value = `${props.code.value}:clone`;
    });
    scenario.context.remapElementClass(
      PhysicalObject.classFullName,
      "ReservationIdentity:Remapped"
    );
    const prepared = await scenario.prepare();
    expect(seenClasses).toEqual(["ReservationIdentity:Remapped"]);
    expect(prepared.element.identity.federationGuid).toBe(transformedGuid);
    expect(prepared.element.identity.classFullName).toBe(
      "ReservationIdentity:Transformed"
    );
    expect(prepared.element.identity.code.value).toBe("object:clone:transform");
    expect(prepared.element.codeBeforeHooks.value).toBe("object");
    expect(scenario.object.code.value).toBe("object");

    await scenario.reserve([prepared.model, prepared.element]);
    const modelId = await scenario.insertModel(prepared.model);
    const replayProps = await scenario.context.transformPreparedElement(
      prepared.element,
      async () => scenario.transformer.onTransformElement(scenario.object)
    );
    expect(seenClasses).toEqual([
      "ReservationIdentity:Remapped",
      "ReservationIdentity:Remapped",
    ]);
    replayProps.id = undefined;
    const objectId = scenario.txn.insertElement(replayProps);
    const stored = scenario.target.elements.getElementProps(objectId);
    expect(stored.classFullName).toBe("ReservationIdentity:Transformed");
    expect(stored.federationGuid).toBe(transformedGuid);
    expect(stored.code.value).toBe("object:clone:transform");
    expect(stored.code.scope).toBe(modelId);
    expect(objectId).toBe(
      scenario.reservations.sync.findReservedElement(transformedGuid)?.elementId
    );
  });

  it.each(["read", "spread", "has", "descriptor"])(
    "rejects a hook that needs incomplete target props (%s), and restores preparation state",
    async (operation) => {
      class TargetDependentTransformer extends IModelTransformer {
        public override async onTransformElement(
          source: Element
        ): Promise<ElementProps> {
          const props = await super.onTransformElement(source);
          if (source instanceof PhysicalObject) {
            if (operation === "read") props.code.value = props.model;
            else if (operation === "has")
              props.code.value = "model" in props ? "present" : "absent";
            else if (operation === "descriptor")
              Object.getOwnPropertyDescriptor(props, "model");
            else return { ...props };
          }
          return props;
        }
      }
      using scenario = await createScenario(
        `unsupported-${operation}`,
        TargetDependentTransformer
      );
      const insert = vi.spyOn(scenario.txn, "insertElement");
      const error = await scenario.prepare().catch((caught: unknown) => caught);
      expect(
        ITwinError.isError(
          error,
          IModelTransformerErrorScope,
          IModelTransformerError.ElementReservationUnsupported
        )
      ).toBe(true);
      expect(insert).not.toHaveBeenCalled();
      expect(scenario.reservations.calls).toBe(0);
      expect(scenario.context.isPreparingIdentityOnly(scenario.object.id)).toBe(
        false
      );
      await expect(
        scenario.transformer.onTransformElement(scenario.object)
      ).rejects.toThrow("Invalid target model");
    }
  );

  it("preserves stored code whitespace instead of copying the trimmed Element.code value", async () => {
    using scenario = await createScenario("exact-code");
    scenario.source.codeValueBehavior = "exact";
    scenario.target.codeValueBehavior = "exact";
    withEditTxn(scenario.source, "set exact source code", (txn) => {
      txn.updateElement({
        ...scenario.object.toJSON(),
        code: { ...scenario.object.code.toJSON(), value: "  object  " },
      });
    });
    expect(
      scenario.source.elements.getElement(scenario.object.id).code.value
    ).toBe("object");
    const prepared = await scenario.prepare();
    expect(prepared.element.identity.code.value).toBe("  object  ");
    await scenario.reserve([prepared.model, prepared.element]);
    await scenario.insertModel(prepared.model);
    const props = await scenario.context.transformPreparedElement(
      prepared.element,
      async () => scenario.transformer.onTransformElement(scenario.object)
    );
    props.id = undefined;
    const id = scenario.txn.insertElement(props);
    expect(scenario.target.elements.getElementProps(id).code.value).toBe(
      "  object  "
    );
  });

  it("prepares genuinely empty codes without inventing a model-scoped code", async () => {
    using scenario = await createScenario("empty-code");
    withEditTxn(scenario.source, "clear source code", (txn) => {
      txn.updateElement({
        ...scenario.object.toJSON(),
        code: Code.createEmpty(),
      });
    });
    const object = scenario.source.elements.getElement(
      scenario.sourceIds.object
    );
    const model = await scenario.context.prepareElementIdentity(
      scenario.partition,
      IModel.rootSubjectId,
      async () => scenario.transformer.onTransformElement(scenario.partition)
    );
    const element = await scenario.context.prepareElementIdentity(
      object,
      IModel.rootSubjectId,
      async () => scenario.transformer.onTransformElement(object)
    );
    expect(Code.isEmpty(element.identity.code)).toBe(true);
    await scenario.reserve([model, element]);
    await scenario.insertModel(model);
    const props = await scenario.context.transformPreparedElement(
      element,
      async () => scenario.transformer.onTransformElement(object)
    );
    props.id = undefined;
    const id = scenario.txn.insertElement(props);
    expect(
      Code.isEmpty(scenario.target.elements.getElementProps(id).code)
    ).toBe(true);
  });

  it("uses complete native-cloned props when the target model already exists", async () => {
    class ModelAwareTransformer extends IModelTransformer {
      public override async onTransformElement(
        source: Element
      ): Promise<ElementProps> {
        const props = await super.onTransformElement(source);
        if (source instanceof PhysicalObject)
          props.code.value = `model:${props.model}`;
        return props;
      }
    }
    using scenario = await createScenario(
      "existing-model",
      ModelAwareTransformer
    );
    const model = await scenario.context.prepareElementIdentity(
      scenario.partition,
      IModel.rootSubjectId,
      async () => scenario.transformer.onTransformElement(scenario.partition)
    );
    await scenario.reserve([model]);
    const modelId = await scenario.insertModel(model);
    // A later operation may use target dependencies that already exist.
    const element = await scenario.context.prepareElementIdentity(
      scenario.object,
      modelId,
      async () => scenario.transformer.onTransformElement(scenario.object)
    );
    expect(element.identity.code.value).toBe(`model:${modelId}`);
    await scenario.reserve([element]);
    const replayProps = await scenario.context.transformPreparedElement(
      element,
      async () => scenario.transformer.onTransformElement(scenario.object)
    );
    replayProps.id = undefined;
    const id = scenario.txn.insertElement(replayProps);
    expect(scenario.target.elements.getElementProps(id).code.scope).toBe(
      modelId
    );
  });

  it("retries a failed public processModel on the same transformer", async () => {
    let fail = true;
    class RetryTransformer extends IModelTransformer {
      public override async onTransformElement(
        source: Element
      ): Promise<ElementProps> {
        if (fail && source instanceof PhysicalObject)
          throw new Error("retryable public preparation failure");
        return super.onTransformElement(source);
      }
    }
    using scenario = await createScenario("public-retry", RetryTransformer);
    const firstError = await scenario.transformer
      .processModel(scenario.sourceIds.model)
      .catch((caught: unknown) => caught);
    expect(firstError).toBeInstanceOf(Error);
    expect((firstError as Error).message).toBe(
      "retryable public preparation failure"
    );
    expect(scenario.reservations.calls).toBe(0);
    expect(scenario.context.findTargetElementId(scenario.sourceIds.model)).toBe(
      Id64.invalid
    );
    fail = false;
    await scenario.transformer.processModel(scenario.sourceIds.model);
    expect(scenario.reservations.calls).toBe(1);
    expect(
      scenario.context.findTargetElementId(scenario.sourceIds.object)
    ).not.toBe(Id64.invalid);
  });

  it("preserves a hook error and permits preparation to be retried", async () => {
    using scenario = await createScenario("hook-failure");
    const failure = new Error("custom failure");
    const hook = vi
      .spyOn(scenario.transformer, "onTransformElement")
      .mockRejectedValueOnce(failure);
    await expect(scenario.prepare()).rejects.toBe(failure);
    hook.mockRestore();
    expect(scenario.context.isPreparingIdentityOnly(scenario.object.id)).toBe(
      false
    );
    const prepared = await scenario.prepare();
    expect(prepared.element.identity.federationGuid).toBe(
      scenario.object.federationGuid
    );
    expect(scenario.reservations.calls).toBe(0);
  });

  it("reuses reservations and rolls back only a failed batch, including its ID allocation", async () => {
    using scenario = await createScenario("batch-rollback");
    const prepared = await scenario.prepare();
    await scenario.reserve([prepared.model, prepared.element]);
    const oldId = scenario.reservations.sync.findReservedElement(
      prepared.element.identity.federationGuid!
    )!.elementId;
    await scenario.reserve([prepared.model, prepared.element]);
    expect(
      scenario.reservations.sync.findReservedElement(
        prepared.element.identity.federationGuid!
      )?.elementId
    ).toBe(oldId);
    const fresh = {
      ...prepared.element.identity,
      federationGuid: Guid.createValue(),
      code: { ...prepared.element.identity.code, value: "fresh" },
    };
    const conflict = {
      ...prepared.element.identity,
      federationGuid: prepared.element.identity.federationGuid!,
      code: { ...prepared.element.identity.code, value: "conflicting" },
    };
    const insert = vi.spyOn(scenario.txn, "insertElement");
    const error = await scenario.target.reservations
      .reserveElements({ elements: [fresh, conflict] })
      .catch((caught: unknown) => caught);
    expect(
      ITwinError.isError(
        error,
        ElementReservationError.scope,
        "reservation-conflict"
      )
    ).toBe(true);
    expect(error).toBe(scenario.reservations.lastError);
    expect(insert).not.toHaveBeenCalled();
    expect(
      scenario.reservations.sync.findReservedElement(fresh.federationGuid)
    ).toBeUndefined();
    expect(
      scenario.reservations.sync.findReservedElement(conflict.federationGuid)
        ?.elementId
    ).toBe(oldId);
    await scenario.target.reservations.reserveElements({ elements: [fresh] });
    expect(
      scenario.reservations.sync.findReservedElement(fresh.federationGuid)
        ?.elementId
    ).toBe(
      Id64.fromLocalAndBriefcaseIds(
        Id64.getLocalId(oldId) + 1,
        Id64.getBriefcaseId(oldId)
      )
    );
  });

  it("rejects an equivalent numeric scope after GUID reservation, and reservations survive caller abandonment", async () => {
    using scenario = await createScenario("scope-conflict");
    const prepared = await scenario.prepare();
    await scenario.reserve([prepared.model, prepared.element]);
    const modelId = await scenario.insertModel(prepared.model);
    const props = await scenario.context.transformPreparedElement(
      prepared.element,
      async () => scenario.transformer.onTransformElement(scenario.object)
    );
    props.id = undefined;
    props.code.scope = modelId;
    let error: unknown;
    try {
      scenario.txn.insertElement(props);
    } catch (caught) {
      error = caught;
    }
    expect(
      ITwinError.isError(
        error,
        ElementReservationError.scope,
        "reservation-conflict"
      )
    ).toBe(true);
    scenario.txn.abandonChanges();
    expect(scenario.target.models.tryGetModelProps(modelId)).toBeUndefined();
    expect(
      scenario.reservations.sync.findReservedElement(
        prepared.model.identity.federationGuid!
      )?.elementId
    ).toBe(modelId);
    expect(
      scenario.reservations.sync.findReservedElement(
        prepared.element.identity.federationGuid!
      )
    ).toBeDefined();
  });

  it("rejects identity drift without consuming the reserved element ID", async () => {
    let suffix = "prepared";
    class ChangingTransformer extends IModelTransformer {
      public override async onTransformElement(
        source: Element
      ): Promise<ElementProps> {
        const props = await super.onTransformElement(source);
        if (source instanceof PhysicalObject)
          props.code.value = `${props.code.value}:${suffix}`;
        return props;
      }
    }
    using scenario = await createScenario("drift", ChangingTransformer);
    const prepared = await scenario.prepare();
    await scenario.reserve([prepared.model, prepared.element]);
    await scenario.insertModel(prepared.model);
    suffix = "changed";
    const error = await scenario.context
      .transformPreparedElement(prepared.element, async () =>
        scenario.transformer.onTransformElement(scenario.object)
      )
      .catch((caught: unknown) => caught);
    expect(
      ITwinError.isError(
        error,
        IModelTransformerErrorScope,
        IModelTransformerError.ElementReservationPlanChanged
      )
    ).toBe(true);
    const reserved = scenario.reservations.sync.findReservedElement(
      prepared.element.identity.federationGuid!
    );
    expect(reserved).toBeDefined();
    expect(
      scenario.target.elements.tryGetElement(reserved!.elementId)
    ).toBeUndefined();
  });
});
