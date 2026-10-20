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
import { Guid, Id64 } from "@itwin/core-bentley";
import {
  Code,
  CodeScopeSpec,
  CodeSpec,
  ElementProps,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import { describe, expect, it, vi } from "vitest";
import { IModelTransformer } from "../../IModelTransformer";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import { useLocalReservations } from "./ElementReservationTestUtils";

async function createScenario(
  name: string,
  transformerClass = IModelTransformer,
  options: ConstructorParameters<typeof IModelTransformer>[1] = {
    noProvenance: true,
  }
) {
  using resources = new DisposableStack();
  const file = (suffix: string) =>
    IModelTransformerTestUtils.prepareOutputFile(
      "ElementReservationIdentity",
      `${name}-${suffix}`
    );
  const source = resources.adopt(
    StandaloneDb.createEmpty(file("source.bim"), { rootSubject: { name } }),
    (db) => db.close()
  );
  const target = resources.adopt(
    StandaloneDb.createEmpty(file("target.bim"), { rootSubject: { name } }),
    (db) => {
      if (db.isOpen) db.close();
    }
  );
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
    const object = txn.insertElement({
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
    } as PhysicalElementProps);
    return { spec, category, model, object };
  });
  const targetIds = withEditTxn(target, "target prerequisites", (txn) => {
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
  });
  const reservations = resources.adopt(
    await useLocalReservations(target, file("sync.db")),
    (adapter) => adapter.dispose()
  );
  const targetTxn = resources.adopt(createStartedEditTxn(target), (edit) =>
    edit.end()
  );
  const transformer = resources.adopt(
    new transformerClass({ source, target: targetTxn }, options),
    (instance) => instance.dispose()
  );
  const context = transformer["_cloneContext"];
  await context.initialize();
  context.remapElement(IModel.rootSubjectId, IModel.rootSubjectId);
  context.remapElement(sourceIds.category, targetIds.category);
  context.remapCodeSpec(specName, specName);
  const owned = resources.move();
  return {
    source,
    target,
    sourceIds,
    targetIds,
    reservations,
    txn: targetTxn,
    transformer,
    context,
    [Symbol.dispose]: () => owned.dispose(),
  };
}

const reservationError = (key: IModelTransformerError) => ({
  iTwinErrorId: { scope: IModelTransformerErrorScope, key },
});

describe("reservation identities", () => {
  it.each([false, true])(
    "clones geometry and customized identities (existing model: %s)",
    async (existingModel) => {
      const guid = Guid.createValue();
      class CustomTransformer extends IModelTransformer {
        public override async onTransformElement(
          element: Element
        ): Promise<ElementProps> {
          const props = await super.onTransformElement(element);
          if (element instanceof PhysicalObject) {
            props.classFullName = "ReservationIdentity:Transformed";
            props.federationGuid = guid;
            props.code.value = `${props.code.value}:${existingModel ? props.model : "new"}`;
          }
          return props;
        }
      }
      using s = await createScenario(
        `custom-${existingModel}`,
        CustomTransformer
      );
      const classes: string[] = [];
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
        classes.push(props.classFullName);
        props.code.value = `${props.code.value}:clone`;
      });
      s.context.remapElementClass(
        PhysicalObject.classFullName,
        "ReservationIdentity:Remapped"
      );
      const reserve = vi.spyOn(s.target.reservations, "reserveElements");
      if (existingModel)
        await s.transformer.processModelContents(
          s.sourceIds.model,
          s.targetIds.existingModel
        );
      else await s.transformer.processModel(s.sourceIds.model);
      const modelId = s.context.findTargetElementId(s.sourceIds.model);
      const objectId = s.context.findTargetElementId(s.sourceIds.object);
      const stored = s.target.elements.getElementProps(objectId);
      expect(reserve).toHaveBeenCalledTimes(1);
      expect([...reserve.mock.calls[0][0].elements]).toHaveLength(
        existingModel ? 1 : 2
      );
      expect(stored).toMatchObject({
        classFullName: "ReservationIdentity:Transformed",
        federationGuid: guid,
        model: modelId,
        category: s.targetIds.category,
        code: {
          spec: s.targetIds.spec,
          scope: modelId,
          value: `object:clone:${existingModel ? modelId : "new"}`,
        },
      });
      expect(s.sourceIds.spec).not.toBe(s.targetIds.spec);
      expect(objectId).not.toBe(s.sourceIds.object);
      expect(objectId).toBe(
        s.reservations.sync.findReservedElement(guid)?.elementId
      );
      // Reading props.model exceeds the identity-only view, so planning retries
      // with a full clone when the target model exists.
      expect(classes).toEqual(
        Array(existingModel ? 3 : 2).fill("ReservationIdentity:Remapped")
      );
      expect(
        s.target.elements.getElement<PhysicalObject>({
          id: objectId,
          wantGeometry: true,
        }).geom
      ).toEqual(
        s.source.elements.getElement<PhysicalObject>({
          id: s.sourceIds.object,
          wantGeometry: true,
        }).geom
      );
    }
  );

  it("leaves default-subcategory insertion to the backend", async () => {
    using s = await createScenario("category");
    const category = s.source.elements.getElement<SpatialCategory>(
      s.sourceIds.category
    );
    const defaultId = category.myDefaultSubCategoryId();
    s.context.removeElement(category.id);
    s.context.removeElement(defaultId);
    const extraId = withEditTxn(s.source, "appearances", (txn) => {
      category.setDefaultAppearance(txn, { transp: 0.4 });
      return SubCategory.insert(txn, category.id, "extra", { transp: 0.6 });
    });
    const reserve = vi.spyOn(s.target.reservations, "reserveElements");
    await s.transformer.process();
    expect(reserve).toHaveBeenCalledTimes(1);
    expect(
      [...reserve.mock.calls[0][0].elements].map(
        (entry) => entry.federationGuid
      )
    ).not.toContain(s.source.elements.getElement(defaultId).federationGuid);
    const targetCategory = s.target.elements.getElement<SpatialCategory>(
      s.context.findTargetElementId(category.id)
    );
    expect(s.context.findTargetElementId(defaultId)).toBe(
      targetCategory.myDefaultSubCategoryId()
    );
    expect(
      s.target.elements.getElement<SubCategory>(
        s.context.findTargetElementId(defaultId)
      ).appearance.transparency
    ).toBe(0.4);
    const extra = s.target.elements.getElement<SubCategory>(
      s.context.findTargetElementId(extraId)
    );
    expect(extra.getCategoryId()).toBe(targetCategory.id);
    expect(extra.appearance.transparency).toBe(0.6);
  });

  it("defers scope provenance and retries implicit initialization after reservation failure", async () => {
    using s = await createScenario("provenance", IModelTransformer, {});
    const failure = new Error("reservation unavailable");
    vi.spyOn(s.target.reservations, "reserveElements").mockRejectedValueOnce(
      failure
    );
    const insertAspect = vi.spyOn(s.txn, "insertAspect");
    await expect(s.transformer.processModel(s.sourceIds.model)).rejects.toBe(
      failure
    );
    expect(insertAspect).not.toHaveBeenCalled();
    await s.transformer.processModel(s.sourceIds.model);
    const aspects = s.target.elements.getAspects(
      IModel.rootSubjectId,
      ExternalSourceAspect.classFullName
    );
    expect(aspects).toHaveLength(1);
    expect((aspects[0] as ExternalSourceAspect).kind).toBe(
      ExternalSourceAspect.Kind.Scope
    );
  });

  it("defers legacy scope migration and restores its cached version on failure", async () => {
    class VersionTransformer extends IModelTransformer {
      public async readVersion() {
        return this.getSynchronizationVersion();
      }
    }
    using s = await createScenario("migration", VersionTransformer, {
      branchRelationshipDataBehavior: "unsafe-migrate",
    });
    const transformer = s.transformer as VersionTransformer;
    const scopeId = s.txn.insertAspect({
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: IModel.rootSubjectId },
      scope: { id: IModel.rootSubjectId },
      identifier: s.source.iModelId,
      kind: ExternalSourceAspect.Kind.Scope,
      version: "older;3",
      jsonProperties: JSON.stringify({
        reverseSyncVersion: "",
        pendingSyncChangesetIndices: [],
        pendingReverseSyncChangesetIndices: [],
      }),
    } as ExternalSourceAspectProps);
    expect(await transformer.readVersion()).toEqual({ id: "older", index: 3 });
    s.txn.updateAspect({
      ...s.target.elements.getAspect(scopeId).toJSON(),
      version: "",
      jsonProperties: "{}",
    } as ExternalSourceAspectProps);
    const failure = new Error("migration reservation failure");
    vi.spyOn(s.target.reservations, "reserveElements").mockImplementationOnce(
      async () => {
        expect(await transformer.readVersion()).toEqual({ id: "", index: -1 });
        expect(
          (s.target.elements.getAspect(scopeId) as ExternalSourceAspect)
            .jsonProperties
        ).toBe("{}");
        throw failure;
      }
    );
    await expect(transformer.processModel(s.sourceIds.model)).rejects.toBe(
      failure
    );
    expect(
      (s.target.elements.getAspect(scopeId) as ExternalSourceAspect)
        .jsonProperties
    ).toBe("{}");
    expect(await transformer.readVersion()).toEqual({ id: "older", index: 3 });
    await transformer.processModel(s.sourceIds.model);
    expect(
      JSON.parse(
        (s.target.elements.getAspect(scopeId) as ExternalSourceAspect)
          .jsonProperties ?? "{}"
      )
    ).toEqual({
      reverseSyncVersion: "",
      pendingSyncChangesetIndices: [],
      pendingReverseSyncChangesetIndices: [],
    });
    expect(await transformer.readVersion()).toEqual({ id: "", index: -1 });
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
          props.code.value = this.drift
            ? "changed"
            : `${props.code.value}:transformed`;
          return props;
        }
      }
      using s = await createScenario(`conflict-${drift}`, CodeTransformer);
      const identity = {
        classFullName: "ReservationIdentity:Remapped",
        federationGuid: Guid.createValue(),
        code: {
          spec: s.targetIds.spec,
          scope: s.targetIds.existingModel,
          value: "object:transformed",
        },
      };
      await s.target.reservations.reserveElements({ elements: [identity] });
      const conflictingId = s.txn.insertElement({
        ...identity,
        model: s.targetIds.existingModel,
        category: s.targetIds.category,
      } as PhysicalElementProps);
      const reserve = s.target.reservations.reserveElements.bind(
        s.target.reservations
      );
      vi.spyOn(s.target.reservations, "reserveElements").mockImplementation(
        async (args) => {
          await reserve(args);
          (s.transformer as CodeTransformer).drift = drift;
        }
      );
      const insert = vi.spyOn(s.txn, "insertElement");
      const operation = s.transformer.processModelContents(
        s.sourceIds.model,
        s.targetIds.existingModel
      );
      if (drift) {
        await expect(operation).rejects.toMatchObject(
          reservationError(IModelTransformerError.ElementReservationPlanChanged)
        );
        expect(insert).not.toHaveBeenCalled();
      } else {
        await operation;
        expect(insert).toHaveBeenCalledTimes(1);
        expect(
          s.target.elements.getElementProps(
            s.context.findTargetElementId(s.sourceIds.object)
          ).code
        ).toEqual(Code.createEmpty().toJSON());
      }
      expect(s.target.elements.getElementProps(conflictingId).code.value).toBe(
        identity.code.value
      );
    }
  );

  it("rejects a related-code-scope cycle before reservation or writes", async () => {
    using s = await createScenario("cycle");
    const name = "RelatedCodes";
    withEditTxn(s.source, "cycle", (txn) => {
      const spec = s.source.codeSpecs.insert(
        txn,
        CodeSpec.create(s.source, name, CodeScopeSpec.Type.RelatedElement)
      );
      const props = s.source.elements.getElementProps(s.sourceIds.object);
      const other = txn.insertElement({
        ...props,
        id: undefined,
        federationGuid: Guid.createValue(),
        code: Code.createEmpty().toJSON(),
      });
      txn.updateElement({
        ...props,
        code: { spec, scope: other, value: "first" },
      });
      txn.updateElement({
        ...s.source.elements.getElementProps(other),
        code: { spec, scope: props.id!, value: "second" },
      });
    });
    s.target.codeSpecs.insert(
      s.txn,
      CodeSpec.create(s.target, name, CodeScopeSpec.Type.RelatedElement)
    );
    s.context.remapCodeSpec(name, name);
    const insert = vi.spyOn(s.txn, "insertElement");
    await expect(
      s.transformer.processModelContents(
        s.sourceIds.model,
        s.targetIds.existingModel
      )
    ).rejects.toMatchObject({
      ...reservationError(IModelTransformerError.ElementReservationUnsupported),
      message: expect.stringContaining("cycle"),
    });
    expect(s.reservations.calls).toBe(0);
    expect(insert).not.toHaveBeenCalled();
  });

  it.each([false, true])(
    "leaves GUID-less inserts unreserved (preserve IDs: %s)",
    async (preserveElementIdsForFiltering) => {
      using s = await createScenario(
        `no-guid-${preserveElementIdsForFiltering}`,
        IModelTransformer,
        { noProvenance: true, preserveElementIdsForFiltering }
      );
      withEditTxn(s.source, "clear GUID", (txn) =>
        txn.updateElement({
          ...s.source.elements.getElementProps(s.sourceIds.object),
          federationGuid: Guid.empty,
        })
      );
      const insert = vi.spyOn(s.txn, "insertElement");
      await s.transformer.processModelContents(
        s.sourceIds.model,
        s.targetIds.existingModel
      );
      expect(s.reservations.calls).toBe(0);
      expect(insert).toHaveBeenCalledTimes(1);
      expect(insert.mock.calls[0][1]?.forceUseId).toBe(
        preserveElementIdsForFiltering
      );
      if (preserveElementIdsForFiltering)
        expect(s.context.findTargetElementId(s.sourceIds.object)).toBe(
          s.sourceIds.object
        );
    }
  );

  it.each(["preserve-id", "missing-model-guid"])(
    "rejects unsupported reserved model insertion (%s)",
    async (mode) => {
      using s = await createScenario(mode, IModelTransformer, {
        noProvenance: true,
        preserveElementIdsForFiltering: mode === "preserve-id",
      });
      if (mode === "missing-model-guid")
        withEditTxn(s.source, "clear model GUID", (txn) =>
          txn.updateElement({
            ...s.source.elements.getElementProps(s.sourceIds.model),
            federationGuid: Guid.empty,
          })
        );
      const insert = vi.spyOn(s.txn, "insertElement");
      await expect(
        s.transformer.processModel(s.sourceIds.model)
      ).rejects.toMatchObject(
        reservationError(IModelTransformerError.ElementReservationUnsupported)
      );
      expect(s.reservations.calls).toBe(0);
      expect(insert).not.toHaveBeenCalled();
    }
  );

  it("uses a code-matched model's real ID, not its source GUID", async () => {
    using s = await createScenario("code-match");
    const sourceModel = s.source.elements.getElementProps(s.sourceIds.model);
    expect(
      s.target.elements.getElement(s.targetIds.existingModel).federationGuid
    ).not.toBe(sourceModel.federationGuid);
    withEditTxn(s.source, "match code", (txn) =>
      txn.updateElement({
        ...sourceModel,
        code: { ...sourceModel.code, value: "existing-model" },
      })
    );
    const reserve = vi.spyOn(s.target.reservations, "reserveElements");
    await s.transformer.processModel(s.sourceIds.model);
    expect(s.context.findTargetElementId(s.sourceIds.model)).toBe(
      s.targetIds.existingModel
    );
    const entries = [...reserve.mock.calls[0][0].elements];
    expect(entries).toHaveLength(1);
    expect(entries[0].code?.scope).toBe(s.targetIds.existingModel);
    expect(
      s.reservations.sync.findReservedElement(sourceModel.federationGuid!)
    ).toBeUndefined();
    await s.transformer.processElement(s.sourceIds.model);
    expect(reserve).toHaveBeenCalledTimes(1);
  });

  it.each(["read", "spread", "has", "descriptor"])(
    "rejects unavailable target props and restores preparation state (%s)",
    async (operation) => {
      let invalid = true;
      class TargetDependentTransformer extends IModelTransformer {
        public override async onTransformElement(
          element: Element
        ): Promise<ElementProps> {
          const props = await super.onTransformElement(element);
          if (invalid && element instanceof PhysicalObject) {
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
      using s = await createScenario(
        `unsupported-${operation}`,
        TargetDependentTransformer
      );
      const insert = vi.spyOn(s.txn, "insertElement");
      await expect(
        s.transformer.processModel(s.sourceIds.model)
      ).rejects.toMatchObject(
        reservationError(IModelTransformerError.ElementReservationUnsupported)
      );
      expect(insert).not.toHaveBeenCalled();
      expect(s.reservations.calls).toBe(0);
      expect(s.context.isPreparingIdentityOnly(s.sourceIds.object)).toBe(false);
      expect(s.context.findTargetElementId(s.sourceIds.model)).toBe(
        Id64.invalid
      );
      invalid = false;
      await s.transformer.processModel(s.sourceIds.model);
      expect(s.reservations.calls).toBe(1);
    }
  );

  it.each(["  object  ", ""])(
    "preserves exact and genuinely empty codes (%j)",
    async (value) => {
      using s = await createScenario(`code-${value.length}`);
      s.source.codeValueBehavior = s.target.codeValueBehavior = "exact";
      withEditTxn(s.source, "source code", (txn) => {
        const props = s.source.elements.getElementProps(s.sourceIds.object);
        txn.updateElement({
          ...props,
          code: value ? { ...props.code, value } : Code.createEmpty().toJSON(),
        });
      });
      await s.transformer.processModel(s.sourceIds.model);
      const code = s.target.elements.getElementProps(
        s.context.findTargetElementId(s.sourceIds.object)
      ).code;
      if (value) expect(code.value).toBe(value);
      else expect(Code.isEmpty(code)).toBe(true);
    }
  );

  it("restores mappings, handler, and guard after replay failure without rolling back caller writes", async () => {
    const failure = new Error("replay failure");
    class FailingTransformer extends IModelTransformer {
      public override async onExportModel(): Promise<void> {
        throw failure;
      }
    }
    using s = await createScenario("replay-failure", FailingTransformer);
    const model = s.source.elements.getElementProps(s.sourceIds.model);
    withEditTxn(s.source, "match model", (txn) =>
      txn.updateElement({
        ...model,
        code: { ...model.code, value: "existing-model" },
      })
    );
    await expect(s.transformer.processModel(s.sourceIds.model)).rejects.toBe(
      failure
    );
    expect(s.reservations.calls).toBe(1);
    expect(s.context.findTargetElementId(s.sourceIds.model)).toBe(Id64.invalid);
    expect(s.context.findTargetElementId(s.sourceIds.category)).toBe(
      s.targetIds.category
    );
    expect(s.transformer.exporter["handler"]).toBe(s.transformer);
    expect(
      s.target.elements.getElement(s.targetIds.existingModel).federationGuid
    ).toBe(model.federationGuid);
    const props = Subject.create(
      s.target,
      IModel.rootSubjectId,
      "after-failure"
    ).toJSON();
    props.federationGuid = undefined;
    await s.transformer.importer.importElement(props);
    expect(Id64.isValidId64(props.id ?? Id64.invalid)).toBe(true);
  });
});
