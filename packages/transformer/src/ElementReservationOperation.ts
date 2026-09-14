/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @internal */

import { strict as assert } from "node:assert";
import { Id64, Id64String, ITwinError, Logger } from "@itwin/core-bentley";
import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  ElementMultiAspect,
  ElementUniqueAspect,
  IModelDb,
  Model,
  Relationship,
} from "@itwin/core-backend";
import { Code, CodeProps, ElementProps, IModel } from "@itwin/core-common";
import {
  ExportSchemaResult,
  IModelExporter,
  IModelExportHandler,
} from "./IModelExporter";
import {
  ElementReservationIdentity,
  IModelCloneContext,
  PreparedElementIdentity,
} from "./IModelCloneContext";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import {
  ReservationExportContext,
  setElementReservationInsertionGuard,
  setReservationExportContext,
} from "./ElementReservationState";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError";

const loggerCategory = TransformerLoggerCategory.IModelTransformer;

/** The identity of an element which is allowed to reach ordinary insertion. @internal */
export interface ElementReservationElementPlan {
  readonly sourceId: Id64String;
  readonly targetElementId: Id64String;
  /** Only unreserved preserve-ID inserts may supply an ID. */
  readonly insertId?: Id64String;
  /** Native insertion of this Category owns creation of the default SubCategory. */
  readonly autoInsertedForCategory?: Id64String;
  readonly prepared?: PreparedElementIdentity;
  readonly identity: ElementReservationIdentity;
}

/** The ordered, compact event journal used by one reservation operation. @internal */
export type ElementReservationEvent =
  | {
      readonly kind: "element";
      readonly sourceId: Id64String;
      readonly isUpdate: boolean | undefined;
      readonly plan: ElementReservationElementPlan;
    }
  | {
      readonly kind: "model";
      readonly sourceId: Id64String;
      readonly isUpdate: boolean | undefined;
    }
  | {
      readonly kind: "delete-model";
      readonly sourceId: Id64String;
    }
  | {
      readonly kind: "delete-elements";
      readonly sourceIds: readonly Id64String[];
    }
  | {
      readonly kind: "aspect-cleanup";
      readonly excludedClassFullNames: readonly string[];
      readonly sourceIds: readonly Id64String[];
    }
  | {
      readonly kind: "unique-aspect";
      readonly sourceId: Id64String;
      readonly isUpdate: boolean | undefined;
    }
  | {
      readonly kind: "multi-aspects";
      readonly sourceIds: readonly Id64String[];
    }
  | {
      readonly kind: "relationship";
      readonly sourceId: Id64String;
      readonly classFullName: string;
      readonly isUpdate: boolean | undefined;
      readonly sourceFedGuid?: string;
      readonly targetFedGuid?: string;
    }
  | {
      readonly kind: "delete-relationship";
      readonly sourceId: Id64String;
    }
  | {
      readonly kind: "skip-element";
      readonly sourceId: Id64String;
    }
  | {
      readonly kind: "repair-elements" | "repair-aspects";
    }
  | {
      readonly kind: "progress";
    };

export interface ElementReservationOperationCallbacks {
  /** Classifies one accepted source element without importing it. */
  collectElement(
    sourceElement: Element
  ): Promise<ElementReservationElementPlan | undefined>;
  /** Replays an aspect cleanup event after target identities exist. */
  replayAspectCleanup(
    excludedClassFullNames: ReadonlySet<string>,
    sourceIds: ReadonlySet<Id64String>
  ): Promise<void>;
  /** Preserve the existing reference-repair position among execution events. */
  repairReferences(kind: "repair-elements" | "repair-aspects"): Promise<void>;
  /** Persists deferred scope provenance after reservation succeeds. */
  persistProvenance(): Promise<void>;
  /** Discards deferred scope provenance and preparation-only mappings. */
  discardPreparation(): void;
  /** Rejects a known unsupported preparation or identity case. */
  rejectUnsupported(sourceId: Id64String, reason: string): never;
}

/** A private exporter adapter and ordered journal for one outer transformer operation. @internal */
export class ElementReservationOperation extends IModelExportHandler {
  private readonly _events: ElementReservationEvent[] = [];
  private readonly _elementPlans = new Map<
    Id64String,
    ElementReservationElementPlan
  >();
  private readonly _seenElements = new Set<Id64String>();
  private readonly _visitingElements = new Set<Id64String>();
  private readonly _scheduledModels = new Set<Id64String>();
  private readonly _journaledModels = new Set<Id64String>();
  private readonly _checkedRequiredDependencies = new Set<Id64String>();
  private readonly _exportContext: ReservationExportContext = {
    replaying: false,
  };
  private readonly _modeledPartitionDeletes = new Set<Id64String>();
  private _phase: "collecting" | "replaying" | "done" | "aborted" =
    "collecting";
  private _installed = false;
  private _flushed = false;
  private _currentInsertPlan: ElementReservationElementPlan | undefined;
  private _insertWasObserved = false;

  public constructor(
    private readonly _sourceDb: IModelDb,
    private readonly _targetDb: IModelDb,
    private readonly _exporter: IModelExporter,
    private readonly _importer: object,
    private readonly _delegate: IModelExportHandler,
    private readonly _cloneContext: IModelCloneContext,
    private readonly _callbacks: ElementReservationOperationCallbacks
  ) {
    super();
  }

  public get isCollecting(): boolean {
    return this._phase === "collecting";
  }

  public get isReplaying(): boolean {
    return this._phase === "replaying";
  }

  public get isDone(): boolean {
    return this._flushed;
  }

  public install(): void {
    if (this._installed) return;
    this._exporter.registerHandler(this);
    setReservationExportContext(this._exporter, this._exportContext);
    this._installed = true;
  }

  public restoreDelegate(): void {
    if (!this._installed) return;
    this._exporter.registerHandler(this._delegate);
    this._installed = false;
  }

  public release(): void {
    this.restoreDelegate();
    setElementReservationInsertionGuard(this._importer, undefined);
    setReservationExportContext(this._exporter, undefined);
  }

  /** True if the source element is already a real target mapping or a prepared logical identity. */
  public hasElementIdentity(sourceId: Id64String): boolean {
    return (
      this._elementPlans.has(sourceId) ||
      Id64.isValidId64(this._cloneContext.findTargetElementId(sourceId))
    );
  }

  public getElementPlan(
    sourceId: Id64String
  ): ElementReservationElementPlan | undefined {
    return this._elementPlans.get(sourceId);
  }

  /** Check dependency edges, not model-content traversal edges, before recursive export starts. */
  public assertNoRequiredDependencyCycle(
    sourceElement: Element,
    getRequiredReferences: (element: Element) => Id64String[]
  ): void {
    const visiting = new Set<Id64String>();
    const stack = [{ id: sourceElement.id, leaving: false }];
    while (stack.length > 0) {
      const frame = stack.pop();
      assert(frame !== undefined);
      const { id, leaving } = frame;
      if (leaving) {
        visiting.delete(id);
        this._checkedRequiredDependencies.add(id);
        continue;
      }
      if (
        id === IModel.rootSubjectId ||
        this.hasElementIdentity(id) ||
        this._checkedRequiredDependencies.has(id)
      )
        continue;
      if (visiting.has(id))
        this._callbacks.rejectUnsupported(
          id,
          "a required element or code-scope dependency forms a cycle"
        );
      const element =
        id === sourceElement.id
          ? sourceElement
          : this._sourceDb.elements.tryGetElement(id);
      // Missing references retain the ordinary dangling-reference policy.
      if (element === undefined) continue;
      visiting.add(id);
      stack.push({ id, leaving: true });
      const references = getRequiredReferences(element);
      if (element.code.spec !== Code.createEmpty().spec)
        references.push(element.code.scope);
      for (const reference of references) {
        if (Id64.isValidId64(reference))
          stack.push({ id: reference, leaving: false });
      }
    }
  }

  public isVisiting(sourceId: Id64String): boolean {
    return this._visitingElements.has(sourceId);
  }

  public isModelScheduled(sourceModelId: Id64String): boolean {
    return this._scheduledModels.has(sourceModelId);
  }

  public hasTargetModel(sourceModelId: Id64String): boolean {
    const targetModeledElementId = this.getLogicalScope(sourceModelId);
    return (
      targetModeledElementId !== undefined &&
      Id64.isValidId64(targetModeledElementId) &&
      this._targetDb.models.tryGetModelProps(targetModeledElementId) !==
        undefined
    );
  }

  public markModelScheduled(sourceModelId: Id64String): boolean {
    if (this._scheduledModels.has(sourceModelId)) return false;
    this._scheduledModels.add(sourceModelId);
    return true;
  }

  /** Resolve an existing mapping or the prepared GUID for a not-yet-inserted scope element. */
  public getLogicalScope(sourceScopeId: Id64String): string | undefined {
    const mappedId = this._cloneContext.findTargetElementId(sourceScopeId);
    if (Id64.isValidId64(mappedId)) return mappedId;
    return this._elementPlans.get(sourceScopeId)?.identity.federationGuid;
  }

  public scheduleModeledPartitionDeletion(sourceId: Id64String): void {
    this._modeledPartitionDeletes.add(sourceId);
  }

  public recordAspectCleanup(
    excludedClassFullNames: ReadonlySet<string>,
    sourceIds: ReadonlySet<Id64String>
  ): void {
    if (sourceIds.size === 0) return;
    this._events.push({
      kind: "aspect-cleanup",
      excludedClassFullNames: [...excludedClassFullNames],
      sourceIds: [...sourceIds],
    });
  }

  public recordReferenceRepair(
    kind: "repair-elements" | "repair-aspects"
  ): void {
    this._events.push({ kind });
  }

  /** Mark the ordinary importer insertion currently being performed. */
  public beginElementInsert(plan: ElementReservationElementPlan): void {
    if (this._currentInsertPlan !== undefined)
      throw new Error("Nested reserved element insertion is not supported.");
    this._currentInsertPlan = plan;
    this._insertWasObserved = false;
  }

  public endElementInsert(): boolean {
    const observed = this._insertWasObserved;
    this._currentInsertPlan = undefined;
    this._insertWasObserved = false;
    return observed;
  }

  /** Called by IModelImporter immediately before its native insertElement call. */
  public guardElementInsertion(elementProps: ElementProps): void {
    if (this._phase === "collecting" || this._phase === "aborted") return;
    const plan = this._currentInsertPlan;
    if (plan === undefined || Id64.isValidId64(plan.targetElementId)) {
      this.throwPlanChanged(
        plan?.sourceId ?? elementProps.id ?? Id64.invalid,
        "an unplanned element insert reached the importer"
      );
    }
    if (this._insertWasObserved)
      this.throwPlanChanged(
        plan.sourceId,
        "the importer attempted more than one ordinary insert for a planned element"
      );
    this._insertWasObserved = true;
    if (elementProps.id !== plan.insertId)
      this.throwPlanChanged(
        plan.sourceId,
        "the insert did not preserve its planned ID-allocation policy"
      );
    const expected = plan.identity;
    const actualCode = elementProps.code;
    if (
      elementProps.classFullName !== expected.classFullName ||
      elementProps.federationGuid !== expected.federationGuid ||
      actualCode.spec !== expected.code.spec ||
      actualCode.scope !== expected.code.scope ||
      (actualCode.value ?? "") !== (expected.code.value ?? "")
    ) {
      ITwinError.throwError({
        iTwinErrorId: {
          scope: IModelTransformerErrorScope,
          key: IModelTransformerError.ElementReservationPlanChanged,
        },
        message: `Reservation identity changed at the importer insertion boundary for ${plan.sourceId}.`,
      });
    }
  }

  private throwPlanChanged(sourceId: Id64String, reason: string): never {
    ITwinError.throwError({
      iTwinErrorId: {
        scope: IModelTransformerErrorScope,
        key: IModelTransformerError.ElementReservationPlanChanged,
      },
      message: `Reservation plan changed for ${sourceId}: ${reason}.`,
    });
  }

  /** Collect a complete exporter callback and replay the journal once reservation succeeds. */
  public async flush(exportElements?: () => Promise<void>): Promise<void> {
    if (this._flushed) {
      if (exportElements !== undefined) await exportElements();
      return;
    }

    this._phase = "collecting";
    this._exportContext.changes =
      this._exporter.sourceDbChanges ?? this._exportContext.changes;
    try {
      if (exportElements !== undefined) await exportElements();
      this._exportContext.changes =
        this._exporter.sourceDbChanges ?? this._exportContext.changes;
      // Model deletion can schedule a partition only during execution. Ensure
      // an element-delete boundary exists even if discovery had no element deletes.
      let lastModelDelete = -1;
      this._events.forEach((event, index) => {
        if (event.kind === "delete-model") lastModelDelete = index;
      });
      if (
        lastModelDelete >= 0 &&
        !this._events.some(
          (event, index) =>
            index > lastModelDelete && event.kind === "delete-elements"
        )
      )
        this._events.splice(lastModelDelete + 1, 0, {
          kind: "delete-elements",
          sourceIds: [],
        });
      await this.reservePreparedElements();
      this._phase = "replaying";
      this._exportContext.replaying = true;
      await this._callbacks.persistProvenance();

      this.restoreDelegate();
      setElementReservationInsertionGuard(this._importer, (elementProps) =>
        this.guardElementInsertion(elementProps)
      );
      for (const event of this._events) await this.replayEvent(event);
      this._phase = "done";
      this._flushed = true;
    } catch (error) {
      if (this._phase === "collecting") this._callbacks.discardPreparation();
      throw error;
    } finally {
      setReservationExportContext(this._exporter, undefined);
      this._exportContext.changes = undefined;
      this._exportContext.replaying = false;
    }
  }

  public abort(): void {
    if (this._flushed) return;
    this.release();
    this._phase = "aborted";
    setReservationExportContext(this._exporter, undefined);
    this._exportContext.changes = undefined;
    this._modeledPartitionDeletes.clear();
    this._callbacks.discardPreparation();
    this._events.length = 0;
    this._elementPlans.clear();
    this._seenElements.clear();
    this._visitingElements.clear();
    this._scheduledModels.clear();
    this._journaledModels.clear();
    this._checkedRequiredDependencies.clear();
  }

  private async reservePreparedElements(): Promise<void> {
    const reservationElements: Array<{
      federationGuid: string;
      classFullName: string;
      code: CodeProps;
    }> = [];
    const federationGuids = new Map<string, Id64String>();
    const codes = new Map<string, Id64String>();

    for (const event of this._events) {
      if (event.kind !== "element") continue;
      const plan = event.plan;
      if (Id64.isValidId64(plan.targetElementId)) continue;

      const codeValue = plan.identity.code.value;
      if (codeValue !== undefined && codeValue !== "") {
        const codeKey = [
          plan.identity.code.spec,
          plan.identity.code.scope,
          codeValue,
        ].join("\u0000");
        const previousCodeSourceId = codes.get(codeKey);
        if (
          previousCodeSourceId !== undefined &&
          previousCodeSourceId !== plan.sourceId
        ) {
          this._callbacks.rejectUnsupported(
            plan.sourceId,
            `its pending code identity duplicates source element ${previousCodeSourceId}`
          );
        }
        codes.set(codeKey, plan.sourceId);
      }

      const federationGuid = plan.identity.federationGuid;
      if (federationGuid === undefined) continue;
      const guidKey = federationGuid.toLowerCase();
      const previousGuidSourceId = federationGuids.get(guidKey);
      if (
        previousGuidSourceId !== undefined &&
        previousGuidSourceId !== plan.sourceId
      ) {
        this._callbacks.rejectUnsupported(
          plan.sourceId,
          `its pending federation GUID duplicates source element ${previousGuidSourceId}`
        );
      }
      federationGuids.set(guidKey, plan.sourceId);
      if (plan.autoInsertedForCategory !== undefined) continue;
      reservationElements.push({
        federationGuid,
        classFullName: plan.identity.classFullName,
        code: { ...plan.identity.code },
      });
    }

    if (reservationElements.length === 0) return;
    try {
      await this._targetDb.reservations.reserveElements({
        elements: reservationElements,
      });
    } catch (error) {
      Logger.logError(loggerCategory, error, {
        phase: "reserve-elements",
        batchSize: reservationElements.length,
        error,
      });
      throw error;
    }
  }

  private async replayEvent(event: ElementReservationEvent): Promise<void> {
    switch (event.kind) {
      case "element": {
        const sourceElement = this._sourceDb.elements.getElement({
          id: event.sourceId,
          wantGeometry: this._exporter.wantGeometry,
          wantBRepData: this._exporter.wantGeometry,
        });
        await this._delegate.onExportElement(sourceElement, event.isUpdate);
        return;
      }
      case "model":
        await this._delegate.onExportModel(
          this._sourceDb.models.getModel(event.sourceId),
          event.isUpdate
        );
        return;
      case "delete-model":
        await this._delegate.onDeleteModel(event.sourceId);
        return;
      case "delete-elements": {
        const sourceIds = new Set([
          ...event.sourceIds,
          ...this._modeledPartitionDeletes,
        ]);
        this._modeledPartitionDeletes.clear();
        if (sourceIds.size > 0)
          await this._delegate.onDeleteElements(sourceIds);
        return;
      }
      case "aspect-cleanup":
        await this._callbacks.replayAspectCleanup(
          new Set(event.excludedClassFullNames),
          new Set(event.sourceIds)
        );
        return;
      case "unique-aspect": {
        const aspect = this._sourceDb.elements.getAspect(event.sourceId);
        if (!(aspect instanceof ElementUniqueAspect))
          throw new Error(`Source aspect ${event.sourceId} is not unique.`);
        await this._delegate.onExportElementUniqueAspect(
          aspect,
          event.isUpdate
        );
        return;
      }
      case "multi-aspects": {
        const aspects = event.sourceIds.map((sourceId) => {
          const aspect = this._sourceDb.elements.getAspect(sourceId);
          if (!(aspect instanceof ElementMultiAspect))
            throw new Error(`Source aspect ${sourceId} is not multi-valued.`);
          return aspect;
        });
        await this._delegate.onExportElementMultiAspects(aspects);
        return;
      }
      case "relationship": {
        const relationship = this._sourceDb.relationships.getInstance(
          event.classFullName,
          event.sourceId
        );
        await this._delegate.onExportRelationship(
          relationship,
          event.isUpdate,
          event.sourceFedGuid,
          event.targetFedGuid
        );
        return;
      }
      case "delete-relationship":
        await this._delegate.onDeleteRelationship(event.sourceId);
        return;
      case "skip-element":
        await this._delegate.onSkipElement(event.sourceId);
        return;
      case "repair-elements":
      case "repair-aspects":
        await this._callbacks.repairReferences(event.kind);
        return;
      case "progress":
        await this._delegate.onProgress();
        return;
    }
  }

  // ── Selection and setup callbacks are delegated. Mutation callbacks are journaled. ──

  public override async shouldExportCodeSpec(
    codeSpec: Parameters<IModelExportHandler["shouldExportCodeSpec"]>[0]
  ): Promise<boolean> {
    return this._delegate.shouldExportCodeSpec(codeSpec);
  }

  public override async onExportCodeSpec(
    codeSpec: Parameters<IModelExportHandler["onExportCodeSpec"]>[0],
    isUpdate: boolean | undefined
  ): Promise<void> {
    return this._delegate.onExportCodeSpec(codeSpec, isUpdate);
  }

  public override async onExportFont(
    font: Parameters<IModelExportHandler["onExportFont"]>[0],
    isUpdate: boolean | undefined
  ): Promise<void> {
    return this._delegate.onExportFont(font, isUpdate);
  }

  public override async shouldExportElement(
    sourceElement: Element
  ): Promise<boolean> {
    return this._delegate.shouldExportElement(sourceElement);
  }

  public override async onSkipElement(sourceId: Id64String): Promise<void> {
    this._events.push({ kind: "skip-element", sourceId });
  }

  public override async preExportElement(
    sourceElement: Element
  ): Promise<void> {
    return this._delegate.preExportElement(sourceElement);
  }

  /** Classify each source element once, even when dependency traversal revisits it. */
  public override async onExportElement(
    sourceElement: Element,
    isUpdate?: boolean
  ): Promise<void> {
    if (this._seenElements.has(sourceElement.id)) return;
    if (this._visitingElements.has(sourceElement.id)) return;

    this._visitingElements.add(sourceElement.id);
    try {
      const plan = await this._callbacks.collectElement(sourceElement);
      this._seenElements.add(sourceElement.id);
      if (plan === undefined) return;
      this._elementPlans.set(sourceElement.id, plan);
      this._events.push({
        kind: "element",
        sourceId: sourceElement.id,
        isUpdate,
        plan,
      });
    } finally {
      this._visitingElements.delete(sourceElement.id);
    }
  }

  /** Journal a model after ensuring its modeled element has a logical identity. */
  public override async onExportModel(
    sourceModel: Model,
    isUpdate?: boolean
  ): Promise<void> {
    if (this._journaledModels.has(sourceModel.id)) return;
    this._scheduledModels.add(sourceModel.id);
    if (
      sourceModel.id !== IModel.repositoryModelId &&
      !this.hasElementIdentity(sourceModel.id)
    ) {
      await this._exporter.exportElement(sourceModel.id);
    }
    if (
      sourceModel.id !== IModel.repositoryModelId &&
      !this.hasElementIdentity(sourceModel.id)
    ) {
      this._callbacks.rejectUnsupported(
        sourceModel.id,
        "the model's modeled element was not accepted by the exporter"
      );
    }
    this._journaledModels.add(sourceModel.id);
    this._events.push({
      kind: "model",
      sourceId: sourceModel.id,
      isUpdate,
    });
  }

  public override async onDeleteModel(
    sourceModelId: Id64String
  ): Promise<void> {
    this._events.push({ kind: "delete-model", sourceId: sourceModelId });
  }

  public override async onDeleteElements(
    sourceElementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    if (sourceElementIds.size === 0) return;
    this._events.push({
      kind: "delete-elements",
      sourceIds: [...sourceElementIds],
    });
  }

  public override async shouldExportElementAspect(
    aspect: ElementAspect
  ): Promise<boolean> {
    return this._delegate.shouldExportElementAspect(aspect);
  }

  public override async onExportElementUniqueAspect(
    aspect: ElementUniqueAspect,
    isUpdate?: boolean
  ): Promise<void> {
    this._events.push({
      kind: "unique-aspect",
      sourceId: aspect.id,
      isUpdate,
    });
  }

  public override async onExportElementMultiAspects(
    aspects: ElementMultiAspect[]
  ): Promise<void> {
    if (aspects.length === 0) return;
    this._events.push({
      kind: "multi-aspects",
      sourceIds: aspects.map((aspect) => aspect.id),
    });
  }

  public override async shouldExportRelationship(
    relationship: Relationship
  ): Promise<boolean> {
    return this._delegate.shouldExportRelationship(relationship);
  }

  public override async onExportRelationship(
    relationship: Relationship,
    isUpdate?: boolean,
    sourceFedGuid?: string,
    targetFedGuid?: string
  ): Promise<void> {
    this._events.push({
      kind: "relationship",
      sourceId: relationship.id,
      classFullName: relationship.classFullName,
      isUpdate,
      sourceFedGuid,
      targetFedGuid,
    });
  }

  public override async onDeleteRelationship(
    sourceRelationshipId: Id64String
  ): Promise<void> {
    this._events.push({
      kind: "delete-relationship",
      sourceId: sourceRelationshipId,
    });
  }

  public override async onProgress(): Promise<void> {
    this._events.push({ kind: "progress" });
  }

  public override async shouldExportSchema(
    schemaKey: Parameters<IModelExportHandler["shouldExportSchema"]>[0]
  ): Promise<boolean> {
    return this._delegate.shouldExportSchema(schemaKey);
  }

  public override async onExportSchema(
    schema: Parameters<IModelExportHandler["onExportSchema"]>[0]
  ): Promise<void | ExportSchemaResult> {
    return this._delegate.onExportSchema(schema);
  }
}
