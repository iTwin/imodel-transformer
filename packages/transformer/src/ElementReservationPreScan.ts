/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import { GuidString, Id64, Id64String } from "@itwin/core-bentley";
import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  SubCategory,
} from "@itwin/core-backend";
import {
  Code,
  CodeProps,
  ElementProps,
  IModel,
  RelatedElement,
} from "@itwin/core-common";
import { IModelExporter, IModelExportHandler } from "./IModelExporter.js";
import type { IModelCloneContext } from "./IModelCloneContext.js";

/** How the element reservation pre-scan determines the identity of a new target element.
 * - `"transform"` calls [[IModelTransformer.onTransformElement]], without geometry, when the element's target model and
 *   category are already mapped, and falls back to `"identity"` otherwise, because the native clone requires a mapped
 *   target model and imports an unmapped category into the target.
 * - `"identity"` derives the class, federation GUID, and code from the source element without cloning it.
 * @internal
 */
export type ElementReservationPreScanMode = "transform" | "identity";

/** One identity to pass to `SynchronousChannel.Reservations.reserveElements`.
 * @internal
 */
export interface PlannedElementReservation {
  federationGuid: GuidString;
  classFullName: string;
  code: CodeProps;
}

/** Counters and timings produced by one element reservation pre-scan.
 * @internal
 */
export interface ElementReservationPreScanStats {
  mode: ElementReservationPreScanMode;
  /** Elements that reached classification. */
  classified: number;
  /** Elements the change filter treated as unchanged. */
  unchanged: number;
  /** Elements already mapped, for example by provenance. */
  matchedByMapping: number;
  matchedByFederationGuid: number;
  matchedByCode: number;
  /** New target elements with a federation GUID, which need a reservation. */
  insertsWithFederationGuid: number;
  /** New target elements without a federation GUID, which cannot be reserved. */
  insertsWithoutFederationGuid: number;
  /** Default SubCategories of new Categories, which the backend creates with the Category. */
  defaultSubCategories: number;
  /** Inserts reserved with a federation-GUID code scope because their scope is new in this run. */
  newScopeCodes: number;
  /** Inserts whose code scope is a new element without a federation GUID. Their reserved code cannot match the insert. */
  unreservableScopes: number;
  /** Elements classified with [[IModelTransformer.onTransformElement]]. */
  transformed: number;
  /** Elements in `"transform"` mode whose target model was not mapped, so their identity came from the source. */
  identityFallbacks: number;
  /** Whether the scan loaded only element identity properties instead of full elements. */
  identityLoads: boolean;
  /** Required references the scan visited ahead of the traversal, as the transformer does. */
  requiredReferenceVisits: number;
  scanMs: number;
  reserveMs: number;
  /** Whether the reservations were sent to the target's reservation service. */
  submitted: boolean;
}

/** Transformer state used by the pre-scan.
 * @internal
 */
export interface ElementReservationPreScanHost {
  readonly sourceDb: IModelDb;
  readonly targetDb: IModelDb;
  readonly exporter: IModelExporter;
  readonly context: IModelCloneContext;
  shouldExportElement(sourceElement: Element): Promise<boolean>;
  hasElementChanged(sourceElement: Element): boolean;
  onTransformElement(sourceElement: Element): Promise<ElementProps>;
  queryElementIdByCode(code: Required<CodeProps>): Id64String | undefined;
  /** Whether the export filters and change check read only an element's id, class, model, parent, code, federation GUID,
   * and category, so the `"identity"` scan can skip full element loads.
   */
  readonly canLoadIdentityOnly: boolean;
}

/** The planned target of a source element visited by the scan. `targetId` is set for matches. */
interface PlannedElement {
  targetId?: Id64String;
  federationGuid?: GuidString;
}

/** The class, federation GUID, and code the transformation will insert for a source element. */
interface TargetIdentity {
  classFullName: string;
  federationGuid?: GuidString;
  code: CodeProps;
  /** Whether the code can identify an existing target element. */
  lookUpCode: boolean;
  /** Whether the code scope is a new element of this run, written in federation GUID form. */
  hasNewScope: boolean;
}

/** Visits the elements a transformation would export and classifies each as a match or a new target element,
 * without writing elements to the target.
 * @note The scan may import a CodeSpec into the target, which the transformation itself would also do.
 * @internal
 */
export class ElementReservationPreScan extends IModelExportHandler {
  public readonly reservations: PlannedElementReservation[] = [];
  /** Codes, keyed by federation GUID, whose scope is a new element of this run and is written in federation GUID form. */
  public readonly newScopeCodes = new Map<GuidString, CodeProps>();
  public readonly stats: ElementReservationPreScanStats;
  private readonly _planned = new Map<Id64String, PlannedElement>();
  private readonly _classNames = new Map<Id64String, string>();
  /** Classes whose constructors need more than identity properties. */
  private readonly _fullLoadClasses = new Set<string>();

  public constructor(
    private readonly _host: ElementReservationPreScanHost,
    mode: ElementReservationPreScanMode
  ) {
    super();
    this.stats = {
      mode,
      classified: 0,
      unchanged: 0,
      matchedByMapping: 0,
      matchedByFederationGuid: 0,
      matchedByCode: 0,
      insertsWithFederationGuid: 0,
      insertsWithoutFederationGuid: 0,
      defaultSubCategories: 0,
      newScopeCodes: 0,
      unreservableScopes: 0,
      transformed: 0,
      identityFallbacks: 0,
      identityLoads: mode === "identity" && _host.canLoadIdentityOnly,
      requiredReferenceVisits: 0,
      scanMs: 0,
      reserveMs: 0,
      submitted: false,
    };
  }

  /** Runs the scan over the selection that the transformation will export. */
  public async run(options: {
    skipPropagateChangesToRootElements: boolean;
  }): Promise<void> {
    const start = performance.now();
    await this._host.exporter.scanElements(this, {
      skipPropagateChangesToRootElements:
        options.skipPropagateChangesToRootElements,
      // Identity never depends on geometry, and cloning a geometry stream imports unmapped GeometryParts and SubCategories into the target.
      wantGeometry: false,
      loadElement: this.stats.identityLoads
        ? (elementId) => this.loadIdentity(elementId)
        : undefined,
    });
    this.stats.scanMs = performance.now() - start;
  }

  /** Loads an element with only the properties the `"identity"` scan reads, which avoids reading its other properties. */
  private loadIdentity(elementId: Id64String): Element {
    const props = this._host.sourceDb.withPreparedSqliteStatement(
      `SELECT e.ECClassId, e.ModelId, e.ParentId, e.ParentRelECClassId, e.FederationGuid,
              e.CodeSpecId, e.CodeScopeId, e.CodeValue, COALESCE(g3.CategoryId, g2.CategoryId)
       FROM bis_Element e
       LEFT JOIN bis_GeometricElement3d g3 ON g3.ElementId = e.Id
       LEFT JOIN bis_GeometricElement2d g2 ON g2.ElementId = e.Id
       WHERE e.Id = ?`,
      (stmt): (ElementProps & { category?: Id64String }) | undefined => {
        stmt.bindId(1, elementId);
        if (!stmt.nextRow()) return undefined;
        return {
          id: elementId,
          classFullName: this.getClassFullName(stmt.getValueId(0)),
          model: stmt.getValueId(1),
          parent: stmt.isValueNull(2)
            ? undefined
            : {
                id: stmt.getValueId(2),
                relClassName: this.getClassFullName(stmt.getValueId(3)),
              },
          federationGuid: stmt.isValueNull(4)
            ? undefined
            : stmt.getValueGuid(4),
          code: {
            spec: stmt.getValueId(5),
            scope: stmt.getValueId(6),
            value: stmt.isValueNull(7) ? "" : stmt.getValueString(7),
          },
          category: stmt.isValueNull(8) ? undefined : stmt.getValueId(8),
        };
      }
    );
    // A missing element fails as a full load does.
    if (
      props !== undefined &&
      !this._fullLoadClasses.has(props.classFullName)
    ) {
      try {
        return this._host.sourceDb.constructEntity<Element>(props);
      } catch {
        // Some classes, such as ViewDefinitions, validate other properties when constructed.
        this._fullLoadClasses.add(props.classFullName);
      }
    }
    return this._host.sourceDb.elements.getElement({
      id: elementId,
      wantGeometry: false,
    });
  }

  private getClassFullName(classId: Id64String): string {
    let name = this._classNames.get(classId);
    if (name === undefined) {
      name = this._host.sourceDb.withPreparedSqliteStatement(
        "SELECT s.Name, c.Name FROM ec_Class c JOIN ec_Schema s ON s.Id = c.SchemaId WHERE c.Id = ?",
        (stmt) => {
          stmt.bindId(1, classId);
          if (!stmt.nextRow())
            throw new Error(`ECClass ${classId} was not found.`);
          return `${stmt.getValueString(0)}:${stmt.getValueString(1)}`;
        }
      );
      this._classNames.set(classId, name);
    }
    return name;
  }

  public override async shouldExportElement(
    sourceElement: Element
  ): Promise<boolean> {
    return this._host.shouldExportElement(sourceElement);
  }

  /** Mirrors [[IModelTransformer.preExportElement]]: changed required references are visited before the element. */
  public override async preExportElement(
    sourceElement: Element
  ): Promise<void> {
    if (!this._host.hasElementChanged(sourceElement)) return;
    const elemClass = sourceElement.constructor as typeof Element;
    const changes = this._host.exporter.sourceDbChanges;
    for (const referenceKey of elemClass.requiredReferenceKeys) {
      const container = sourceElement.asAny[referenceKey] as
        Id64String | RelatedElement | undefined;
      const referenceId =
        typeof container === "string" ? container : container?.id;
      if (
        referenceId === undefined ||
        !Id64.isValidId64(referenceId) ||
        referenceId === IModel.rootSubjectId ||
        this.isPlannedOrMapped(referenceId)
      )
        continue;
      const isSubModeled =
        this._host.sourceDb.models.tryGetModelProps(referenceId) !== undefined;
      const referenceChanged =
        changes === undefined ||
        changes.element.insertIds.has(referenceId) ||
        changes.element.updateIds.has(referenceId);
      this.stats.requiredReferenceVisits++;
      if (referenceChanged) {
        await this._host.exporter.exportElement(referenceId);
      } else {
        // The transformation only looks up an unchanged reference in the target; it never inserts it.
        const wantGeometry = this._host.exporter.wantGeometry;
        const reference = this.stats.identityLoads
          ? this.loadIdentity(referenceId)
          : this._host.sourceDb.elements.getElement({
              id: referenceId,
              wantGeometry,
              wantBRepData: wantGeometry,
            });
        const accepted =
          this._host.exporter.getUnchangedAncestorFilterResult(referenceId) ??
          (await this._host.exporter.shouldExportElement(reference));
        if (accepted) {
          const match = await this.match(reference);
          if (match.targetId !== undefined)
            this._planned.set(referenceId, { targetId: match.targetId });
        }
      }
      if (isSubModeled) await this._host.exporter.exportModel(referenceId);
    }
  }

  public override async onExportElement(sourceElement: Element): Promise<void> {
    if (this._planned.has(sourceElement.id)) return;
    if (!this._host.hasElementChanged(sourceElement)) {
      this.stats.unchanged++;
      return;
    }
    this.stats.classified++;

    const match = await this.match(sourceElement);
    if (match.targetId !== undefined) {
      this._planned.set(sourceElement.id, { targetId: match.targetId });
      return;
    }
    const identity = match.identity;
    this._planned.set(sourceElement.id, {
      federationGuid: identity.federationGuid,
    });
    if (this.isDefaultSubCategoryOfNewCategory(sourceElement)) {
      this.stats.defaultSubCategories++;
      return;
    }
    if (identity.federationGuid === undefined) {
      this.stats.insertsWithoutFederationGuid++;
      return;
    }
    this.stats.insertsWithFederationGuid++;
    this.reservations.push({
      federationGuid: identity.federationGuid,
      classFullName: identity.classFullName,
      code: identity.code,
    });
    if (identity.hasNewScope) {
      this.stats.newScopeCodes++;
      this.newScopeCodes.set(identity.federationGuid, identity.code);
    }
  }

  /** Finds the target element the transformation will update, in the order the transformation does: mapping, federation GUID, then code.
   * @returns the matching target element, or the identity of the element the transformation will insert.
   */
  private async match(
    sourceElement: Element
  ): Promise<
    | { targetId: Id64String; identity?: undefined }
    | { targetId?: undefined; identity: TargetIdentity }
  > {
    const mappedId = this._host.context.findTargetElementId(sourceElement.id);
    if (Id64.isValidId64(mappedId)) {
      this.stats.matchedByMapping++;
      return { targetId: mappedId };
    }

    if (sourceElement.federationGuid !== undefined) {
      const targetId = this._host.targetDb.elements.getIdFromFederationGuid(
        sourceElement.federationGuid
      );
      if (targetId !== undefined && Id64.isValidId64(targetId)) {
        this.stats.matchedByFederationGuid++;
        return { targetId };
      }
    }

    const identity = await this.getTargetIdentity(sourceElement);
    if (identity.lookUpCode) {
      const targetId = this._host.queryElementIdByCode(
        identity.code as Required<CodeProps>
      );
      if (targetId !== undefined) {
        const targetClass =
          this._host.targetDb.elements.getElementProps(targetId).classFullName;
        if (targetClass === identity.classFullName) {
          this.stats.matchedByCode++;
          return { targetId };
        }
        // The transformation clears a code that matches an element of another class.
        identity.code = Code.createEmpty().toJSON();
        identity.hasNewScope = false;
      }
    }
    return { identity };
  }

  private isPlannedOrMapped(sourceId: Id64String): boolean {
    return (
      this._planned.has(sourceId) ||
      Id64.isValidId64(this._host.context.findTargetElementId(sourceId))
    );
  }

  /** The backend inserts a Category's default SubCategory with the Category, so the transformation updates it. */
  private isDefaultSubCategoryOfNewCategory(sourceElement: Element): boolean {
    if (!(sourceElement instanceof SubCategory)) return false;
    const categoryId = sourceElement.parent?.id;
    if (
      categoryId === undefined ||
      sourceElement.id !== IModelDb.getDefaultSubCategoryId(categoryId)
    )
      return false;
    const category = this._planned.get(categoryId);
    return category !== undefined && category.targetId === undefined;
  }

  /** The native clone requires a mapped target model, and it imports an unmapped Category into the target,
   * which would bypass the export filter before the transformation runs.
   */
  private canClone(sourceElement: Element): boolean {
    const isMapped = (id: Id64String) =>
      Id64.isValidId64(this._host.context.findTargetElementId(id));
    const category =
      (sourceElement.asAny.category as Id64String | undefined) ?? undefined;
    return (
      isMapped(sourceElement.model) &&
      (category === undefined ||
        !Id64.isValidId64(category) ||
        isMapped(category))
    );
  }

  /** Returns the class, federation GUID, and code that the transformation will insert. */
  private async getTargetIdentity(
    sourceElement: Element
  ): Promise<TargetIdentity> {
    const sourceCode = sourceElement.code;
    const scope = this.getTargetCodeScope(sourceCode.scope);

    let props: ElementProps | undefined;
    if (this.stats.mode === "transform") {
      if (this.canClone(sourceElement)) {
        props = await this._host.onTransformElement(sourceElement);
        this.stats.transformed++;
      } else {
        this.stats.identityFallbacks++;
      }
    }
    const classFullName =
      props?.classFullName ??
      this._host.context.findTargetElementClass(sourceElement.classFullName);
    const federationGuid = props
      ? props.federationGuid
      : sourceElement.federationGuid;

    if (scope.kind === "unresolved" || scope.kind === "newElementWithoutGuid") {
      if (scope.kind === "newElementWithoutGuid")
        this.stats.unreservableScopes++;
      return {
        classFullName,
        federationGuid,
        code: Code.createEmpty().toJSON(),
        lookUpCode: false,
        hasNewScope: false,
      };
    }

    let code: CodeProps;
    // The scan does not record mappings, so a clone only sees scopes that were mapped before the scan.
    if (props !== undefined && scope.kind === "mapped") {
      code = { ...props.code, value: props.code.value ?? "" };
    } else {
      code = {
        spec: this.getTargetCodeSpecId(sourceCode.spec),
        scope: scope.value,
        value: sourceCode.value ?? "",
      };
    }
    const isTargetScope = scope.kind === "mapped" || scope.kind === "matched";
    return {
      classFullName,
      federationGuid,
      code,
      lookUpCode:
        isTargetScope && Id64.isValidId64(code.scope) && !Code.isEmpty(code),
      hasNewScope: scope.kind === "newElement",
    };
  }

  /** Resolves the target code scope the transformation will use, in the order the traversal reaches elements.
   * A scope inserted earlier in this run is written in federation GUID form, because it has no target ID until it is inserted.
   * A scope the traversal has not reached yet is unmapped at insert time, so the transformation inserts an empty code
   * and completes it after the traversal.
   */
  private getTargetCodeScope(
    sourceScopeId: Id64String
  ):
    | { kind: "mapped" | "matched"; value: Id64String }
    | { kind: "newElement"; value: GuidString }
    | { kind: "newElementWithoutGuid" }
    | { kind: "unresolved" } {
    const mappedId = this._host.context.findTargetElementId(sourceScopeId);
    if (Id64.isValidId64(mappedId)) return { kind: "mapped", value: mappedId };
    const planned = this._planned.get(sourceScopeId);
    if (planned === undefined) return { kind: "unresolved" };
    if (planned.targetId !== undefined)
      return { kind: "matched", value: planned.targetId };
    if (planned.federationGuid !== undefined)
      return { kind: "newElement", value: planned.federationGuid };
    // The scope gets an unreserved ID at insert, which the reservation cannot name.
    return { kind: "newElementWithoutGuid" };
  }

  private getTargetCodeSpecId(sourceCodeSpecId: Id64String): Id64String {
    const mappedId = this._host.context.findTargetCodeSpecId(sourceCodeSpecId);
    if (Id64.isValidId64(mappedId)) return mappedId;
    // The native clone imports a missing CodeSpec in the same way.
    this._host.context.importCodeSpec(sourceCodeSpecId);
    return this._host.context.findTargetCodeSpecId(sourceCodeSpecId);
  }
}
