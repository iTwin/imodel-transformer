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
  /** Elements that reached classification. */
  classified: number;
  /** Elements the change filter treated as unchanged. */
  unchanged: number;
  /** Elements already mapped, for example by provenance. */
  matchedByMapping: number;
  matchedByFederationGuid: number;
  matchedByCode: number;
  /** Elements whose code matches an element that the transformation inserts earlier in the same run. */
  matchedByPlannedCode: number;
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
  queryElementIdByCode(code: Required<CodeProps>): Id64String | undefined;
  /** Whether the export filters and change check read only an element's id, class, model, parent, code, federation GUID,
   * and category, so the scan can skip full element loads.
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
 * without writing elements to the target. The identity of a new element comes from the source element; transform hooks
 * do not run, so they must not change an element's class, federation GUID, or code.
 * @note The scan may import a CodeSpec into the target, which the transformation itself would also do.
 * @internal
 */
export class ElementReservationPreScan extends IModelExportHandler {
  public readonly reservations: PlannedElementReservation[] = [];
  /** Code scopes in federation GUID form, keyed by the federation GUID of the reserved element whose code they scope.
   * Each scope is a new element of this run.
   */
  public readonly newCodeScopes = new Map<GuidString, GuidString>();
  public readonly stats: ElementReservationPreScanStats;
  private readonly _planned = new Map<Id64String, PlannedElement>();
  /** Codes of elements the transformation will insert, keyed by [[getPlannedCodeKey]], with the first such insert. */
  private readonly _plannedCodes = new Map<
    string,
    { classFullName: string; planned: PlannedElement }
  >();
  private readonly _classNames = new Map<Id64String, string>();
  /** Classes whose constructors need more than identity properties. */
  private readonly _fullLoadClasses = new Set<string>();

  public constructor(private readonly _host: ElementReservationPreScanHost) {
    super();
    this.stats = {
      classified: 0,
      unchanged: 0,
      matchedByMapping: 0,
      matchedByFederationGuid: 0,
      matchedByCode: 0,
      matchedByPlannedCode: 0,
      insertsWithFederationGuid: 0,
      insertsWithoutFederationGuid: 0,
      defaultSubCategories: 0,
      newScopeCodes: 0,
      unreservableScopes: 0,
      identityLoads: _host.canLoadIdentityOnly,
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
      // Custom filters get the same elements as in the transformation. Otherwise nothing the scan reads depends on geometry.
      wantGeometry: this._host.canLoadIdentityOnly
        ? false
        : this._host.exporter.wantGeometry,
      loadElement: this.stats.identityLoads
        ? (elementId) => this.loadIdentity(elementId)
        : undefined,
    });
    this.stats.scanMs = performance.now() - start;
  }

  /** Loads an element with only the properties the scan reads, which avoids reading its other properties. */
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

  /** Mirrors the transformer's required-reference import at the start of [[IModelTransformer.onExportElement]]: changed required references are visited before the element. */
  private async visitRequiredReferences(sourceElement: Element): Promise<void> {
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
          if (match.planned !== undefined)
            this._planned.set(referenceId, match.planned);
        }
      }
      if (isSubModeled) await this._host.exporter.exportModel(referenceId);
    }
  }

  public override async onExportElement(sourceElement: Element): Promise<void> {
    await this.visitRequiredReferences(sourceElement);
    if (this._planned.has(sourceElement.id)) return;
    if (!this._host.hasElementChanged(sourceElement)) {
      this.stats.unchanged++;
      return;
    }
    this.stats.classified++;

    const match = await this.match(sourceElement);
    if (match.planned !== undefined) {
      this._planned.set(sourceElement.id, match.planned);
      return;
    }
    const identity = match.identity;
    const planned: PlannedElement = { federationGuid: identity.federationGuid };
    this._planned.set(sourceElement.id, planned);
    const codeKey = this.getPlannedCodeKey(identity);
    if (codeKey !== undefined && !this._plannedCodes.has(codeKey))
      this._plannedCodes.set(codeKey, {
        classFullName: identity.classFullName,
        planned,
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
      this.newCodeScopes.set(identity.federationGuid, identity.code.scope);
    }
  }

  /** Finds the target element the transformation will update, in the order the transformation does: mapping, federation GUID, then code.
   * The code lookup also finds an element this run inserts earlier, as the transformation's lookup in the target does.
   * @returns the planned match, or the identity of the element the transformation will insert.
   */
  private async match(
    sourceElement: Element
  ): Promise<
    | { planned: PlannedElement; identity?: undefined }
    | { planned?: undefined; identity: TargetIdentity }
  > {
    const mappedId = this._host.context.findTargetElementId(sourceElement.id);
    if (Id64.isValidId64(mappedId)) {
      this.stats.matchedByMapping++;
      return { planned: { targetId: mappedId } };
    }

    if (sourceElement.federationGuid !== undefined) {
      const targetId = this._host.targetDb.elements.getIdFromFederationGuid(
        sourceElement.federationGuid
      );
      if (targetId !== undefined && Id64.isValidId64(targetId)) {
        this.stats.matchedByFederationGuid++;
        return { planned: { targetId } };
      }
    }

    const identity = this.getTargetIdentity(sourceElement);
    let codeMatch:
      { classFullName: string; planned: PlannedElement } | undefined;
    if (identity.lookUpCode) {
      const targetId = this._host.queryElementIdByCode(
        identity.code as Required<CodeProps>
      );
      if (targetId !== undefined)
        codeMatch = {
          classFullName:
            this._host.targetDb.elements.getElementProps(targetId)
              .classFullName,
          planned: { targetId },
        };
    }
    const codeKey = this.getPlannedCodeKey(identity);
    const plannedMatch =
      codeMatch === undefined && codeKey !== undefined
        ? this._plannedCodes.get(codeKey)
        : undefined;
    if (codeMatch !== undefined || plannedMatch !== undefined) {
      const found = codeMatch ?? plannedMatch;
      if (found?.classFullName === identity.classFullName) {
        if (codeMatch !== undefined) this.stats.matchedByCode++;
        else this.stats.matchedByPlannedCode++;
        return { planned: found.planned };
      }
      // The transformation clears a code that matches an element of another class.
      identity.code = Code.createEmpty().toJSON();
      identity.hasNewScope = false;
      identity.lookUpCode = false;
    }
    return { identity };
  }

  /** Keys a code the way the target's code index compares it: exact CodeSpec and scope, and a value that SQLite
   * `COLLATE NOCASE` compares without ASCII case. A new scope is keyed by its federation GUID.
   * @returns `undefined` for a code the transformation does not look up.
   */
  private getPlannedCodeKey(identity: TargetIdentity): string | undefined {
    if (!identity.lookUpCode && !identity.hasNewScope) return undefined;
    if (Code.isEmpty(identity.code)) return undefined;
    const value = (identity.code.value ?? "").replace(/[A-Z]/g, (c) =>
      c.toLowerCase()
    );
    return `${identity.code.spec}\0${identity.code.scope}\0${value}`;
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

  /** Returns the class, federation GUID, and code that the transformation will insert, derived from the source element
   * with the clone context's class, CodeSpec, and element remaps.
   */
  private getTargetIdentity(sourceElement: Element): TargetIdentity {
    const sourceCode = sourceElement.code;
    const scope = this.getTargetCodeScope(sourceCode.scope);
    let code: CodeProps;
    if (scope.kind === "unresolved" || scope.kind === "newElementWithoutGuid") {
      if (scope.kind === "newElementWithoutGuid")
        this.stats.unreservableScopes++;
      code = Code.createEmpty().toJSON();
    } else {
      code = {
        spec: this.getTargetCodeSpecId(sourceCode.spec),
        scope: scope.value,
        value: sourceCode.value ?? "",
      };
    }
    const hasNewScope = scope.kind === "newElement";
    return {
      classFullName: this._host.context.findTargetElementClass(
        sourceElement.classFullName
      ),
      federationGuid: sourceElement.federationGuid,
      code,
      lookUpCode:
        !hasNewScope && Id64.isValidId64(code.scope) && !Code.isEmpty(code),
      hasNewScope,
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
