/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import * as path from "path";
import { EventEmitter } from "events";
import * as Semver from "semver";
import * as nodeAssert from "assert";
import {
  AccessToken, assert, CompressedId64Set, DbResult, Guid, GuidString, Id64, Id64Array, Id64Set, Id64String, IModelStatus, Logger, MarkRequired,
  OpenMode, YieldManager,
} from "@itwin/core-bentley";
import * as ECSchemaMetaData from "@itwin/ecschema-metadata";
import { Point3d, Transform } from "@itwin/core-geometry";
import {
  ChangeSummaryManager,
  ChannelRootAspect, ConcreteEntity, DefinitionElement, DefinitionModel, DefinitionPartition, ECSchemaXmlContext, ECSqlStatement, Element, ElementAspect, ElementMultiAspect, ElementOwnsExternalSourceAspects,
  ElementRefersToElements, ElementUniqueAspect, Entity, EntityReferences, ExternalSource, ExternalSourceAspect, ExternalSourceAttachment,
  FolderLink, GeometricElement2d, GeometricElement3d, IModelDb, IModelHost, IModelJsFs, InformationPartitionElement, KnownLocations, Model,
  RecipeDefinitionElement, Relationship, RelationshipProps, Schema, SQLiteDb, Subject, SynchronizationConfigLink,
} from "@itwin/core-backend";
import {
  ChangeOpCode, ChangesetIndexAndId, Code, CodeProps, CodeSpec, ConcreteEntityTypes, ElementAspectProps, ElementProps, EntityReference, EntityReferenceSet,
  ExternalSourceAspectProps, FontProps, GeometricElement2dProps, GeometricElement3dProps, IModel, IModelError, ModelProps,
  Placement2d, Placement3d, PrimitiveTypeCode, PropertyMetaData, RelatedElement,
} from "@itwin/core-common";
import { ExportSchemaResult, IModelExporter, IModelExporterState, IModelExportHandler } from "./IModelExporter";
import { IModelImporter, IModelImporterState, OptimizeGeometryOptions } from "./IModelImporter";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { PendingReference, PendingReferenceMap } from "./PendingReferenceMap";
import { EntityMap } from "./EntityMap";
import { IModelCloneContext } from "./IModelCloneContext";
import { EntityUnifier } from "./EntityUnifier";

const loggerCategory: string = TransformerLoggerCategory.IModelTransformer;

const nullLastProvenanceEntityInfo = {
  entityId: Id64.invalid,
  aspectId: Id64.invalid,
  aspectVersion: "",
  aspectKind: ExternalSourceAspect.Kind.Element,
};

type LastProvenanceEntityInfo = typeof nullLastProvenanceEntityInfo;

type EntityTransformHandler = (entity: ConcreteEntity) => ElementProps | ModelProps | RelationshipProps | ElementAspectProps;

/** Options provided to the [[IModelTransformer]] constructor.
 * @beta
 * @note if adding an option, you must explicitly add its serialization to [[IModelTransformer.saveStateToFile]]!
 */
export interface IModelTransformOptions {
  /** The Id of the Element in the **target** iModel that represents the **source** repository as a whole and scopes its [ExternalSourceAspect]($backend) instances.
   * It is always a good idea to define this, although particularly necessary in any multi-source scenario such as multiple branches that reverse synchronize
   * or physical consolidation.
   */
  targetScopeElementId?: Id64String;

  /** Set to `true` if IModelTransformer should not record its provenance.
   * Provenance tracks a target element back to its corresponding source element and is essential for [[IModelTransformer.processChanges]] to work properly.
   * Turning off IModelTransformer provenance is really only relevant for producing snapshots or another one time transformations.
   * @note See the [[includeSourceProvenance]] option for determining whether existing source provenance is cloned into the target.
   * @note The default is `false` which means that new IModelTransformer provenance will be recorded.
   */
  noProvenance?: boolean;

  /** Set to `true` to clone existing source provenance into the target.
   * @note See the [[noProvenance]] option for determining whether new IModelTransformer provenance is recorded.
   * @note The default is `false` which means that existing provenance in the source will not be carried into the target.
   */
  includeSourceProvenance?: boolean;

  /** Flag that indicates that the target iModel was created by copying the source iModel.
   * This is common when the target iModel is intended to be a *branch* of the source iModel.
   * This *hint* is essential to properly initialize the source to target element mapping and to cause provenance to be recorded for future synchronizations.
   * @note This *hint* is typically only set for the first synchronization after the iModel was copied since every other synchronization can utilize the provenance.
   */
  wasSourceIModelCopiedToTarget?: boolean;

  /** Flag that indicates that the current source and target iModels are now synchronizing in the reverse direction from a prior synchronization.
   * The most common example is to first synchronize master to branch, make changes to the branch, and then reverse directions to synchronize from branch to master.
   * This means that the provenance on the (current) source is used instead.
   * @note This also means that only [[IModelTransformer.processChanges]] can detect deletes.
   */
  isReverseSynchronization?: boolean;

  /** Flag that indicates whether or not the transformation process needs to consider the source geometry before cloning/transforming.
   * For standard cases, it is not required to load the source GeometryStream in JavaScript since the cloning happens in native code.
   * Also, the target GeometryStream will be available in JavaScript prior to insert.
   * @note If the source geometry affects the class mapping or transformation logic, then this flag should be set to `true`. The default is `false`.
   * @see [IModelExporter.wantGeometry]($transformer)
   */
  loadSourceGeometry?: boolean;

  /** Flag that indicates whether or not the transformation process should clone using binary geometry.
   *
   * Prefer to never to set this flag. If you need geometry changes, instead override [[IModelTransformer.onTransformElement]]
   * and provide an [ElementGeometryBuilderParams]($backend) to the `elementGeometryBuilderParams`
   * property of [ElementProps]($common) instead, it is much faster. You can read geometry during the transformation by setting the
   * [[IModelTransformOptions.loadSourceGeometry]] property to `true`, and passing that to a [GeometryStreamIterator]($common)
   * @note this flag will be deprecated when `elementGeometryBuilderParams` is no longer an alpha API
   *
   * @default true
   */
  cloneUsingBinaryGeometry?: boolean;

  /** Flag that indicates that ids should be preserved while copying elements to the target
   * Intended only for pure-filter transforms, so you can keep parts of the source, while deleting others,
   * and element ids are guaranteed to be the same, (other entity ids are not, however)
   * @note The target must be empty.
   * @note It is invalid to insert elements during the transformation, do not use this with transformers that try to.
   * @note This does not preserve the ids of non-element entities such as link table relationships, or aspects, etc.
   * @default false
   * @beta
   */
  preserveElementIdsForFiltering?: boolean;

  /** The behavior to use when an element reference (id) is found stored as a reference on an element in the source,
   * but the referenced element does not actually exist in the source.
   * It is possible to craft an iModel with dangling references/invalidated relationships by, e.g., deleting certain
   * elements without fixing up references.
   *
   * @note "reject" will throw an error and reject the transformation upon finding this case.
   * @note "ignore" passes the issue down to consuming applications, iModels that have invalid element references
   *       like this can cause errors, and you should consider adding custom logic in your transformer to remove the
   *       reference depending on your use case.
   * @default "reject"
   * @beta
   * @deprecated in 3.x. use [[danglingReferencesBehavior]] instead, the use of the term *predecessors* was confusing and became inaccurate when the transformer could handle cycles
   */
  danglingPredecessorsBehavior?: "reject" | "ignore";

  /** The behavior to use when an element reference (id) is found stored as a reference on an element in the source,
   * but the referenced element does not actually exist in the source.
   * It is possible to craft an iModel with dangling references/invalidated relationships by, e.g., deleting certain
   * elements without fixing up references.
   *
   * @note "reject" will throw an error and reject the transformation upon finding this case.
   * @note "ignore" passes the issue down to consuming applications, iModels that have invalid element references
   *       like this can cause errors, and you should consider adding custom logic in your transformer to remove the
   *       reference depending on your use case.
   * @default "reject"
   * @beta
   */
  danglingReferencesBehavior?: "reject" | "ignore";

  /** If defined, options to be supplied to [[IModelImporter.optimizeGeometry]] by [[IModelTransformer.processChanges]] and [[IModelTransformer.processAll]]
   * as a post-processing step to optimize the geometry in the iModel.
   * @beta
   */
  optimizeGeometry?: OptimizeGeometryOptions;

  /**
   * force the insertion of external source aspects to provide provenance, even if there are federation guids
   * in the source that we can use. This can make some operations (like transforming new elements or initializing forks)
   * much slower due to needing to insert aspects, but prevents requiring change information for future merges.
   * @default false
   */
  forceExternalSourceAspectProvenance?: boolean;
}

/**
 * A container for tracking the state of a partially committed entity and finalizing it when it's ready to be fully committed
 * @internal
 */
class PartiallyCommittedEntity {
  public constructor(
    /**
     * A set of "model|element ++ ID64" pairs, (e.g. `model0x11` or `element0x12`)
     * It is possible for the submodel of an element to be separately resolved from the actual element,
     * so its resolution must be tracked separately
     */
    private _missingReferences: EntityReferenceSet,
    private _onComplete: () => void
  ) {}
  public resolveReference(id: EntityReference) {
    this._missingReferences.delete(id);
    if (this._missingReferences.size === 0)
      this._onComplete();
  }
  public forceComplete() {
    this._onComplete();
  }
}

/**
 * Apply a function to each Id64 in a supported container type of Id64s.
 * Currently only supports raw Id64String or RelatedElement-like objects containing an `id` property that is a Id64String,
 * which matches the possible containers of references in [Element.requiredReferenceKeys]($backend).
 * @internal
 */
function mapId64<R>(
  idContainer: Id64String | { id: Id64String } | undefined,
  func: (id: Id64String) => R
): R[] {
  const isId64String = (arg: any): arg is Id64String => {
    const isString = typeof arg === "string";
    assert(() => !isString || Id64.isValidId64(arg));
    return isString;
  };
  const isRelatedElem = (arg: any): arg is RelatedElement =>
    arg && typeof arg === "object" && "id" in arg;

  const results = [];

  // is a string if compressed or singular id64, but check for singular just checks if it's a string so do this test first
  if (idContainer === undefined) {
    // nothing
  } else if (isId64String(idContainer)) {
    results.push(func(idContainer));
  } else if (isRelatedElem(idContainer)) {
    results.push(func(idContainer.id));
  } else {
    throw Error([
      `Id64 container '${idContainer}' is unsupported.`,
      "Currently only singular Id64 strings or prop-like objects containing an 'id' property are supported.",
    ].join("\n"));
  }
  return results;
}

/** Arguments you can pass to [[IModelTransformer.initialize]]
 * @beta
 */
export interface InitArgs {
  accessToken?: AccessToken;
  startChangesetId?: string;
}

/** Arguments you can pass to [[IModelTransformer.initExternalSourceAspects]]
 * @deprecated in 0.1.0. Use [[InitArgs]] (and [[IModelTransformer.initialize]]) instead.
 */
export type InitFromExternalSourceAspectsArgs = InitArgs;

/** events that the transformer emits, e.g. for signaling profilers @internal */
export enum TransformerEvent {
  beginProcessSchemas = "beginProcessSchemas",
  endProcessSchemas = "endProcessSchemas",
  beginProcessAll = "beginProcessAll",
  endProcessAll = "endProcessAll",
  beginProcessChanges = "beginProcessChanges",
  endProcessChanges = "endProcessChanges",
}

/** Base class used to transform a source iModel into a different target iModel.
 * @see [iModel Transformation and Data Exchange]($docs/learning/transformer/index.md), [IModelExporter]($transformer), [IModelImporter]($transformer)
 * @beta
 */
export class IModelTransformer extends IModelExportHandler {
  /** The IModelExporter that will export from the source iModel. */
  public readonly exporter: IModelExporter;
  /** The IModelImporter that will import into the target iModel. */
  public readonly importer: IModelImporter;
  /** The normally read-only source iModel.
   * @note The source iModel will need to be read/write when provenance is being stored during a reverse synchronization.
   */
  public readonly sourceDb: IModelDb;
  /** The read/write target iModel. */
  public readonly targetDb: IModelDb;
  /** The IModelTransformContext for this IModelTransformer. */
  public readonly context: IModelCloneContext;
  /** The Id of the Element in the **target** iModel that represents the **source** repository as a whole and scopes its [ExternalSourceAspect]($backend) instances. */
  public get targetScopeElementId(): Id64String {
    return this._options.targetScopeElementId;
  }

  /** map of (unprocessed element, referencing processed element) pairs to the partially committed element that needs the reference resolved
   * and have some helper methods below for now */
  protected _pendingReferences = new PendingReferenceMap<PartiallyCommittedEntity>();

  /** a set of elements for which source provenance will be explicitly tracked by ExternalSourceAspects */
  protected _elementsWithExplicitlyTrackedProvenance = new Set<Id64String> ();

  /** map of partially committed entities to their partial commit progress */
  protected _partiallyCommittedEntities = new EntityMap<PartiallyCommittedEntity>();

  /** the options that were used to initialize this transformer */
  private readonly _options: MarkRequired<IModelTransformOptions, "targetScopeElementId" | "danglingReferencesBehavior">;

  /** Set if it can be determined whether this is the first source --> target synchronization. */
  private _isFirstSynchronization?: boolean;

  /** The element classes that are considered to define provenance in the iModel */
  public static get provenanceElementClasses(): (typeof Entity)[] {
    return [FolderLink, SynchronizationConfigLink, ExternalSource, ExternalSourceAttachment];
  }

  /** The element aspect classes that are considered to define provenance in the iModel */
  public static get provenanceElementAspectClasses(): (typeof Entity)[] {
    return [ExternalSourceAspect];
  }

  /**
   * Internal event emitter that is used by the transformer to signal events to profilers
   * @internal
   */
  public events = new EventEmitter();

  /** Construct a new IModelTransformer
   * @param source Specifies the source IModelExporter or the source IModelDb that will be used to construct the source IModelExporter.
   * @param target Specifies the target IModelImporter or the target IModelDb that will be used to construct the target IModelImporter.
   * @param options The options that specify how the transformation should be done.
   */
  public constructor(source: IModelDb | IModelExporter, target: IModelDb | IModelImporter, options?: IModelTransformOptions) {
    super();
    // initialize IModelTransformOptions
    this._options = {
      ...options,
      // non-falsy defaults
      cloneUsingBinaryGeometry: options?.cloneUsingBinaryGeometry ?? true,
      targetScopeElementId: options?.targetScopeElementId ?? IModel.rootSubjectId,
      // eslint-disable-next-line deprecation/deprecation
      danglingReferencesBehavior: options?.danglingReferencesBehavior ?? options?.danglingPredecessorsBehavior ?? "reject",
    };
    this._isFirstSynchronization = this._options.wasSourceIModelCopiedToTarget ? true : undefined;
    // initialize exporter and sourceDb
    if (source instanceof IModelDb) {
      this.exporter = new IModelExporter(source);
    } else {
      this.exporter = source;
    }
    this.sourceDb = this.exporter.sourceDb;
    this.exporter.registerHandler(this);
    this.exporter.wantGeometry = options?.loadSourceGeometry ?? false; // optimization to not load source GeometryStreams by default
    if (!this._options.includeSourceProvenance) { // clone provenance from the source iModel into the target iModel?
      IModelTransformer.provenanceElementClasses.forEach((cls) => this.exporter.excludeElementClass(cls.classFullName));
      IModelTransformer.provenanceElementAspectClasses.forEach((cls) => this.exporter.excludeElementAspectClass(cls.classFullName));
    }
    this.exporter.excludeElementAspectClass(ChannelRootAspect.classFullName); // Channel boundaries within the source iModel are not relevant to the target iModel
    this.exporter.excludeElementAspectClass("BisCore:TextAnnotationData"); // This ElementAspect is auto-created by the BisCore:TextAnnotation2d/3d element handlers
    // initialize importer and targetDb
    if (target instanceof IModelDb) {
      this.importer = new IModelImporter(target, { preserveElementIdsForFiltering: this._options.preserveElementIdsForFiltering });
    } else {
      this.importer = target;
      /* eslint-disable deprecation/deprecation */
      if (Boolean(this._options.preserveElementIdsForFiltering) !== this.importer.preserveElementIdsForFiltering) {
        Logger.logWarning(
          loggerCategory,
          [
            "A custom importer was passed as a target but its 'preserveElementIdsForFiltering' option is out of sync with the transformer's option.",
            "The custom importer target's option will be force updated to use the transformer's value.",
            "This behavior is deprecated and will be removed in a future version, throwing an error if they are out of sync.",
          ].join("\n")
        );
        this.importer.preserveElementIdsForFiltering = Boolean(this._options.preserveElementIdsForFiltering);
      }
      /* eslint-enable deprecation/deprecation */
    }
    this.targetDb = this.importer.targetDb;
    // create the IModelCloneContext, it must be initialized later
    this.context = new IModelCloneContext(this.sourceDb, this.targetDb);

    this._registerEvents();
  }

  /** @internal */
  public _registerEvents() {
    this.events.on(TransformerEvent.beginProcessAll, () => {
      Logger.logTrace(loggerCategory, "processAll()");
    });
    this.events.on(TransformerEvent.beginProcessChanges, () => {
      Logger.logTrace(loggerCategory, "processChanges()");
    });
  }

  /** Dispose any native resources associated with this IModelTransformer. */
  public dispose(): void {
    Logger.logTrace(loggerCategory, "dispose()");
    this.context.dispose();
  }

  /** Log current settings that affect IModelTransformer's behavior. */
  private logSettings(): void {
    Logger.logInfo(TransformerLoggerCategory.IModelExporter, `this.exporter.visitElements=${this.exporter.visitElements}`);
    Logger.logInfo(TransformerLoggerCategory.IModelExporter, `this.exporter.visitRelationships=${this.exporter.visitRelationships}`);
    Logger.logInfo(TransformerLoggerCategory.IModelExporter, `this.exporter.wantGeometry=${this.exporter.wantGeometry}`);
    Logger.logInfo(TransformerLoggerCategory.IModelExporter, `this.exporter.wantSystemSchemas=${this.exporter.wantSystemSchemas}`);
    Logger.logInfo(TransformerLoggerCategory.IModelExporter, `this.exporter.wantTemplateModels=${this.exporter.wantTemplateModels}`);
    Logger.logInfo(loggerCategory, `this.targetScopeElementId=${this.targetScopeElementId}`);
    Logger.logInfo(loggerCategory, `this._noProvenance=${this._options.noProvenance}`);
    Logger.logInfo(loggerCategory, `this._includeSourceProvenance=${this._options.includeSourceProvenance}`);
    Logger.logInfo(loggerCategory, `this._cloneUsingBinaryGeometry=${this._options.cloneUsingBinaryGeometry}`);
    Logger.logInfo(loggerCategory, `this._wasSourceIModelCopiedToTarget=${this._options.wasSourceIModelCopiedToTarget}`);
    Logger.logInfo(loggerCategory, `this._isReverseSynchronization=${this._options.isReverseSynchronization}`);
    Logger.logInfo(TransformerLoggerCategory.IModelImporter, `this.importer.autoExtendProjectExtents=${this.importer.options.autoExtendProjectExtents}`);
    Logger.logInfo(TransformerLoggerCategory.IModelImporter, `this.importer.simplifyElementGeometry=${this.importer.options.simplifyElementGeometry}`);
  }

  /** Return the IModelDb where IModelTransformer will store its provenance.
   * @note This will be [[targetDb]] except when it is a reverse synchronization. In that case it be [[sourceDb]].
   */
  public get provenanceDb(): IModelDb {
    return this._options.isReverseSynchronization ? this.sourceDb : this.targetDb;
  }

  /** Return the IModelDb where IModelTransformer will NOT store its provenance.
   * @note This will be [[sourceDb]] except when it is a reverse synchronization. In that case it be [[targetDb]].
   */
  public get provenanceSourceDb(): IModelDb {
    return this._options.isReverseSynchronization ? this.targetDb : this.sourceDb;
  }

  private initElementProvenance(sourceElementId: Id64String, targetElementId: Id64String): ExternalSourceAspectProps {
    const elementId = this._options.isReverseSynchronization ? sourceElementId : targetElementId;
    const aspectIdentifier = this._options.isReverseSynchronization ? targetElementId : sourceElementId;
    const aspectProps: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: elementId, relClassName: ElementOwnsExternalSourceAspects.classFullName },
      scope: { id: this.targetScopeElementId },
      identifier: aspectIdentifier,
      kind: ExternalSourceAspect.Kind.Element,
      version: this.sourceDb.elements.queryLastModifiedTime(sourceElementId),
    };
    return aspectProps;
  }

  /** Create an ExternalSourceAspectProps in a standard way for a Relationship in an iModel --> iModel transformations.
   * The ExternalSourceAspect is meant to be owned by the Element in the target iModel that is the `sourceId` of transformed relationship.
   * The `identifier` property of the ExternalSourceAspect will be the ECInstanceId of the relationship in the source iModel.
   * The ECInstanceId of the relationship in the target iModel will be stored in the JsonProperties of the ExternalSourceAspect.
   */
  private initRelationshipProvenance(sourceRelationship: Relationship, targetRelInstanceId: Id64String): ExternalSourceAspectProps {
    const targetRelationship: Relationship = this.targetDb.relationships.getInstance(ElementRefersToElements.classFullName, targetRelInstanceId);
    const elementId = this._options.isReverseSynchronization ? sourceRelationship.sourceId : targetRelationship.sourceId;
    const aspectIdentifier = this._options.isReverseSynchronization ? targetRelInstanceId : sourceRelationship.id;
    const aspectProps: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: elementId, relClassName: ElementOwnsExternalSourceAspects.classFullName },
      scope: { id: this.targetScopeElementId },
      identifier: aspectIdentifier,
      kind: ExternalSourceAspect.Kind.Relationship,
      jsonProperties: JSON.stringify({ targetRelInstanceId }),
    };
    [aspectProps.id] = this.queryScopeExternalSource(aspectProps);
    return aspectProps;
  }

  private _targetScopeProvenanceProps: ExternalSourceAspectProps | undefined = undefined;

  private _cachedTargetScopeVersion: ChangesetIndexAndId | undefined = undefined;

  /** the changeset in the scoping element's source version found for this transformation
   * @note: empty string and -1 for changeset and index if it has never been transformed
   */
  private get _targetScopeVersion(): ChangesetIndexAndId {
    if (!this._cachedTargetScopeVersion) {
      nodeAssert(this._targetScopeProvenanceProps?.version !== undefined, "_targetScopeProvenanceProps was not set yet, or contains no version");
      const [id, index] = this._targetScopeProvenanceProps.version === ""
        ? ["", -1]
        : this._targetScopeProvenanceProps.version.split(";");
      this._cachedTargetScopeVersion = { index: Number(index), id, };
      nodeAssert(!Number.isNaN(this._cachedTargetScopeVersion.index), "bad parse: invalid index in version");
    }
    return this._cachedTargetScopeVersion;
  }

  /**
   * Make sure there are no conflicting other scope-type external source aspects on the *target scope element*,
   * If there are none at all, insert one, then this must be a first synchronization.
   * @returns the last synced version (changesetId) on the target scope's external source aspect,
   *          if this was a [BriefcaseDb]($backend)
   */
  private initScopeProvenance(): void {
    const aspectProps: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: this.targetScopeElementId, relClassName: ElementOwnsExternalSourceAspects.classFullName },
      scope: { id: IModel.rootSubjectId }, // the root Subject scopes scope elements
      identifier: this._options.isReverseSynchronization ? this.targetDb.iModelId : this.sourceDb.iModelId, // the opposite side of where provenance is stored
      kind: ExternalSourceAspect.Kind.Scope,
    };

    // FIXME: handle older transformed iModels
    let version!: Id64String | undefined;
    [aspectProps.id, version] = this.queryScopeExternalSource(aspectProps) ?? []; // this query includes "identifier"
    aspectProps.version = version;

    if (undefined === aspectProps.id) {
      aspectProps.version = ""; // empty since never before transformed. Will be updated in [[finalizeTransformation]]
      // this query does not include "identifier" to find possible conflicts
      const sql = `
        SELECT ECInstanceId
        FROM ${ExternalSourceAspect.classFullName}
        WHERE Element.Id=:elementId
          AND Scope.Id=:scopeId
          AND Kind=:kind
        LIMIT 1
      `;
      const hasConflictingScope = this.provenanceDb.withPreparedStatement(sql, (statement: ECSqlStatement): boolean => {
        statement.bindId("elementId", aspectProps.element.id);
        statement.bindId("scopeId", aspectProps.scope.id); // this scope.id can never be invalid, we create it above
        statement.bindString("kind", aspectProps.kind);
        return DbResult.BE_SQLITE_ROW === statement.step();
      });
      if (hasConflictingScope) {
        throw new IModelError(IModelStatus.InvalidId, "Provenance scope conflict");
      }
      if (!this._options.noProvenance) {
        this.provenanceDb.elements.insertAspect(aspectProps);
        this._isFirstSynchronization = true; // couldn't tell this is the first time without provenance
      }
    }

    this._targetScopeProvenanceProps = aspectProps;
  }

  /** @returns the [id, version] of an aspect with the given element, scope, kind, and identifier */
  private queryScopeExternalSource(aspectProps: ExternalSourceAspectProps): [Id64String, Id64String] | [undefined, undefined] {
    const sql = `
      SELECT ECInstanceId, Version
      FROM ${ExternalSourceAspect.classFullName}
      WHERE Element.Id=:elementId
        AND Scope.Id=:scopeId
        AND Kind=:kind
        AND Identifier=:identifier
      LIMIT 1
    `;
    return this.provenanceDb.withPreparedStatement(sql, (statement: ECSqlStatement) => {
      statement.bindId("elementId", aspectProps.element.id);
      if (aspectProps.scope === undefined)
        return [undefined, undefined]; // return undefined instead of binding an invalid id
      statement.bindId("scopeId", aspectProps.scope.id);
      statement.bindString("kind", aspectProps.kind);
      statement.bindString("identifier", aspectProps.identifier);
      if (DbResult.BE_SQLITE_ROW !== statement.step())
        return [undefined, undefined];
      const aspectId = statement.getValue(0).getId();
      const version = statement.getValue(1).getString();
      return [aspectId, version];
    });
  }

  /**
   * Iterate all matching ExternalSourceAspects in the provenance iModel (target unless reverse sync) and call a function for each one.
   * @note provenance is done by federation guids where possible
   */
  private forEachTrackedElement(fn: (sourceElementId: Id64String, targetElementId: Id64String) => void): void {
    if (!this.provenanceDb.containsClass(ExternalSourceAspect.classFullName)) {
      throw new IModelError(IModelStatus.BadSchema, "The BisCore schema version of the target database is too old");
    }

    // query for provenanceDb
    const provenanceContainerQuery = `
      SELECT e.ECInstanceId, FederationGuid, esa.Identifier as AspectIdentifier
      FROM bis.Element e
      LEFT JOIN bis.ExternalSourceAspect esa ON e.ECInstanceId=esa.Element.Id
      WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10) -- special non-federated iModel-local elements
        AND ((Scope.Id IS NULL AND KIND IS NULL) OR (Scope.Id=:scopeId AND Kind=:kind))
      ORDER BY FederationGuid
    `;

    // query for nonProvenanceDb, the source to which the provenance is referring
    const provenanceSourceQuery = `
      SELECT e.ECInstanceId, FederationGuid
      FROM bis.Element e
      WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10) -- special non-federated iModel-local elements
      ORDER BY FederationGuid
    `;

    // iterate through sorted list of fed guids from both dbs to get the intersection
    // NOTE: if we exposed the native attach database support,
    // we could get the intersection of fed guids in one query, not sure if it would be faster
    // OR we could do a raw sqlite query...
    this.provenanceSourceDb.withStatement(provenanceSourceQuery, (sourceStmt) => this.provenanceDb.withStatement(provenanceContainerQuery, (containerStmt) => {
      containerStmt.bindId("scopeId", this.targetScopeElementId);
      containerStmt.bindString("kind", ExternalSourceAspect.Kind.Element);

      if (sourceStmt.step() !== DbResult.BE_SQLITE_ROW) return;
      let sourceRow = sourceStmt.getRow() as { federationGuid?: GuidString; id: Id64String };
      if (containerStmt.step() !== DbResult.BE_SQLITE_ROW) return;
      let containerRow = containerStmt.getRow() as { federationGuid?: GuidString; id: Id64String; aspectIdentifier?: Id64String };

      const runFnInProvDirection = (sourceId: Id64String, targetId: Id64String) =>
        this._options.isReverseSynchronization ? fn(sourceId, targetId) : fn(targetId, sourceId);

      while (true) {
        const currSourceRow = sourceRow, currContainerRow = containerRow;
        if (currSourceRow.federationGuid !== undefined
          && currContainerRow.federationGuid !== undefined
          && currSourceRow.federationGuid === currContainerRow.federationGuid
        ) {
          fn(sourceRow.id, containerRow.id);
        }
        if (currContainerRow.federationGuid === undefined
          || (currSourceRow.federationGuid !== undefined
            && currSourceRow.federationGuid >= currContainerRow.federationGuid)
        ) {
          if (containerStmt.step() !== DbResult.BE_SQLITE_ROW) return;
          containerRow = containerStmt.getRow();
        }
        if (currSourceRow.federationGuid === undefined
          || (currContainerRow.federationGuid !== undefined
            && currSourceRow.federationGuid <= currContainerRow.federationGuid)
        ) {
          if (sourceStmt.step() !== DbResult.BE_SQLITE_ROW) return;
          sourceRow = sourceStmt.getRow();
        }
        if (!currContainerRow.federationGuid  && currContainerRow.aspectIdentifier)
          runFnInProvDirection(currContainerRow.id, currContainerRow.aspectIdentifier);
      }
    }));
  }

  /** Initialize the source to target Element mapping from ExternalSourceAspects in the target iModel.
   * @note This method is called from all `process*` functions and should never need to be called directly.
   * @deprecated in 3.x. call [[initialize]] instead, it does the same thing among other initialization
   * @note Passing an [[InitFromExternalSourceAspectsArgs]] is required when processing changes, to remap any elements that may have been deleted.
   *       You must await the returned promise as well in this case. The synchronous behavior has not changed but is deprecated and won't process everything.
   */
  public initFromExternalSourceAspects(args?: InitArgs): void | Promise<void> {
    this.forEachTrackedElement((sourceElementId: Id64String, targetElementId: Id64String) => {
      this.context.remapElement(sourceElementId, targetElementId);
    });

    if (args)
      return this.remapDeletedSourceElements();
  }

  /** When processing deleted elements in a reverse synchronization, the [[provenanceDb]] has already
   * deleted the provenance that tell us which elements in the reverse synchronization target (usually
   * a master iModel) should be deleted.
   * We must use the changesets to get the values of those before they were deleted.
   */
  private async remapDeletedSourceElements() {
    // we need a connected iModel with changes to remap elements with deletions
    const notConnectedModel = this.sourceDb.iTwinId === undefined;
    const noChanges = this._targetScopeVersion.index === this.sourceDb.changeset.index;
    if (notConnectedModel || noChanges)
      return;

    nodeAssert(this._changeSummaryIds, "change summaries should be initialized before we get here");
    nodeAssert(this._changeSummaryIds.length > 0, "change summaries should have at least one");

    const deletedElemSql = `
      SELECT ic.ChangedInstance.Id, ${
        this._coalesceChangeSummaryJoinedValue((_, i) => `ec${i}.FederationGuid`)
      }, ${
        this._coalesceChangeSummaryJoinedValue((_, i) => `esac${i}.Identifier`)
      }
      FROM ecchange.change.InstanceChange ic
      -- ask affan about whether this is worth it...
      ${
        this._changeSummaryIds.map((id, i) => `
          LEFT JOIN bis.Element.Changes(${id}, 'BeforeDelete') ec${i}
            ON ic.ChangedInstance.Id=ec${i}.ECInstanceId
        `).join(' ')
      }
      ${
        this._changeSummaryIds.map((id, i) => `
          LEFT JOIN bis.ExternalSourceAspect.Changes(${id}, 'BeforeDelete') esac${i}
            ON ic.ChangedInstance.Id=esac${i}.ECInstanceId
        `).join(' ')
      }
      WHERE ic.OpCode=:opDelete
        AND InVirtualSet(:changeSummaryIds, ic.Summary.Id)
        -- not yet documented ecsql feature to check class id
        AND (
          ic.ChangedInstance.ClassId IS (BisCore.Element)
          OR (
            ic.ChangedInstance.ClassId IS (BisCore.ExternalSourceAspect)
            AND (${
                this._changeSummaryIds
                  .map((_, i) => `esac${i}.Scope.Id=:targetScopeElement`)
                  .join(' OR ')
              })
          )
        )
    `;

    // FIXME: test deletion in both forward and reverse sync
    this.sourceDb.withStatement(deletedElemSql, (stmt) => {
      stmt.bindInteger("opDelete", ChangeOpCode.Delete);
      stmt.bindIdSet("changeSummaryIds", this._changeSummaryIds!);
      stmt.bindId("targetScopeElement", this.targetScopeElementId);
      while (DbResult.BE_SQLITE_ROW === stmt.step()) {
        const sourceId = stmt.getValue(0).getId();
        const sourceFedGuid = stmt.getValue(1).getGuid();
        const maybeEsaIdentifier = stmt.getValue(2).getId();
        // TODO: if I could attach the second db, will probably be much faster to get target id
        // as part of the whole query rather than with _queryElemIdByFedGuid
        const targetId = maybeEsaIdentifier
          ?? (sourceFedGuid && this._queryElemIdByFedGuid(this.targetDb, sourceFedGuid));
        // don't assert because currently we get separate rows for the element and external source aspect change
        // so we may get a no-sourceFedGuid row which is fixed later (usually right after)
        //nodeAssert(targetId, `target for elem ${sourceId} in source could not be determined, provenance is broken`);
        const deletionNotInTarget = !targetId;
        if (deletionNotInTarget) continue;
        // TODO: maybe delete and don't just remap?
        this.context.remapElement(sourceId, targetId);
      }
    });
  }

  private _queryElemIdByFedGuid(db: IModelDb, fedGuid: GuidString): Id64String | undefined {
    return db.withPreparedStatement("SELECT ECInstanceId FROM Bis.Element WHERE FederationGuid=?", (stmt) => {
      stmt.bindGuid(1, fedGuid);
      if (stmt.step() === DbResult.BE_SQLITE_ROW)
        return stmt.getValue(0).getId();
      else
        return undefined;
    });
  }

  /** Returns `true` if *brute force* delete detections should be run.
   * @note Not relevant for processChanges when change history is known.
   */
  private shouldDetectDeletes(): boolean {
    if (this._isFirstSynchronization)
      return false; // not necessary the first time since there are no deletes to detect

    if (this._options.isReverseSynchronization)
      return false; // not possible for a reverse synchronization since provenance will be deleted when element is deleted

    return true;
  }

  /** Detect Element deletes using ExternalSourceAspects in the target iModel and a *brute force* comparison against Elements in the source iModel.
   * @see processChanges
   * @note This method is called from [[processAll]] and is not needed by [[processChanges]], so it only needs to be called directly when processing a subset of an iModel.
   * @throws [[IModelError]] If the required provenance information is not available to detect deletes.
   */
  public async detectElementDeletes(): Promise<void> {
    // FIXME: this is no longer possible to do without change data loading, but I don't think
    // anyone uses this obscure feature, maybe we can remove it?
    if (this._options.isReverseSynchronization) {
      throw new IModelError(IModelStatus.BadRequest, "Cannot detect deletes when isReverseSynchronization=true");
    }
    const targetElementsToDelete: Id64String[] = [];
    this.forEachTrackedElement((sourceElementId: Id64String, targetElementId: Id64String) => {
      if (undefined === this.sourceDb.elements.tryGetElementProps(sourceElementId)) {
        // if the sourceElement is not found, then it must have been deleted, so propagate the delete to the target iModel
        targetElementsToDelete.push(targetElementId);
      }
    });
    targetElementsToDelete.forEach((targetElementId: Id64String) => {
      this.importer.deleteElement(targetElementId);
    });
  }

  /**
   * @deprecated in 3.x, this no longer has any effect except emitting a warning
   */
  protected skipElement(_sourceElement: Element): void {
    Logger.logWarning(loggerCategory, `Tried to defer/skip an element, which is no longer necessary`);
  }

  /** Transform the specified sourceElement into ElementProps for the target iModel.
   * @param sourceElement The Element from the source iModel to transform.
   * @returns ElementProps for the target iModel.
   * @note A subclass can override this method to provide custom transform behavior.
   * @note This can be called more than once for an element in arbitrary order, so it should not have side-effects.
   */
  public onTransformElement(sourceElement: Element): ElementProps {
    Logger.logTrace(loggerCategory, `onTransformElement(${sourceElement.id}) "${sourceElement.getDisplayLabel()}"`);
    const targetElementProps: ElementProps = this.context.cloneElement(sourceElement, { binaryGeometry: this._options.cloneUsingBinaryGeometry });
    if (sourceElement instanceof Subject) {
      if (targetElementProps.jsonProperties?.Subject?.Job) {
        // don't propagate source channels into target (legacy bridge case)
        targetElementProps.jsonProperties.Subject.Job = undefined;
      }
    }
    return targetElementProps;
  }

  // handle sqlite coalesce requiring 2 arguments
  private _coalesceChangeSummaryJoinedValue(f: (id: Id64String, index: number) => string) {
    nodeAssert(this._changeSummaryIds?.length && this._changeSummaryIds.length > 0, "should have changeset data by now");
    const valueList = this._changeSummaryIds!.map(f).join(',');
    return this._changeSummaryIds!.length > 1 ? `coalesce(${valueList})` : valueList;
  };

  // if undefined, it can be initialized by calling [[this._cacheSourceChanges]]
  private _hasElementChangedCache?: Set<Id64String> = undefined;
  private _deletedSourceRelationshipData?: Map<Id64String, {
    sourceFedGuid: Id64String;
    targetFedGuid: Id64String;
    classFullName: Id64String;
  }> = undefined;

  // FIXME: this is a PoC, don't load this all into memory
  private _cacheSourceChanges() {
    nodeAssert(this._changeSummaryIds && this._changeSummaryIds.length > 0, "should have changeset data by now");
    this._hasElementChangedCache = new Set();
    this._deletedSourceRelationshipData = new Map();

    // somewhat complicated query because doing two things at once...
    // (not to mention the multijoin coalescing hack)
    // FIXME: perhaps the coalescing indicates that part should be done manually, not in the query?
    const query = `
      SELECT
        ic.ChangedInstance.Id AS InstId,
        -- NOTE: parse error even with () without iif, also elem or rel is enforced in WHERE
        iif(ic.ChangedInstance.ClassId IS (BisCore.Element), TRUE, FALSE) AS IsElemNotDeletedRel,
        coalesce(${
          // HACK: adding "NONE" for empty result seems to prevent a bug where getValue(3) stops working after the NULL columns
          this._changeSummaryIds.map((_, i) => `se${i}.FederationGuid, sec${i}.FederationGuid`).concat("'NONE'").join(',')
        }) AS SourceFedGuid,
        coalesce(${
          this._changeSummaryIds.map((_, i) => `te${i}.FederationGuid, tec${i}.FederationGuid`).concat("'NONE'").join(',')
        }) AS TargetFedGuid,
        ic.ChangedInstance.ClassId AS ClassId
      FROM ecchange.change.InstanceChange ic
      JOIN iModelChange.Changeset imc ON ic.Summary.Id=imc.Summary.Id
      -- ask affan about whether this is worth it... maybe the ""
      ${
        this._changeSummaryIds.map((id, i) => `
          LEFT JOIN bis.ElementRefersToElements.Changes(${id}, 'BeforeDelete') ertec${i}
            -- NOTE: see how the AND affects performance, it could be dropped
            ON ic.ChangedInstance.Id=ertec${i}.ECInstanceId
              AND NOT ic.ChangedInstance.ClassId IS (BisCore.Element)
          -- FIXME: test a deletion of both an element and a relationship at the same time
          LEFT JOIN bis.Element se${i}
            ON se${i}.ECInstanceId=ertec${i}.SourceECInstanceId
          LEFT JOIN bis.Element te${i}
            ON te${i}.ECInstanceId=ertec${i}.TargetECInstanceId
          LEFT JOIN bis.Element.Changes(${id}, 'BeforeDelete') sec${i}
            ON sec${i}.ECInstanceId=ertec${i}.SourceECInstanceId
          LEFT JOIN bis.Element.Changes(${id}, 'BeforeDelete') tec${i}
            ON tec${i}.ECInstanceId=ertec${i}.TargetECInstanceId
        `).join('')
      }
      WHERE ((ic.ChangedInstance.ClassId IS (BisCore.Element)
              OR ic.ChangedInstance.ClassId IS (BisCore.ElementRefersToElements))
            -- ignore deleted elems, we take care of those separately.
            -- include inserted elems since inserted code-colliding elements should be considered
            -- a change so that the colliding element is exported to the target
            ) AND ic.OpCode<>:opDelete
    `;


    this.sourceDb.withPreparedStatement(query,
      (stmt) => {
        stmt.bindInteger("opDelete", ChangeOpCode.Delete);
        while (DbResult.BE_SQLITE_ROW === stmt.step()) {
          // REPORT: stmt.getValue(>3) seems to be bugged but the values survive .getRow so using that for now
          const instId = stmt.getValue(0).getId();
          const isElemNotDeletedRel = stmt.getValue(1).getBoolean();
          if (isElemNotDeletedRel)
            this._hasElementChangedCache!.add(instId);
          else {
            const sourceFedGuid = stmt.getValue(2).getGuid();
            const targetFedGuid = stmt.getValue(3).getGuid();
            const classFullName = stmt.getValue(4).getClassNameForClassId();
            this._deletedSourceRelationshipData!.set(instId, { classFullName, sourceFedGuid, targetFedGuid });
          }
        }
      }
    );
  }

  /** Returns true if a change within sourceElement is detected.
   * @param sourceElement The Element from the source iModel
   * @param targetElementId The Element from the target iModel to compare against.
   * @note A subclass can override this method to provide custom change detection behavior.
   */
  protected hasElementChanged(sourceElement: Element, _targetElementId: Id64String): boolean {
    if (this._changeDataState === "no-changes") return false;
    if (this._changeDataState === "unconnected") return true;
    nodeAssert(this._changeDataState === "has-changes", "change data should be initialized by now");
    if (this._hasElementChangedCache === undefined) this._cacheSourceChanges();
    return this._hasElementChangedCache!.has(sourceElement.id);
  }

  private static transformCallbackFor(transformer: IModelTransformer, entity: ConcreteEntity): EntityTransformHandler {
    if (entity instanceof Element)
      return transformer.onTransformElement as EntityTransformHandler; // eslint-disable-line @typescript-eslint/unbound-method
    else if (entity instanceof Element)
      return transformer.onTransformModel as EntityTransformHandler; // eslint-disable-line @typescript-eslint/unbound-method
    else if (entity instanceof Relationship)
      return transformer.onTransformRelationship as EntityTransformHandler; // eslint-disable-line @typescript-eslint/unbound-method
    else if (entity instanceof ElementAspect)
      return transformer.onTransformElementAspect as EntityTransformHandler; // eslint-disable-line @typescript-eslint/unbound-method
    else
      assert(false, `unreachable; entity was '${entity.constructor.name}' not an Element, Relationship, or ElementAspect`);
  }

  /** callback to perform when a partial element says it's ready to be completed
   * transforms the source element with all references now valid, then updates the partial element with the results
   */
  private makePartialEntityCompleter(
    sourceEntity: ConcreteEntity
  ) {
    return () => {
      const targetId = this.context.findTargetEntityId(EntityReferences.from(sourceEntity));
      if (!EntityReferences.isValid(targetId))
        throw Error(`${sourceEntity.id} has not been inserted into the target yet, the completer is invalid. This is a bug.`);
      const onEntityTransform = IModelTransformer.transformCallbackFor(this, sourceEntity);
      const updateEntity = EntityUnifier.updaterFor(this.targetDb, sourceEntity);
      const targetProps = onEntityTransform.call(this, sourceEntity);
      if (sourceEntity instanceof Relationship) {
        (targetProps as RelationshipProps).sourceId = this.context.findTargetElementId(sourceEntity.sourceId);
        (targetProps as RelationshipProps).targetId = this.context.findTargetElementId(sourceEntity.targetId);
      }
      updateEntity({ ...targetProps, id: EntityReferences.toId64(targetId) });
      this._partiallyCommittedEntities.delete(sourceEntity);
    };
  }

  /** collect references this entity has that are yet to be mapped, and if there are any
   * create a [[PartiallyCommittedEntity]] to track resolution of those references
   */
  private collectUnmappedReferences(entity: ConcreteEntity) {
    const missingReferences = new EntityReferenceSet();
    let thisPartialElem: PartiallyCommittedEntity | undefined;

    for (const referenceId of entity.getReferenceConcreteIds()) {
      // TODO: probably need to rename from 'id' to 'ref' so these names aren't so ambiguous
      const referenceIdInTarget = this.context.findTargetEntityId(referenceId);
      const alreadyImported = EntityReferences.isValid(referenceIdInTarget);
      if (alreadyImported)
        continue;
      Logger.logTrace(loggerCategory, `Deferring resolution of reference '${referenceId}' of element '${entity.id}'`);
      const referencedExistsInSource = EntityUnifier.exists(this.sourceDb, { entityReference: referenceId });
      if (!referencedExistsInSource) {
        Logger.logWarning(loggerCategory, `Source ${EntityUnifier.getReadableType(entity)} (${entity.id}) has a dangling reference to (${referenceId})`);
        switch (this._options.danglingReferencesBehavior) {
          case "ignore":
            continue;
          case "reject":
            throw new IModelError(
              IModelStatus.NotFound,
              [
                `Found a reference to an element "${referenceId}" that doesn't exist while looking for references of "${entity.id}".`,
                "This must have been caused by an upstream application that changed the iModel.",
                "You can set the IModelTransformerOptions.danglingReferencesBehavior option to 'ignore' to ignore this, but this will leave the iModel",
                "in a state where downstream consuming applications will need to handle the invalidity themselves. In some cases, writing a custom",
                "transformer to remove the reference and fix affected elements may be suitable.",
              ].join("\n")
            );
        }
      }
      if (thisPartialElem === undefined) {
        thisPartialElem = new PartiallyCommittedEntity(missingReferences, this.makePartialEntityCompleter(entity));
        if (!this._partiallyCommittedEntities.has(entity))
          this._partiallyCommittedEntities.set(entity, thisPartialElem);
      }
      missingReferences.add(referenceId);
      const entityReference = EntityReferences.from(entity);
      this._pendingReferences.set({ referenced: referenceId, referencer: entityReference }, thisPartialElem);
    }
  }

  /** Cause the specified Element and its child Elements (if applicable) to be exported from the source iModel and imported into the target iModel.
   * @param sourceElementId Identifies the Element from the source iModel to import.
   * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
   */
  public async processElement(sourceElementId: Id64String): Promise<void> {
    await this.initialize();
    if (sourceElementId === IModel.rootSubjectId) {
      throw new IModelError(IModelStatus.BadRequest, "The root Subject should not be directly imported");
    }
    return this.exporter.exportElement(sourceElementId);
  }

  /** Import child elements into the target IModelDb
   * @param sourceElementId Import the child elements of this element in the source IModelDb.
   * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
   */
  public async processChildElements(sourceElementId: Id64String): Promise<void> {
    await this.initialize();
    return this.exporter.exportChildElements(sourceElementId);
  }

  /** Override of [IModelExportHandler.shouldExportElement]($transformer) that is called to determine if an element should be exported from the source iModel.
   * @note Reaching this point means that the element has passed the standard exclusion checks in IModelExporter.
   */
  public override shouldExportElement(_sourceElement: Element): boolean { return true; }

  /**
   * If they haven't been already, import all of the required references
   * @internal do not call, override or implement this, it will be removed
   */
  public override async preExportElement(sourceElement: Element): Promise<void> {
    const elemClass = sourceElement.constructor as typeof Element;

    const unresolvedReferences = elemClass.requiredReferenceKeys
      .map((referenceKey) => {
        const idContainer = sourceElement[referenceKey as keyof Element];
        const referenceType = elemClass.requiredReferenceKeyTypeMap[referenceKey];
        // For now we just consider all required references to be elements (as they are in biscore), and do not support
        // entities that refuse to be inserted without a different kind of entity (e.g. aspect or relationship) first being inserted
        assert(referenceType === ConcreteEntityTypes.Element || referenceType === ConcreteEntityTypes.Model);
        return mapId64(idContainer, (id) => {
          if (id === Id64.invalid || id === IModel.rootSubjectId)
            return undefined; // not allowed to directly export the root subject
          if (!this.context.isBetweenIModels) {
            // Within the same iModel, can use existing DefinitionElements without remapping
            // This is relied upon by the TemplateModelCloner
            // TODO: extract this out to only be in the TemplateModelCloner
            const asDefinitionElem = this.sourceDb.elements.tryGetElement(id, DefinitionElement);
            if (asDefinitionElem && !(asDefinitionElem instanceof RecipeDefinitionElement)) {
              this.context.remapElement(id, id);
            }
          }
          return id;
        })
          .filter((sourceReferenceId: Id64String | undefined): sourceReferenceId is Id64String => {
            if (sourceReferenceId === undefined)
              return false;
            const referenceInTargetId = this.context.findTargetElementId(sourceReferenceId);
            const isInTarget = Id64.isValid(referenceInTargetId);
            return !isInTarget;
          });
      })
      .flat();

    if (unresolvedReferences.length > 0) {
      for (const reference of unresolvedReferences) {
        const processState = this.getElemTransformState(reference);
        // must export element first
        if (processState.needsElemImport)
          await this.exporter.exportElement(reference);
        if (processState.needsModelImport)
          await this.exporter.exportModel(reference);
      }
    }
  }

  private getElemTransformState(elementId: Id64String) {
    const dbHasModel = (db: IModelDb, id: Id64String) => {
      const maybeModelId = EntityReferences.fromEntityType(id, ConcreteEntityTypes.Model);
      return EntityUnifier.exists(db, { entityReference: maybeModelId });
    };
    const isSubModeled = dbHasModel(this.sourceDb, elementId);
    const idOfElemInTarget = this.context.findTargetElementId(elementId);
    const isElemInTarget = Id64.invalid !== idOfElemInTarget;
    const needsModelImport = isSubModeled && (!isElemInTarget || !dbHasModel(this.targetDb, idOfElemInTarget));
    return { needsElemImport: !isElemInTarget, needsModelImport };
  }

  /** Override of [IModelExportHandler.onExportElement]($transformer) that imports an element into the target iModel when it is exported from the source iModel.
   * This override calls [[onTransformElement]] and then [IModelImporter.importElement]($transformer) to update the target iModel.
   */
  public override onExportElement(sourceElement: Element): void {
    let targetElementId: Id64String | undefined;
    let targetElementProps: ElementProps;
    if (this._options.preserveElementIdsForFiltering) {
      targetElementId = sourceElement.id;
      targetElementProps = this.onTransformElement(sourceElement);
    } else if (this._options.wasSourceIModelCopiedToTarget) {
      targetElementId = sourceElement.id;
      targetElementProps = this.targetDb.elements.getElementProps(targetElementId);
    } else {
      targetElementId = this.context.findTargetElementId(sourceElement.id);
      targetElementProps = this.onTransformElement(sourceElement);
    }
    // if an existing remapping was not yet found, check by Code as long as the CodeScope is valid (invalid means a missing reference so not worth checking)
    if (!Id64.isValidId64(targetElementId) && Id64.isValidId64(targetElementProps.code.scope)) {
      // respond the same way to undefined code value as the @see Code class, but don't use that class because is trims
      // whitespace from the value, and there are iModels out there with untrimmed whitespace that we ought not to trim
      targetElementProps.code.value = targetElementProps.code.value ?? "";
      targetElementId = this.targetDb.elements.queryElementIdByCode(targetElementProps.code as Required<CodeProps>);
      if (undefined !== targetElementId) {
        const targetElement: Element = this.targetDb.elements.getElement(targetElementId);
        if (targetElement.classFullName === targetElementProps.classFullName) { // ensure code remapping doesn't change the target class
          this.context.remapElement(sourceElement.id, targetElementId); // record that the targetElement was found by Code
        } else {
          targetElementId = undefined;
          targetElementProps.code = Code.createEmpty(); // clear out invalid code
        }
      }
    }

    if (targetElementId !== undefined
      && Id64.isValid(targetElementId)
      && !this.hasElementChanged(sourceElement, targetElementId)
    ) return;

    this.collectUnmappedReferences(sourceElement);

    // TODO: untangle targetElementId state...
    if (targetElementId === Id64.invalid)
      targetElementId = undefined;

    targetElementProps.id = targetElementId; // targetElementId will be valid (indicating update) or undefined (indicating insert)
    if (!this._options.wasSourceIModelCopiedToTarget) {
      this.importer.importElement(targetElementProps); // don't need to import if iModel was copied
    }
    this.context.remapElement(sourceElement.id, targetElementProps.id!); // targetElementProps.id assigned by importElement
    // now that we've mapped this elem we can fix unmapped references to it
    this.resolvePendingReferences(sourceElement);

    // the transformer does not currently 'split' or 'join' any elements, therefore, it does not
    // insert external source aspects because federation guids are sufficient for this.
    // Other transformer subclasses must insert the appropriate aspect (as provided by a TBD API)
    // when splitting/joining elements
    // physical consolidation is an example of a 'joining' transform
    // FIXME: document this externally!
    // verify at finalization time that we don't lose provenance on new elements
    // make public and improve `initElementProvenance` API for usage by consolidators
    if (!this._options.noProvenance) {
      let provenance: Parameters<typeof this.markLastProvenance>[0] | undefined
        = !this._options.forceExternalSourceAspectProvenance
        ? sourceElement.federationGuid
        : undefined;
      if (!provenance || this._elementsWithExplicitlyTrackedProvenance.has(sourceElement.id)) {
        const aspectProps = this.initElementProvenance(sourceElement.id, targetElementProps.id!);
        const [aspectId] = this.queryScopeExternalSource(aspectProps);
        if (aspectId === undefined) {
          aspectProps.id = this.provenanceDb.elements.insertAspect(aspectProps);
        } else {
          aspectProps.id = aspectId;
          this.provenanceDb.elements.updateAspect(aspectProps);
        }
        provenance = aspectProps as MarkRequired<ExternalSourceAspectProps, "id">;
      }
      this.markLastProvenance(provenance, { isRelationship: false });
    }
  }

  private resolvePendingReferences(entity: ConcreteEntity) {
    for (const referencer of this._pendingReferences.getReferencers(entity)) {
      const key = PendingReference.from(referencer, entity);
      const pendingRef = this._pendingReferences.get(key);
      if (!pendingRef)
        continue;
      pendingRef.resolveReference(EntityReferences.from(entity));
      this._pendingReferences.delete(key);
    }
  }

  /** Override of [IModelExportHandler.onDeleteElement]($transformer) that is called when [IModelExporter]($transformer) detects that an Element has been deleted from the source iModel.
   * This override propagates the delete to the target iModel via [IModelImporter.deleteElement]($transformer).
   */
  public override onDeleteElement(sourceElementId: Id64String): void {
    const targetElementId: Id64String = this.context.findTargetElementId(sourceElementId);
    if (Id64.isValidId64(targetElementId)) {
      this.importer.deleteElement(targetElementId);
    }
  }

  /** Override of [IModelExportHandler.onExportModel]($transformer) that is called when a Model should be exported from the source iModel.
   * This override calls [[onTransformModel]] and then [IModelImporter.importModel]($transformer) to update the target iModel.
   */
  public override onExportModel(sourceModel: Model): void {
    if (IModel.repositoryModelId === sourceModel.id) {
      return; // The RepositoryModel should not be directly imported
    }
    const targetModeledElementId: Id64String = this.context.findTargetElementId(sourceModel.id);
    const targetModelProps: ModelProps = this.onTransformModel(sourceModel, targetModeledElementId);
    this.importer.importModel(targetModelProps);
    this.resolvePendingReferences(sourceModel);
  }

  /** Override of [IModelExportHandler.onDeleteModel]($transformer) that is called when [IModelExporter]($transformer) detects that a [Model]($backend) has been deleted from the source iModel. */
  public override onDeleteModel(sourceModelId: Id64String): void {
    // It is possible and apparently occasionally sensical to delete a model without deleting its underlying element.
    // - If only the model is deleted, [[initFromExternalSourceAspects]] will have already remapped the underlying element since it still exists.
    // - If both were deleted, [[remapDeletedSourceElements]] will find and remap the deleted element making this operation valid
    const targetModelId: Id64String = this.context.findTargetElementId(sourceModelId);
    if (Id64.isValidId64(targetModelId)) {
      this.importer.deleteModel(targetModelId);
    }
  }

  /** Cause the model container, contents, and sub-models to be exported from the source iModel and imported into the target iModel.
   * @param sourceModeledElementId Import this [Model]($backend) from the source IModelDb.
   * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
   */
  public async processModel(sourceModeledElementId: Id64String): Promise<void> {
    await this.initialize();
    return this.exporter.exportModel(sourceModeledElementId);
  }

  /** Cause the model contents to be exported from the source iModel and imported into the target iModel.
   * @param sourceModelId Import the contents of this model from the source IModelDb.
   * @param targetModelId Import into this model in the target IModelDb. The target model must exist prior to this call.
   * @param elementClassFullName Optional classFullName of an element subclass to limit import query against the source model.
   * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
   */
  public async processModelContents(sourceModelId: Id64String, targetModelId: Id64String, elementClassFullName: string = Element.classFullName): Promise<void> {
    await this.initialize();
    this.targetDb.models.getModel(targetModelId); // throws if Model does not exist
    this.context.remapElement(sourceModelId, targetModelId); // set remapping in case importModelContents is called directly
    return this.exporter.exportModelContents(sourceModelId, elementClassFullName);
  }

  /** Cause all sub-models that recursively descend from the specified Subject to be exported from the source iModel and imported into the target iModel. */
  private async processSubjectSubModels(sourceSubjectId: Id64String): Promise<void> {
    await this.initialize();
    // import DefinitionModels first
    const childDefinitionPartitionSql = `SELECT ECInstanceId FROM ${DefinitionPartition.classFullName} WHERE Parent.Id=:subjectId`;
    await this.sourceDb.withPreparedStatement(childDefinitionPartitionSql, async (statement: ECSqlStatement) => {
      statement.bindId("subjectId", sourceSubjectId);
      while (DbResult.BE_SQLITE_ROW === statement.step()) {
        await this.processModel(statement.getValue(0).getId());
      }
    });
    // import other partitions next
    const childPartitionSql = `SELECT ECInstanceId FROM ${InformationPartitionElement.classFullName} WHERE Parent.Id=:subjectId`;
    await this.sourceDb.withPreparedStatement(childPartitionSql, async (statement: ECSqlStatement) => {
      statement.bindId("subjectId", sourceSubjectId);
      while (DbResult.BE_SQLITE_ROW === statement.step()) {
        const modelId: Id64String = statement.getValue(0).getId();
        const model: Model = this.sourceDb.models.getModel(modelId);
        if (!(model instanceof DefinitionModel)) {
          await this.processModel(modelId);
        }
      }
    });
    // recurse into child Subjects
    const childSubjectSql = `SELECT ECInstanceId FROM ${Subject.classFullName} WHERE Parent.Id=:subjectId`;
    await this.sourceDb.withPreparedStatement(childSubjectSql, async (statement: ECSqlStatement) => {
      statement.bindId("subjectId", sourceSubjectId);
      while (DbResult.BE_SQLITE_ROW === statement.step()) {
        await this.processSubjectSubModels(statement.getValue(0).getId());
      }
    });
  }

  /** Transform the specified sourceModel into ModelProps for the target iModel.
   * @param sourceModel The Model from the source iModel to be transformed.
   * @param targetModeledElementId The transformed Model will *break down* or *detail* this Element in the target iModel.
   * @returns ModelProps for the target iModel.
   * @note A subclass can override this method to provide custom transform behavior.
   */
  public onTransformModel(sourceModel: Model, targetModeledElementId: Id64String): ModelProps {
    const targetModelProps: ModelProps = sourceModel.toJSON();
    // don't directly edit deep object since toJSON performs a shallow clone
    targetModelProps.modeledElement = { ...targetModelProps.modeledElement, id: targetModeledElementId };
    targetModelProps.id = targetModeledElementId;
    targetModelProps.parentModel = this.context.findTargetElementId(targetModelProps.parentModel!);
    return targetModelProps;
  }

  /** Import elements that were deferred in a prior pass.
   * @deprecated in 3.x. This method is no longer necessary since the transformer no longer needs to defer elements
   */
  public async processDeferredElements(_numRetries: number = 3): Promise<void> {}

  /** called at the end ([[finalizeTransformation]]) of a transformation,
   * updates the target scope element to say that transformation up through the
   * source's changeset has been performed.
   */
  private _updateTargetScopeVersion() {
    nodeAssert(this._targetScopeProvenanceProps);
    if (this._changeDataState === "has-changes") {
      this._targetScopeProvenanceProps.version = `${this.provenanceSourceDb.changeset.id};${this.provenanceSourceDb.changeset.index}`;
      this.provenanceDb.elements.updateAspect(this._targetScopeProvenanceProps);
    }
  }

  // FIXME: is this necessary when manually using lowlevel transform APIs?
  private finalizeTransformation() {
    this._updateTargetScopeVersion();

    if (this._partiallyCommittedEntities.size > 0) {
      Logger.logWarning(
        loggerCategory,
        [
          "The following elements were never fully resolved:",
          [...this._partiallyCommittedEntities.keys()].join(","),
          "This indicates that either some references were excluded from the transformation",
          "or the source has dangling references.",
        ].join("\n")
      );
      for (const partiallyCommittedElem of this._partiallyCommittedEntities.values()) {
        partiallyCommittedElem.forceComplete();
      }
    }

    // FIXME: make processAll have a try {} finally {} that cleans this up
    if (ChangeSummaryManager.isChangeCacheAttached(this.sourceDb))
      ChangeSummaryManager.detachChangeCache(this.sourceDb);
  }

  /** Imports all relationships that subclass from the specified base class.
   * @param baseRelClassFullName The specified base relationship class.
   * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
   */
  public async processRelationships(baseRelClassFullName: string): Promise<void> {
    await this.initialize();
    return this.exporter.exportRelationships(baseRelClassFullName);
  }

  /** Override of [IModelExportHandler.shouldExportRelationship]($transformer) that is called to determine if a [Relationship]($backend) should be exported.
   * @note Reaching this point means that the relationship has passed the standard exclusion checks in [IModelExporter]($transformer).
   */
  public override shouldExportRelationship(_sourceRelationship: Relationship): boolean { return true; }

  /** Override of [IModelExportHandler.onExportRelationship]($transformer) that imports a relationship into the target iModel when it is exported from the source iModel.
   * This override calls [[onTransformRelationship]] and then [IModelImporter.importRelationship]($transformer) to update the target iModel.
   */
  public override onExportRelationship(sourceRelationship: Relationship): void {
    const sourceFedGuid = queryElemFedGuid(this.sourceDb, sourceRelationship.sourceId);
    const targetFedGuid = queryElemFedGuid(this.sourceDb, sourceRelationship.targetId);
    const targetRelationshipProps: RelationshipProps = this.onTransformRelationship(sourceRelationship);
    const targetRelationshipInstanceId: Id64String = this.importer.importRelationship(targetRelationshipProps);
    if (!this._options.noProvenance && Id64.isValid(targetRelationshipInstanceId)) {
      let provenance: Parameters<typeof this.markLastProvenance>[0] | undefined
        = !this._options.forceExternalSourceAspectProvenance
        ? sourceFedGuid && targetFedGuid && `${sourceFedGuid}/${targetFedGuid}`
        : undefined;
      if (!provenance) {
        const aspectProps = this.initRelationshipProvenance(sourceRelationship, targetRelationshipInstanceId);
        if (undefined === aspectProps.id) {
          aspectProps.id = this.provenanceDb.elements.insertAspect(aspectProps);
        }
        assert(aspectProps.id !== undefined);
        provenance = aspectProps as MarkRequired<ExternalSourceAspectProps, "id">;
      }
      this.markLastProvenance(provenance, { isRelationship: true });
    }
  }

  // FIXME: need to check if the class was remapped and use that id instead
  // is this really the best way to get class id? shouldn't we cache it somewhere?
  // NOTE: maybe if we lower remapElementClass into here, we can use that
  private _getRelClassId(db: IModelDb, classFullName: string): Id64String {
    // is it better to use un-cached `SELECT (ONLY ${classFullName})`?
    return db.withPreparedStatement(`
      SELECT c.ECInstanceId
      FROM ECDbMeta.ECClassDef c
      JOIN ECDbMeta.ECSchemaDef s ON c.Schema.Id=s.ECInstanceId
      WHERE s.Name=? AND c.Name=?
    `, (stmt) => {
        const [schemaName, className] = classFullName.split(".");
        stmt.bindString(1, schemaName);
        stmt.bindString(2, className);
        if (stmt.step() === DbResult.BE_SQLITE_ROW)
          return stmt.getValue(0).getId();
        assert(false, "relationship was not found");
      }
    );
  }

  /** Override of [IModelExportHandler.onDeleteRelationship]($transformer) that is called when [IModelExporter]($transformer) detects that a [Relationship]($backend) has been deleted from the source iModel.
   * This override propagates the delete to the target iModel via [IModelImporter.deleteRelationship]($transformer).
   */
  public override onDeleteRelationship(sourceRelInstanceId: Id64String): void {
    nodeAssert(this._deletedSourceRelationshipData, "should be defined at initialization by now");
    const deletedRelData = this._deletedSourceRelationshipData.get(sourceRelInstanceId);
    if (!deletedRelData) {
      Logger.logWarning(loggerCategory, "tried to delete a relationship that wasn't in change data");
      return;
    }
    const targetRelClassId = this._getRelClassId(this.targetDb, deletedRelData.classFullName);
    // NOTE: if no remapping, could store the sourceRel class name earlier and reuse it instead of add to query
    // TODO: name this query
    const sql = `
      SELECT SourceECInstanceId, TargetECInstanceId, erte.ECClassId
      FROM BisCore.ElementRefersToElements erte
      JOIN BisCore.Element se ON se.ECInstanceId=SourceECInstanceId
      JOIN BisCore.Element te ON te.ECInstanceId=TargetECInstanceId
      WHERE se.FederationGuid=:sourceFedGuid
        AND te.FederationGuid=:targetFedGuid
        AND erte.ECClassId=:relClassId
    `;
    this.targetDb.withPreparedStatement(sql, (statement: ECSqlStatement): void => {
      statement.bindGuid("sourceFedGuid", deletedRelData.sourceFedGuid);
      statement.bindGuid("targetFedGuid", deletedRelData.targetFedGuid);
      statement.bindId("relClassId", targetRelClassId);
      if (DbResult.BE_SQLITE_ROW === statement.step()) {
        const sourceId = statement.getValue(0).getId();
        const targetId = statement.getValue(1).getId();
        const targetRelClassFullName = statement.getValue(2).getClassNameForClassId();
        // FIXME: make importer.deleteRelationship not need full props
        const targetRelationship = this.targetDb.relationships.tryGetInstance(targetRelClassFullName, { sourceId, targetId });
        if (targetRelationship) {
          this.importer.deleteRelationship(targetRelationship.toJSON());
        }
        // FIXME: restore in ESA compatible method
        //this.targetDb.elements.deleteAspect(statement.getValue(0).getId());
      }
    });
  }

  private _yieldManager = new YieldManager();

  /** Detect Relationship deletes using ExternalSourceAspects in the target iModel and a *brute force* comparison against relationships in the source iModel.
   * @deprecated
   * @see processChanges
   * @note This method is called from [[processAll]] and is not needed by [[processChanges]], so it only needs to be called directly when processing a subset of an iModel.
   * @throws [[IModelError]] If the required provenance information is not available to detect deletes.
   */
  public async detectRelationshipDeletes(): Promise<void> {
    if (this._options.isReverseSynchronization) {
      throw new IModelError(IModelStatus.BadRequest, "Cannot detect deletes when isReverseSynchronization=true");
    }
    const aspectDeleteIds: Id64String[] = [];
    const sql = `
      SELECT ECInstanceId, Identifier, JsonProperties
      FROM ${ExternalSourceAspect.classFullName} aspect
      WHERE aspect.Scope.Id=:scopeId
        AND aspect.Kind=:kind
    `;
    await this.targetDb.withPreparedStatement(sql, async (statement: ECSqlStatement) => {
      statement.bindId("scopeId", this.targetScopeElementId);
      statement.bindString("kind", ExternalSourceAspect.Kind.Relationship);
      while (DbResult.BE_SQLITE_ROW === statement.step()) {
        const sourceRelInstanceId: Id64String = Id64.fromJSON(statement.getValue(1).getString());
        if (undefined === this.sourceDb.relationships.tryGetInstanceProps(ElementRefersToElements.classFullName, sourceRelInstanceId)) {
          const json: any = JSON.parse(statement.getValue(2).getString());
          if (undefined !== json.targetRelInstanceId) {
            const targetRelationship: Relationship = this.targetDb.relationships.getInstance(ElementRefersToElements.classFullName, json.targetRelInstanceId);
            this.importer.deleteRelationship(targetRelationship.toJSON());
          }
          aspectDeleteIds.push(statement.getValue(0).getId());
        }
        await this._yieldManager.allowYield();
      }
    });
    this.targetDb.elements.deleteAspect(aspectDeleteIds);
  }

  /** Transform the specified sourceRelationship into RelationshipProps for the target iModel.
   * @param sourceRelationship The Relationship from the source iModel to be transformed.
   * @returns RelationshipProps for the target iModel.
   * @note A subclass can override this method to provide custom transform behavior.
   */
  protected onTransformRelationship(sourceRelationship: Relationship): RelationshipProps {
    const targetRelationshipProps: RelationshipProps = sourceRelationship.toJSON();
    targetRelationshipProps.sourceId = this.context.findTargetElementId(sourceRelationship.sourceId);
    targetRelationshipProps.targetId = this.context.findTargetElementId(sourceRelationship.targetId);
    // TODO: move to cloneRelationship in IModelCloneContext
    sourceRelationship.forEachProperty((propertyName: string, propertyMetaData: PropertyMetaData) => {
      if ((PrimitiveTypeCode.Long === propertyMetaData.primitiveType) && ("Id" === propertyMetaData.extendedType)) {
        (targetRelationshipProps as any)[propertyName] = this.context.findTargetElementId(sourceRelationship.asAny[propertyName]);
      }
    });
    return targetRelationshipProps;
  }

  /** Override of [IModelExportHandler.onExportElementUniqueAspect]($transformer) that imports an ElementUniqueAspect into the target iModel when it is exported from the source iModel.
   * This override calls [[onTransformElementAspect]] and then [IModelImporter.importElementUniqueAspect]($transformer) to update the target iModel.
   */
  public override onExportElementUniqueAspect(sourceAspect: ElementUniqueAspect): void {
    const targetElementId: Id64String = this.context.findTargetElementId(sourceAspect.element.id);
    const targetAspectProps = this.onTransformElementAspect(sourceAspect, targetElementId);
    this.collectUnmappedReferences(sourceAspect);
    const targetId = this.importer.importElementUniqueAspect(targetAspectProps);
    this.context.remapElementAspect(sourceAspect.id, targetId);
    this.resolvePendingReferences(sourceAspect);
  }

  /** Override of [IModelExportHandler.onExportElementMultiAspects]($transformer) that imports ElementMultiAspects into the target iModel when they are exported from the source iModel.
   * This override calls [[onTransformElementAspect]] for each ElementMultiAspect and then [IModelImporter.importElementMultiAspects]($transformer) to update the target iModel.
   * @note ElementMultiAspects are handled as a group to make it easier to differentiate between insert, update, and delete.
   */
  public override onExportElementMultiAspects(sourceAspects: ElementMultiAspect[]): void {
    const targetElementId: Id64String = this.context.findTargetElementId(sourceAspects[0].element.id);
    // Transform source ElementMultiAspects into target ElementAspectProps
    const targetAspectPropsArray = sourceAspects.map((srcA) => this.onTransformElementAspect(srcA, targetElementId));
    sourceAspects.forEach((a) => this.collectUnmappedReferences(a));
    // const targetAspectsToImport = targetAspectPropsArray.filter((targetAspect, i) => hasEntityChanged(sourceAspects[i], targetAspect));
    const targetIds = this.importer.importElementMultiAspects(targetAspectPropsArray, (a) => {
      const isExternalSourceAspectFromTransformer = a instanceof ExternalSourceAspect && a.scope?.id === this.targetScopeElementId;
      return !this._options.includeSourceProvenance || !isExternalSourceAspectFromTransformer;
    });
    for (let i = 0; i < targetIds.length; ++i) {
      this.context.remapElementAspect(sourceAspects[i].id, targetIds[i]);
      this.resolvePendingReferences(sourceAspects[i]);
    }
  }

  /** Transform the specified sourceElementAspect into ElementAspectProps for the target iModel.
   * @param sourceElementAspect The ElementAspect from the source iModel to be transformed.
   * @param _targetElementId The ElementId of the target Element that will own the ElementAspects after transformation.
   * @returns ElementAspectProps for the target iModel.
   * @note A subclass can override this method to provide custom transform behavior.
   */
  protected onTransformElementAspect(sourceElementAspect: ElementAspect, _targetElementId: Id64String): ElementAspectProps {
    const targetElementAspectProps = this.context.cloneElementAspect(sourceElementAspect);
    return targetElementAspectProps;
  }

  /** The directory where schemas will be exported, a random temporary directory */
  protected _schemaExportDir: string = path.join(KnownLocations.tmpdir, Guid.createValue());

  /** Override of [IModelExportHandler.shouldExportSchema]($transformer) that is called to determine if a schema should be exported
   * @note the default behavior doesn't import schemas older than those already in the target
   */
  public override shouldExportSchema(schemaKey: ECSchemaMetaData.SchemaKey): boolean {
    const versionInTarget = this.targetDb.querySchemaVersion(schemaKey.name);
    if (versionInTarget === undefined)
      return true;
    return Semver.gt(`${schemaKey.version.read}.${schemaKey.version.write}.${schemaKey.version.minor}`, Schema.toSemverString(versionInTarget));
  }

  private _longNamedSchemasMap = new Map<string, string>();

  /** Override of [IModelExportHandler.onExportSchema]($transformer) that serializes a schema to disk for [[processSchemas]] to import into
   * the target iModel when it is exported from the source iModel.
   * @returns {Promise<ExportSchemaResult>} Although the type is possibly void for backwards compatibility of subclasses,
   *                                        `IModelTransformer.onExportSchema` always returns an[[IModelExportHandler.ExportSchemaResult]]
   *                                        with a defined `schemaPath` property, for subclasses to know where the schema was written.
   *                                        Schemas are *not* guaranteed to be written to [[IModelTransformer._schemaExportDir]] by a
   *                                        known pattern derivable from the schema's name, so you must use this to find it.
   */
  public override async onExportSchema(schema: ECSchemaMetaData.Schema): Promise<void | ExportSchemaResult> {
    const ext = ".ecschema.xml";
    let schemaFileName = schema.name + ext;
    // many file systems have a max file-name/path-segment size of 255, so we workaround that on all systems
    const systemMaxPathSegmentSize = 255;
    if (schemaFileName.length > systemMaxPathSegmentSize) {
      // this name should be well under 255 bytes
      // ( 100 + (Number.MAX_SAFE_INTEGER.toString().length = 16) + (ext.length = 13) ) = 129 which is less than 255
      // You'd have to be past 2**53-1 (Number.MAX_SAFE_INTEGER) long named schemas in order to hit decimal formatting,
      // and that's on the scale of at least petabytes. `Map.prototype.size` shouldn't return floating points, and even
      // if they do they're in scientific notation, size bound and contain no invalid windows path chars
      schemaFileName = `${schema.name.slice(0, 100)}${this._longNamedSchemasMap.size}${ext}`;
      nodeAssert(schemaFileName.length <= systemMaxPathSegmentSize, "Schema name was still long. This is a bug.");
      this._longNamedSchemasMap.set(schema.name, schemaFileName);
    }
    this.sourceDb.nativeDb.exportSchema(schema.name, this._schemaExportDir, schemaFileName);
    return { schemaPath: path.join(this._schemaExportDir, schemaFileName) };
  }

  private _makeLongNameResolvingSchemaCtx(): ECSchemaXmlContext {
    const result = new ECSchemaXmlContext();
    result.setSchemaLocater((key) => {
      const match = this._longNamedSchemasMap.get(key.name);
      if (match !== undefined)
        return path.join(this._schemaExportDir, match);
      return undefined;
    });
    return result;
  }

  /** Cause all schemas to be exported from the source iModel and imported into the target iModel.
   * @note For performance reasons, it is recommended that [IModelDb.saveChanges]($backend) be called after `processSchemas` is complete.
   * It is more efficient to process *data* changes after the schema changes have been saved.
   */
  public async processSchemas(): Promise<void> {
    this.events.emit(TransformerEvent.beginProcessSchemas);
    // we do not need to initialize for this since no entities are exported
    try {
      IModelJsFs.mkdirSync(this._schemaExportDir);
      this._longNamedSchemasMap.clear();
      await this.exporter.exportSchemas();
      const exportedSchemaFiles = IModelJsFs.readdirSync(this._schemaExportDir);
      if (exportedSchemaFiles.length === 0)
        return;
      const schemaFullPaths = exportedSchemaFiles.map((s) => path.join(this._schemaExportDir, s));
      const maybeLongNameResolvingSchemaCtx = this._longNamedSchemasMap.size > 0
        ? this._makeLongNameResolvingSchemaCtx()
        : undefined;
      return await this.targetDb.importSchemas(schemaFullPaths, { ecSchemaXmlContext: maybeLongNameResolvingSchemaCtx });
    } finally {
      IModelJsFs.removeSync(this._schemaExportDir);
      this._longNamedSchemasMap.clear();
      this.events.emit(TransformerEvent.endProcessSchemas);
    }
  }

  /** Cause all fonts to be exported from the source iModel and imported into the target iModel.
 * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
 */
  public async processFonts(): Promise<void> {
    // we do not need to initialize for this since no entities are exported
    await this.initialize();
    return this.exporter.exportFonts();
  }

  /** Override of [IModelExportHandler.onExportFont]($transformer) that imports a font into the target iModel when it is exported from the source iModel. */
  public override onExportFont(font: FontProps, _isUpdate: boolean | undefined): void {
    this.context.importFont(font.id);
  }

  /** Cause all CodeSpecs to be exported from the source iModel and imported into the target iModel.
   * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
   */
  public async processCodeSpecs(): Promise<void> {
    await this.initialize();
    return this.exporter.exportCodeSpecs();
  }

  /** Cause a single CodeSpec to be exported from the source iModel and imported into the target iModel.
   * @note This method is called from [[processChanges]] and [[processAll]], so it only needs to be called directly when processing a subset of an iModel.
   */
  public async processCodeSpec(codeSpecName: string): Promise<void> {
    await this.initialize();
    return this.exporter.exportCodeSpecByName(codeSpecName);
  }

  /** Override of [IModelExportHandler.shouldExportCodeSpec]($transformer) that is called to determine if a CodeSpec should be exported from the source iModel.
   * @note Reaching this point means that the CodeSpec has passed the standard exclusion checks in [IModelExporter]($transformer).
   */
  public override shouldExportCodeSpec(_sourceCodeSpec: CodeSpec): boolean { return true; }

  /** Override of [IModelExportHandler.onExportCodeSpec]($transformer) that imports a CodeSpec into the target iModel when it is exported from the source iModel. */
  public override onExportCodeSpec(sourceCodeSpec: CodeSpec): void {
    this.context.importCodeSpec(sourceCodeSpec.id);
  }

  /** Recursively import all Elements and sub-Models that descend from the specified Subject */
  public async processSubject(sourceSubjectId: Id64String, targetSubjectId: Id64String): Promise<void> {
    await this.initialize();
    this.sourceDb.elements.getElement(sourceSubjectId, Subject); // throws if sourceSubjectId is not a Subject
    this.targetDb.elements.getElement(targetSubjectId, Subject); // throws if targetSubjectId is not a Subject
    this.context.remapElement(sourceSubjectId, targetSubjectId);
    await this.processChildElements(sourceSubjectId);
    await this.processSubjectSubModels(sourceSubjectId);
    return this.processDeferredElements(); // eslint-disable-line deprecation/deprecation
  }

  /** state to prevent reinitialization, @see [[initialize]] */
  private _initialized = false;

  /** length === 0 when _changeDataState = "no-change", length > 0 means "has-changes", otherwise undefined  */
  private _changeSummaryIds?: Id64String[] = undefined;
  private _changeDataState: "uninited" | "has-changes" | "no-changes" | "unconnected" = "uninited";

  /**
   * Initialize prerequisites of processing, you must initialize with an [[InitFromExternalSourceAspectsArgs]] if you
   * are intending process changes, but prefer using [[processChanges]]
   * Called by all `process*` functions implicitly.
   * Overriders must call `super.initialize()` first
   */
  public async initialize(args?: InitArgs): Promise<void> {
    if (this._initialized)
      return;

    await this.context.initialize();
    await this._tryInitChangesetData(args);
    // eslint-disable-next-line deprecation/deprecation
    await this.initFromExternalSourceAspects(args);

    this._initialized = true;
  }

  private async _tryInitChangesetData(args?: InitArgs) {
    if (!args || this.sourceDb.iTwinId === undefined) {
      this._changeDataState = "unconnected";
      return;
    }

    const noChanges = this._targetScopeVersion.index === this.sourceDb.changeset.index;
    if (noChanges) {
      this._changeDataState = "no-changes";
      this._changeSummaryIds = [];
      return;
    }

    // NOTE: that we do NOT download the changesummary for the last transformed version, we want
    // to ignore those already processed changes
    const startChangesetIndexOrId = args?.startChangesetId ?? this._targetScopeVersion.index + 1;
    const endChangesetId = this.sourceDb.changeset.id;
    const [startChangesetIndex, endChangesetIndex] = await Promise.all(
      ([startChangesetIndexOrId, endChangesetId])
        .map(async (indexOrId) => typeof indexOrId === "number"
          ? indexOrId
          : IModelHost.hubAccess
            .queryChangeset({
              iModelId: this.sourceDb.iModelId,
              changeset: { id: indexOrId },
              accessToken: args.accessToken,
            })
            .then((changeset) => changeset.index)
        )
    );

    // FIXME: do we need the startChangesetId?
    this._changeSummaryIds = await ChangeSummaryManager.createChangeSummaries({
      accessToken: args.accessToken,
      iModelId: this.sourceDb.iModelId,
      iTwinId: this.sourceDb.iTwinId,
      range: { first: startChangesetIndex, end: endChangesetIndex },
    });

    ChangeSummaryManager.attachChangeCache(this.sourceDb);
    this._changeDataState = "has-changes";
  }

  /** Export everything from the source iModel and import the transformed entities into the target iModel.
 * @note [[processSchemas]] is not called automatically since the target iModel may want a different collection of schemas.
 */
  public async processAll(): Promise<void> {
    this.events.emit(TransformerEvent.beginProcessAll);
    this.logSettings();
    this.initScopeProvenance();
    await this.initialize();
    await this.exporter.exportCodeSpecs();
    await this.exporter.exportFonts();
    // The RepositoryModel and root Subject of the target iModel should not be transformed.
    await this.exporter.exportChildElements(IModel.rootSubjectId); // start below the root Subject
    await this.exporter.exportModelContents(IModel.repositoryModelId, Element.classFullName, true); // after the Subject hierarchy, process the other elements of the RepositoryModel
    await this.exporter.exportSubModels(IModel.repositoryModelId); // start below the RepositoryModel
    await this.exporter.exportRelationships(ElementRefersToElements.classFullName);
    await this.processDeferredElements(); // eslint-disable-line deprecation/deprecation
    if (this.shouldDetectDeletes()) {
      await this.detectElementDeletes();
      await this.detectRelationshipDeletes();
    }

    if (this._options.optimizeGeometry)
      this.importer.optimizeGeometry(this._options.optimizeGeometry);

    this.importer.computeProjectExtents();
    this.finalizeTransformation();
    this.events.emit(TransformerEvent.endProcessAll);
  }

  /** previous provenance, either a federation guid, a `${sourceFedGuid}/${targetFedGuid}` pair, or required aspect props */
  private _lastProvenanceEntityInfo: string | LastProvenanceEntityInfo = nullLastProvenanceEntityInfo;

  private markLastProvenance(sourceAspect: string | MarkRequired<ExternalSourceAspectProps, "id">, { isRelationship = false }) {
    this._lastProvenanceEntityInfo
      = typeof sourceAspect === "string"
      ? sourceAspect
      : {
        entityId: sourceAspect.element.id,
        aspectId: sourceAspect.id,
        aspectVersion: sourceAspect.version ?? "",
        aspectKind: isRelationship ? ExternalSourceAspect.Kind.Relationship : ExternalSourceAspect.Kind.Element,
      };
  }

  /** @internal the name of the table where javascript state of the transformer is serialized in transformer state dumps */
  public static readonly jsStateTable = "TransformerJsState";

  /** @internal the name of the table where the target state heuristics is serialized in transformer state dumps */
  public static readonly lastProvenanceEntityInfoTable = "LastProvenanceEntityInfo";

  /**
   * Load the state of the active transformation from an open SQLiteDb
   * You can override this if you'd like to load from custom tables in the resumable dump state, but you should call
   * this super implementation
   * @note the SQLiteDb must be open
   */
  protected loadStateFromDb(db: SQLiteDb): void {
    const lastProvenanceEntityInfo: IModelTransformer["_lastProvenanceEntityInfo"] = db.withSqliteStatement(
      `SELECT entityId, aspectId, aspectVersion, aspectKind FROM ${IModelTransformer.lastProvenanceEntityInfoTable}`,
      (stmt) => {
        if (DbResult.BE_SQLITE_ROW !== stmt.step())
          throw Error(
            "expected row when getting lastProvenanceEntityId from target state table"
          );
        const entityId = stmt.getValueString(0);
        const isGuidOrGuidPair = entityId.includes('-')
        return isGuidOrGuidPair
          ? entityId
          : {
            entityId,
            aspectId: stmt.getValueString(1),
            aspectVersion: stmt.getValueString(2),
            aspectKind: stmt.getValueString(3) as ExternalSourceAspect.Kind,
          };
      }
    );

    /*
    // TODO: maybe save transformer state resumption state based on target changset and require calls
    // to saveChanges
    if () {
      const [sourceFedGuid, targetFedGuid, relClassFullName] = lastProvenanceEntityInfo.split("/");
      const isRelProvenance = targetFedGuid !== undefined;
      const instanceId = isRelProvenance
        ? this.targetDb.elements.getElement({federationGuid: sourceFedGuid})
        : "";
      //const classId =
      if (isRelProvenance) {
      }
    }
    */

    const targetHasCorrectLastProvenance =
      typeof lastProvenanceEntityInfo === "string" ||
      // ignore provenance check if it's null since we can't bind those ids
      !Id64.isValidId64(lastProvenanceEntityInfo.entityId) ||
      !Id64.isValidId64(lastProvenanceEntityInfo.aspectId) ||
      this.provenanceDb.withPreparedStatement(`
        SELECT Version FROM ${ExternalSourceAspect.classFullName}
        WHERE Scope.Id=:scopeId
          AND ECInstanceId=:aspectId
          AND Kind=:kind
          AND Element.Id=:entityId
      `,
      (statement: ECSqlStatement): boolean => {
        statement.bindId("scopeId", this.targetScopeElementId);
        statement.bindId("aspectId", lastProvenanceEntityInfo.aspectId);
        statement.bindString("kind", lastProvenanceEntityInfo.aspectKind);
        statement.bindId("entityId", lastProvenanceEntityInfo.entityId);
        const stepResult = statement.step();
        switch (stepResult) {
          case DbResult.BE_SQLITE_ROW:
            const version = statement.getValue(0).getString();
            return version === lastProvenanceEntityInfo.aspectVersion;
          case DbResult.BE_SQLITE_DONE:
            return false;
          default:
            throw new IModelError(IModelStatus.SQLiteError, `got sql error ${stepResult}`);
        }
      });

    if (!targetHasCorrectLastProvenance)
      throw Error([
        "Target for resuming from does not have the expected provenance ",
        "from the target that the resume state was made with",
      ].join("\n"));
    this._lastProvenanceEntityInfo = lastProvenanceEntityInfo;

    const state = db.withSqliteStatement(`SELECT data FROM ${IModelTransformer.jsStateTable}`, (stmt) => {
      if (DbResult.BE_SQLITE_ROW !== stmt.step())
        throw Error("expected row when getting data from js state table");
      return JSON.parse(stmt.getValueString(0)) as TransformationJsonState;
    });
    if (state.transformerClass !== this.constructor.name)
      throw Error("resuming from a differently named transformer class, it is not necessarily valid to resume with a different transformer class");
    // force assign to readonly options since we do not know how the transformer subclass takes options to pass to the superclass
    (this as any)._options = state.options;
    this.context.loadStateFromDb(db);
    this.importer.loadStateFromJson(state.importerState);
    this.exporter.loadStateFromJson(state.exporterState);
    this._elementsWithExplicitlyTrackedProvenance = CompressedId64Set.decompressSet(state.explicitlyTrackedElements);
    this.loadAdditionalStateJson(state.additionalState);
  }

  /**
   * Return a new transformer instance with the same remappings state as saved from a previous [[IModelTransformer.saveStateToFile]] call.
   * This allows you to "resume" an iModel transformation, you will have to call [[IModelTransformer.processChanges]]/[[IModelTransformer.processAll]]
   * again but the remapping state will cause already mapped elements to be skipped.
   * To "resume" an iModel Transformation you need:
   * - the sourceDb at the same changeset
   * - the same targetDb in the state in which it was before
   * @param statePath the path to the serialized state of the transformer, use [[IModelTransformer.saveStateToFile]] to get this from an existing transformer instance
   * @param constructorArgs remaining arguments that you would normally pass to the Transformer subclass you are using, usually (sourceDb, targetDb)
   * @note custom transformers with custom state may need to override this method in order to handle loading their own custom state somewhere
   */
  public static resumeTransformation<SubClass extends new(...a: any[]) => IModelTransformer = typeof IModelTransformer>(
    this: SubClass,
    statePath: string,
    ...constructorArgs: ConstructorParameters<SubClass>
  ): InstanceType<SubClass> {
    const transformer = new this(...constructorArgs);
    const db = new SQLiteDb();
    db.openDb(statePath, OpenMode.Readonly);
    try {
      transformer.loadStateFromDb(db);
    } finally {
      db.closeDb();
    }
    return transformer as InstanceType<SubClass>;
  }

  /**
   * You may override this to store arbitrary json state in a transformer state dump, useful for some resumptions
   * @see [[IModelTransformer.saveStateToFile]]
   */
  protected getAdditionalStateJson(): any {
    return {};
  }

  /**
   * You may override this to load arbitrary json state in a transformer state dump, useful for some resumptions
   * @see [[IModelTransformer.loadStateFromFile]]
   */
  protected loadAdditionalStateJson(_additionalState: any): void {}

  /**
   * Save the state of the active transformation to an open SQLiteDb
   * You can override this if you'd like to write custom tables to the resumable dump state, but you should call
   * this super implementation
   * @note the SQLiteDb must be open
   */
  protected saveStateToDb(db: SQLiteDb): void {
    const jsonState: TransformationJsonState = {
      transformerClass: this.constructor.name,
      options: this._options,
      explicitlyTrackedElements: CompressedId64Set.compressSet(this._elementsWithExplicitlyTrackedProvenance),
      importerState: this.importer.saveStateToJson(),
      exporterState: this.exporter.saveStateToJson(),
      additionalState: this.getAdditionalStateJson(),
    };

    this.context.saveStateToDb(db);
    if (DbResult.BE_SQLITE_DONE !== db.executeSQL(`CREATE TABLE ${IModelTransformer.jsStateTable} (data TEXT)`))
      throw Error("Failed to create the js state table in the state database");

    if (DbResult.BE_SQLITE_DONE !== db.executeSQL(`
      CREATE TABLE ${IModelTransformer.lastProvenanceEntityInfoTable} (
        -- either the invalid id for null provenance state, federation guid (or pair for rels) of the entity, or a hex element id
        entityId TEXT,
        -- the following are only valid if the above entityId is a hex id representation
        aspectId TEXT,
        aspectVersion TEXT,
        aspectKind TEXT
      )
    `))
      throw Error("Failed to create the target state table in the state database");

    db.saveChanges();
    db.withSqliteStatement(
      `INSERT INTO ${IModelTransformer.jsStateTable} (data) VALUES (?)`,
      (stmt) => {
        stmt.bindString(1, JSON.stringify(jsonState));
        if (DbResult.BE_SQLITE_DONE !== stmt.step())
          throw Error("Failed to insert options into the state database");
      });

    db.withSqliteStatement(
      `INSERT INTO ${IModelTransformer.lastProvenanceEntityInfoTable} (entityId, aspectId, aspectVersion, aspectKind) VALUES (?,?,?,?)`,
      (stmt) => {
        const lastProvenanceEntityInfo = this._lastProvenanceEntityInfo as LastProvenanceEntityInfo;
        stmt.bindString(1, lastProvenanceEntityInfo?.entityId ?? this._lastProvenanceEntityInfo as string);
        stmt.bindString(2, lastProvenanceEntityInfo?.aspectId ?? "");
        stmt.bindString(3, lastProvenanceEntityInfo?.aspectVersion ?? "");
        stmt.bindString(4, lastProvenanceEntityInfo?.aspectKind ?? "");
        if (DbResult.BE_SQLITE_DONE !== stmt.step())
          throw Error("Failed to insert options into the state database");
      });

    db.saveChanges();
  }

  /**
   * Save the state of the active transformation to a file path, if a file at the path already exists, it will be overwritten
   * This state can be used by [[IModelTransformer.resumeTransformation]] to resume a transformation from this point.
   * The serialization format is a custom sqlite database.
   * @note custom transformers with custom state may override [[IModelTransformer.saveStateToDb]] or [[IModelTransformer.getAdditionalStateJson]]
   *       and [[IModelTransformer.loadStateFromDb]] (with a super call) or [[IModelTransformer.loadAdditionalStateJson]]
   *       if they have custom state that needs to be stored with
   *       potentially inside the same sqlite file in separate tables
   */
  public saveStateToFile(nativeStatePath: string): void {
    const db = new SQLiteDb();
    if (IModelJsFs.existsSync(nativeStatePath))
      IModelJsFs.unlinkSync(nativeStatePath);
    db.createDb(nativeStatePath);
    try {
      this.saveStateToDb(db);
      db.saveChanges();
    } finally {
      db.closeDb();
    }
  }

  /** Export changes from the source iModel and import the transformed entities into the target iModel.
   * Inserts, updates, and deletes are determined by inspecting the changeset(s).
   * @param accessToken A valid access token string
   * @param startChangesetId Include changes from this changeset up through and including the current changeset.
   * If this parameter is not provided, then just the current changeset will be exported.
   * @note To form a range of versions to process, set `startChangesetId` for the start (inclusive) of the desired range and open the source iModel as of the end (inclusive) of the desired range.
   */
  public async processChanges(accessToken: AccessToken, startChangesetId?: string): Promise<void> {
    this.events.emit(TransformerEvent.beginProcessChanges, startChangesetId);
    this.logSettings();
    this.initScopeProvenance();
    await this.initialize({ accessToken, startChangesetId });
    await this.exporter.exportChanges(accessToken, startChangesetId);
    await this.processDeferredElements(); // eslint-disable-line deprecation/deprecation

    if (this._options.optimizeGeometry)
      this.importer.optimizeGeometry(this._options.optimizeGeometry);

    this.importer.computeProjectExtents();
    this.finalizeTransformation();
    this.events.emit(TransformerEvent.endProcessChanges);
  }

  /** Combine an array source elements into a single target element.
   * All source and target elements must be created before calling this method.
   * The "combine" operation is a simple remap and no properties from the source elements will be exported into the target.
   * Provenance will be explicitly tracked by ExternalSourceAspects for all sourceElements.
   */
  public combineElements(sourceElementIds: Id64Array, targetElementId: Id64String) {
    for (const elementId of sourceElementIds) {
      this.context.remapElement(elementId, targetElementId);
      this._elementsWithExplicitlyTrackedProvenance.add(elementId);
    }
  }
}

/** @internal the json part of a transformation's state */
interface TransformationJsonState {
  transformerClass: string;
  options: IModelTransformOptions;
  importerState: IModelImporterState;
  exporterState: IModelExporterState;
  explicitlyTrackedElements: CompressedId64Set;
  additionalState?: any;
}

/** IModelTransformer that clones the contents of a template model.
 * @beta
 */
export class TemplateModelCloner extends IModelTransformer {
  /** The Placement to apply to the template. */
  private _transform3d?: Transform;
  /** Accumulates the mapping of sourceElementIds to targetElementIds from the elements in the template model that were cloned. */
  private _sourceIdToTargetIdMap?: Map<Id64String, Id64String>;
  /** Construct a new TemplateModelCloner
   * @param sourceDb The source IModelDb that contains the templates to clone
   * @param targetDb Optionally specify the target IModelDb where the cloned template will be inserted.
   *                 Typically this is left unspecified, and the default is to use the sourceDb as the target
   * @note The expectation is that the template definitions are within the same iModel where instances will be placed.
   */
  public constructor(sourceDb: IModelDb, targetDb: IModelDb = sourceDb) {
    const target = new IModelImporter(targetDb, {
      autoExtendProjectExtents: false, // autoExtendProjectExtents is intended for transformation service use cases, not template --> instance cloning
    });
    super(sourceDb, target, { noProvenance: true }); // WIP: need to decide the proper way to handle provenance
  }
  /** Place a template from the sourceDb at the specified placement in the target model within the targetDb.
   * @param sourceTemplateModelId The Id of the template model in the sourceDb
   * @param targetModelId The Id of the target model (must be a subclass of GeometricModel3d) where the cloned component will be inserted.
   * @param placement The placement for the cloned component.
   * @note *Required References* like the SpatialCategory must be remapped before calling this method.
   * @returns The mapping of sourceElementIds from the template model to the instantiated targetElementIds in the targetDb in case further processing is required.
   */
  public async placeTemplate3d(sourceTemplateModelId: Id64String, targetModelId: Id64String, placement: Placement3d): Promise<Map<Id64String, Id64String>> {
    this.context.remapElement(sourceTemplateModelId, targetModelId);
    this._transform3d = Transform.createOriginAndMatrix(placement.origin, placement.angles.toMatrix3d());
    this._sourceIdToTargetIdMap = new Map<Id64String, Id64String>();
    await this.exporter.exportModelContents(sourceTemplateModelId);
    // Note: the source --> target mapping was needed during the template model cloning phase (remapping parent/child, for example), but needs to be reset afterwards
    for (const sourceElementId of this._sourceIdToTargetIdMap.keys()) {
      const targetElementId = this.context.findTargetElementId(sourceElementId);
      this._sourceIdToTargetIdMap.set(sourceElementId, targetElementId);
      this.context.removeElement(sourceElementId); // clear the underlying native remapping context for the next clone operation
    }
    return this._sourceIdToTargetIdMap; // return the sourceElementId -> targetElementId Map in case further post-processing is required.
  }
  /** Place a template from the sourceDb at the specified placement in the target model within the targetDb.
   * @param sourceTemplateModelId The Id of the template model in the sourceDb
   * @param targetModelId The Id of the target model (must be a subclass of GeometricModel2d) where the cloned component will be inserted.
   * @param placement The placement for the cloned component.
   * @note *Required References* like the DrawingCategory must be remapped before calling this method.
   * @returns The mapping of sourceElementIds from the template model to the instantiated targetElementIds in the targetDb in case further processing is required.
   */
  public async placeTemplate2d(sourceTemplateModelId: Id64String, targetModelId: Id64String, placement: Placement2d): Promise<Map<Id64String, Id64String>> {
    this.context.remapElement(sourceTemplateModelId, targetModelId);
    this._transform3d = Transform.createOriginAndMatrix(Point3d.createFrom(placement.origin), placement.rotation);
    this._sourceIdToTargetIdMap = new Map<Id64String, Id64String>();
    await this.exporter.exportModelContents(sourceTemplateModelId);
    // Note: the source --> target mapping was needed during the template model cloning phase (remapping parent/child, for example), but needs to be reset afterwards
    for (const sourceElementId of this._sourceIdToTargetIdMap.keys()) {
      const targetElementId = this.context.findTargetElementId(sourceElementId);
      this._sourceIdToTargetIdMap.set(sourceElementId, targetElementId);
      this.context.removeElement(sourceElementId); // clear the underlying native remapping context for the next clone operation
    }
    return this._sourceIdToTargetIdMap; // return the sourceElementId -> targetElementId Map in case further post-processing is required.
  }
  /** Cloning from a template requires this override of onTransformElement. */
  public override onTransformElement(sourceElement: Element): ElementProps {
    const referenceIds: Id64Set = sourceElement.getReferenceIds();
    referenceIds.forEach((referenceId: Id64String) => {
      if (Id64.invalid === this.context.findTargetElementId(referenceId)) {
        if (this.context.isBetweenIModels) {
          throw new IModelError(IModelStatus.BadRequest, `Remapping for source dependency ${referenceId} not found for target iModel`);
        } else {
          const definitionElement = this.sourceDb.elements.tryGetElement<DefinitionElement>(referenceId, DefinitionElement);
          if (definitionElement && !(definitionElement instanceof RecipeDefinitionElement)) {
            this.context.remapElement(referenceId, referenceId); // when in the same iModel, can use existing DefinitionElements without remapping
          } else {
            throw new IModelError(IModelStatus.BadRequest, `Remapping for dependency ${referenceId} not found`);
          }
        }
      }
    });
    const targetElementProps: ElementProps = super.onTransformElement(sourceElement);
    targetElementProps.federationGuid = Guid.createValue(); // clone from template should create a new federationGuid
    targetElementProps.code = Code.createEmpty(); // clone from template should not maintain codes
    if (sourceElement instanceof GeometricElement3d) {
      const placement = Placement3d.fromJSON((targetElementProps as GeometricElement3dProps).placement);
      if (placement.isValid) {
        placement.multiplyTransform(this._transform3d!);
        (targetElementProps as GeometricElement3dProps).placement = placement;
      }
    } else if (sourceElement instanceof GeometricElement2d) {
      const placement = Placement2d.fromJSON((targetElementProps as GeometricElement2dProps).placement);
      if (placement.isValid) {
        placement.multiplyTransform(this._transform3d!);
        (targetElementProps as GeometricElement2dProps).placement = placement;
      }
    }
    this._sourceIdToTargetIdMap!.set(sourceElement.id, Id64.invalid); // keep track of (source) elementIds from the template model, but the target hasn't been inserted yet
    return targetElementProps;
  }
}


function queryElemFedGuid(db: IModelDb, elemId: Id64String) {
  return db.withPreparedStatement(`
    SELECT FederationGuid
    FROM bis.Element
    WHERE ECInstanceId=?
  `, (stmt) => {
    stmt.bindId(1, elemId);
    assert(stmt.step() === DbResult.BE_SQLITE_ROW);
    const result = stmt.getValue(0).getGuid();
    assert(stmt.step() === DbResult.BE_SQLITE_DONE);
    return result;
  });
}

