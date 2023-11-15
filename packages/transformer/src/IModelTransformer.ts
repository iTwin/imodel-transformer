/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import * as path from "path";
import * as Semver from "semver";
import * as nodeAssert from "assert";
import {
  AccessToken, assert, CompressedId64Set, DbResult, Guid, GuidString, Id64, Id64Array, Id64String, IModelStatus, Logger, MarkRequired,
  OpenMode, YieldManager,
} from "@itwin/core-bentley";
import * as ECSchemaMetaData from "@itwin/ecschema-metadata";
import { Point3d, Transform } from "@itwin/core-geometry";
import {
  ChangeSummaryManager,
  ChannelRootAspect, ConcreteEntity, DefinitionElement, DefinitionModel, DefinitionPartition, ECSchemaXmlContext, ECSqlStatement, ECSqlValue, Element, ElementAspect, ElementMultiAspect, ElementOwnsExternalSourceAspects,
  ElementRefersToElements, ElementUniqueAspect, Entity, EntityReferences, ExternalSource, ExternalSourceAspect, ExternalSourceAttachment,
  FolderLink, GeometricElement, GeometricElement3d, IModelDb, IModelHost, IModelJsFs, InformationPartitionElement, KnownLocations, Model,
  RecipeDefinitionElement, Relationship, RelationshipProps, Schema, SQLiteDb, Subject, SynchronizationConfigLink,
} from "@itwin/core-backend";
import {
  ChangeOpCode, ChangesetIndexAndId, Code, CodeProps, CodeSpec, ConcreteEntityTypes, ElementAspectProps, ElementProps, EntityReference, EntityReferenceSet,
  ExternalSourceAspectProps, FontProps, GeometricElementProps, IModel, IModelError, ModelProps,
  Placement2d, Placement3d, RelatedElement, SourceAndTarget,
} from "@itwin/core-common";
import { ExportChangesOptions, ExporterInitOptions, ExportSchemaResult, IModelExporter, IModelExporterState, IModelExportHandler } from "./IModelExporter";
import { IModelImporter, IModelImporterState, OptimizeGeometryOptions } from "./IModelImporter";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { PendingReference, PendingReferenceMap } from "./PendingReferenceMap";
import { EntityKey, EntityMap } from "./EntityMap";
import { IModelCloneContext } from "./IModelCloneContext";
import { EntityUnifier } from "./EntityUnifier";
import { rangesFromRangeAndSkipped } from "./Algo";

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

  // FIXME: deprecate this, we should now be able to detect it from the external source aspect data
  // add property
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
   * and provide an [ElementGeometryBuilderParams]($common) to the `elementGeometryBuilderParams`
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

  /**
   * Do not detach the change cache that we build. Use this if you want to do multiple transformations to
   * the same iModels, to avoid the performance cost of reinitializing the change cache which can be
   * expensive. You should only use this if you know the cache will be reused.
   * @note You must detach the change cache yourself.
   * @default false
   */
  noDetachChangeCache?: boolean;

  /**
   * Do not check that processChanges is called from the next changeset index.
   * This is an unsafe option (e.g. it can cause data loss in future branch operations)
   * and you should not use it.
   * @default false
   */
  ignoreMissingChangesetsInSynchronizations?: boolean;
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

interface TargetScopeProvenanceJsonProps {
  pendingReverseSyncChangesetIndices: number[];
  pendingSyncChangesetIndices: number[];
  reverseSyncVersion: string;
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
export interface InitOptions {
  accessToken?: AccessToken;
  /**
   * Include changes from this changeset up through and including the current changeset.
   * @note To form a range of versions to process, set `startChangeset` for the start (inclusive)
   * of the desired range and open the source iModel as of the end (inclusive) of the desired range.
   * @default the current changeset of the sourceDb, if undefined
   */
  startChangeset?: {
    id?: string;
    index?: number;
  };
}

/**
 * Arguments for [[IModelTransformer.processChanges]]
 */
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface ProcessChangesOptions extends ExportChangesOptions {
  /** how to call saveChanges on the target. Must call targetDb.saveChanges, should not edit the iModel */
  saveTargetChanges?: (transformer: IModelTransformer) => Promise<void>;
}

type ChangeDataState = "uninited" | "has-changes" | "no-changes" | "unconnected";

/** Arguments you can pass to [[IModelTransformer.initExternalSourceAspects]]
 * @deprecated in 0.1.0. Use [[InitOptions]] (and [[IModelTransformer.initialize]]) instead.
 */
export type InitFromExternalSourceAspectsArgs = InitOptions;

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
  protected _elementsWithExplicitlyTrackedProvenance = new Set<Id64String>();

  /** map of partially committed entities to their partial commit progress */
  protected _partiallyCommittedEntities = new EntityMap<PartiallyCommittedEntity>();

  /** the options that were used to initialize this transformer */
  private readonly _options: MarkRequired<IModelTransformOptions, "targetScopeElementId" | "danglingReferencesBehavior">;

  private _isSynchronization = false;

  private get _isReverseSynchronization() {
    return this._isSynchronization && this._options.isReverseSynchronization;
  }

  private get _isForwardSynchronization() {
    return this._isSynchronization && !this._options.isReverseSynchronization;
  }

  private _changesetRanges: [number, number][] | undefined = undefined;

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

  /** Set of entity keys which were not exported and don't need to be tracked for pending reference resolution.
   * @note Currently only tracks elements which were not exported.
   */
  protected _skippedEntities = new Set<EntityKey>();

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

    if (this.sourceDb.isBriefcase && this.targetDb.isBriefcase) {
      nodeAssert(
        this.sourceDb.changeset.index !== undefined && this.targetDb.changeset.index !== undefined,
        "database has no changeset index"
      );
      this._startingChangesetIndices = {
        target: this.targetDb.changeset.index,
        source: this.sourceDb.changeset.index,
      };
    }

    // this internal is guaranteed stable for just transformer usage
    /* eslint-disable @itwin/no-internal */
    if ("codeValueBehavior" in this.sourceDb as any) {
      (this.sourceDb as any).codeValueBehavior = "exact";
      (this.targetDb as any).codeValueBehavior = "exact";
    }
    /* eslint-enable @itwin/no-internal */
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

  /** Return the IModelDb where IModelTransformer looks for entities referred to by stored provenance.
   * @note This will be [[sourceDb]] except when it is a reverse synchronization. In that case it be [[targetDb]].
   */
  public get provenanceSourceDb(): IModelDb {
    return this._options.isReverseSynchronization ? this.targetDb : this.sourceDb;
  }

  /** Create an ExternalSourceAspectProps in a standard way for an Element in an iModel --> iModel transformation. */
  public static initElementProvenanceOptions(
    sourceElementId: Id64String,
    targetElementId: Id64String,
    args: {
      sourceDb: IModelDb;
      targetDb: IModelDb;
      // FIXME: deprecate isReverseSync option and instead detect from targetScopeElement provenance
      isReverseSynchronization: boolean;
      targetScopeElementId: Id64String;
    },
  ): ExternalSourceAspectProps {
    const elementId = args.isReverseSynchronization ? sourceElementId : targetElementId;
    const version = args.isReverseSynchronization
      ? args.targetDb.elements.queryLastModifiedTime(targetElementId)
      : args.sourceDb.elements.queryLastModifiedTime(sourceElementId);
    const aspectIdentifier = args.isReverseSynchronization ? targetElementId : sourceElementId;
    const aspectProps: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: elementId, relClassName: ElementOwnsExternalSourceAspects.classFullName },
      scope: { id: args.targetScopeElementId },
      identifier: aspectIdentifier,
      kind: ExternalSourceAspect.Kind.Element,
      version,
    };
    return aspectProps;
  }

  public static initRelationshipProvenanceOptions(
    sourceRelInstanceId: Id64String,
    targetRelInstanceId: Id64String,
    args: {
      sourceDb: IModelDb;
      targetDb: IModelDb;
      isReverseSynchronization: boolean;
      targetScopeElementId: Id64String;
      forceOldRelationshipProvenanceMethod: boolean;
    },
  ): ExternalSourceAspectProps {
    const provenanceDb = args.isReverseSynchronization ? args.sourceDb : args.targetDb;
    const aspectIdentifier = args.isReverseSynchronization ? targetRelInstanceId : sourceRelInstanceId;
    const provenanceRelInstanceId = args.isReverseSynchronization ? sourceRelInstanceId : targetRelInstanceId;

    const elementId = provenanceDb.withPreparedStatement(
        "SELECT SourceECInstanceId FROM bis.ElementRefersToElements WHERE ECInstanceId=?",
        (stmt) => {
          stmt.bindId(1, provenanceRelInstanceId);
          nodeAssert(stmt.step() === DbResult.BE_SQLITE_ROW);
          return stmt.getValue(0).getId();
        },
      );

    const jsonProperties
      = args.forceOldRelationshipProvenanceMethod
      ? { targetRelInstanceId }
      : { provenanceRelInstanceId };

    const aspectProps: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: elementId, relClassName: ElementOwnsExternalSourceAspects.classFullName },
      scope: { id: args.targetScopeElementId },
      identifier: aspectIdentifier,
      kind: ExternalSourceAspect.Kind.Relationship,
      jsonProperties: JSON.stringify(jsonProperties),
    };

    return aspectProps;
  }

  // FIXME: add test transforming using this, then switching to new transform method
  /**
   * Previously the transformer would insert provenance always pointing to the "target" relationship.
   * It should (and now by default does) instead insert provenance pointing to the provenanceSource
   * SEE: https://github.com/iTwin/imodel-transformer/issues/54
   * This exists only to facilitate testing that the transformer can handle the older, flawed method
   */
  private _forceOldRelationshipProvenanceMethod = false;

  /** Create an ExternalSourceAspectProps in a standard way for an Element in an iModel --> iModel transformation. */
  public initElementProvenance(sourceElementId: Id64String, targetElementId: Id64String): ExternalSourceAspectProps {
    return IModelTransformer.initElementProvenanceOptions(
      sourceElementId,
      targetElementId,
      {
        isReverseSynchronization: !!this._options.isReverseSynchronization,
        targetScopeElementId: this.targetScopeElementId,
        sourceDb: this.sourceDb,
        targetDb: this.targetDb,
      },
    );
  }

  /** Create an ExternalSourceAspectProps in a standard way for a Relationship in an iModel --> iModel transformations.
   * The ExternalSourceAspect is meant to be owned by the Element in the target iModel that is the `sourceId` of transformed relationship.
   * The `identifier` property of the ExternalSourceAspect will be the ECInstanceId of the relationship in the source (meaning master) iModel.
   * The ECInstanceId of the relationship in the target(meaning branch) iModel will be stored in the JsonProperties of the ExternalSourceAspect.
   */
  private initRelationshipProvenance(sourceRelationship: Relationship, targetRelInstanceId: Id64String): ExternalSourceAspectProps {
    return IModelTransformer.initRelationshipProvenanceOptions(
      sourceRelationship.id,
      targetRelInstanceId,
      {
        sourceDb: this.sourceDb,
        targetDb: this.targetDb,
        isReverseSynchronization: !!this._options.isReverseSynchronization,
        targetScopeElementId: this.targetScopeElementId,
        forceOldRelationshipProvenanceMethod: this._forceOldRelationshipProvenanceMethod,
      }
    );
  }

  /** NOTE: the json properties must be converted to string before insertion */
  private _targetScopeProvenanceProps:
    Omit<ExternalSourceAspectProps, "jsonProperties"> & { jsonProperties: TargetScopeProvenanceJsonProps }
    | undefined
      = undefined;

  /**
   * Index of the changeset that the transformer was at when the transformation begins (was constructed).
   * Used to determine at the end which changesets were part of a synchronization.
   */
  private _startingChangesetIndices: {
    target: number;
    source: number;
  } | undefined = undefined;

  private _cachedSynchronizationVersion: ChangesetIndexAndId | undefined = undefined;

  /** the changeset in the scoping element's source version found for this transformation
   * @note: the version depends on whether this is a reverse synchronization or not, as
   * it is stored separately for both synchronization directions
   * @note: empty string and -1 for changeset and index if it has never been transformed
   */
  private get _synchronizationVersion(): ChangesetIndexAndId {
    if (!this._cachedSynchronizationVersion) {
      nodeAssert(this._targetScopeProvenanceProps, "_targetScopeProvenanceProps was not set yet");
      const version = this._options.isReverseSynchronization
        ? this._targetScopeProvenanceProps.jsonProperties.reverseSyncVersion
        : this._targetScopeProvenanceProps.version;

      nodeAssert(version !== undefined, "no version contained in target scope");

      const [id, index] = version === ""
        ? ["", -1]
        : version.split(";");
      this._cachedSynchronizationVersion = { index: Number(index), id };
      nodeAssert(!Number.isNaN(this._cachedSynchronizationVersion.index), "bad parse: invalid index in version");
    }
    return this._cachedSynchronizationVersion;
  }

  /**
   * Make sure there are no conflicting other scope-type external source aspects on the *target scope element*,
   * If there are none at all, insert one, then this must be a first synchronization.
   * @returns the last synced version (changesetId) on the target scope's external source aspect,
   *          if this was a [BriefcaseDb]($backend)
   */
  private initScopeProvenance(): void {
    const aspectProps = {
      id: undefined as string | undefined,
      version: undefined as string | undefined,
      classFullName: ExternalSourceAspect.classFullName,
      element: { id: this.targetScopeElementId, relClassName: ElementOwnsExternalSourceAspects.classFullName },
      scope: { id: IModel.rootSubjectId }, // the root Subject scopes scope elements
      identifier: this.provenanceSourceDb.iModelId,
      kind: ExternalSourceAspect.Kind.Scope,
      jsonProperties: undefined as TargetScopeProvenanceJsonProps | undefined,
    };

    // FIXME: handle older transformed iModels which do NOT have the version. Add test where we don't set those and then start setting them.
    // or reverseSyncVersion set correctly
    const externalSource = this.queryScopeExternalSource(aspectProps, { getJsonProperties: true }); // this query includes "identifier"
    aspectProps.id = externalSource.aspectId;
    aspectProps.version = externalSource.version;
    aspectProps.jsonProperties = externalSource.jsonProperties ? JSON.parse(externalSource.jsonProperties) : {};

    if (undefined === aspectProps.id) {
      aspectProps.version = ""; // empty since never before transformed. Will be updated in [[finalizeTransformation]]
      aspectProps.jsonProperties = {
        pendingReverseSyncChangesetIndices: [],
        pendingSyncChangesetIndices: [],
        reverseSyncVersion: "", // empty since never before transformed. Will be updated in first reverse sync
      };

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
        this.provenanceDb.elements.insertAspect({
          ...aspectProps,
          jsonProperties: JSON.stringify(aspectProps.jsonProperties) as any,
        });
      }
    }

    this._targetScopeProvenanceProps = aspectProps as typeof this._targetScopeProvenanceProps;
  }

  /**
   * @returns the id and version of an aspect with the given element, scope, kind, and identifier
   * May also return a reverseSyncVersion from json properties if requested
   */
  private queryScopeExternalSource(aspectProps: ExternalSourceAspectProps, { getJsonProperties = false } = {}): {
    aspectId?: Id64String;
    version?: string;
    /** stringified json */
    jsonProperties?: string;
  } {
    const sql = `
      SELECT ECInstanceId, Version
      ${getJsonProperties ? ", JsonProperties" : ""}
      FROM ${ExternalSourceAspect.classFullName}
      WHERE Element.Id=:elementId
        AND Scope.Id=:scopeId
        AND Kind=:kind
        AND Identifier=:identifier
      LIMIT 1
    `;
    const emptyResult = { aspectId: undefined, version: undefined, jsonProperties: undefined };
    return this.provenanceDb.withPreparedStatement(sql, (statement: ECSqlStatement) => {
      statement.bindId("elementId", aspectProps.element.id);
      if (aspectProps.scope === undefined)
        return emptyResult; // return undefined instead of binding an invalid id
      statement.bindId("scopeId", aspectProps.scope.id);
      statement.bindString("kind", aspectProps.kind);
      statement.bindString("identifier", aspectProps.identifier);
      if (DbResult.BE_SQLITE_ROW !== statement.step())
        return emptyResult;
      const aspectId = statement.getValue(0).getId();
      const version = statement.getValue(1).getString();
      const jsonProperties = getJsonProperties ? statement.getValue(2).getString() : undefined;
      return { aspectId, version, jsonProperties };
    });
  }

  /**
   * Iterate all matching federation guids and ExternalSourceAspects in the provenance iModel (target unless reverse sync)
   * and call a function for each one.
   * @note provenance is done by federation guids where possible
   * @note this may execute on each element more than once! Only use in cases where that is handled
   */
  public static forEachTrackedElement(args: {
    provenanceSourceDb: IModelDb;
    provenanceDb: IModelDb;
    targetScopeElementId: Id64String;
    isReverseSynchronization: boolean;
    fn: (sourceElementId: Id64String, targetElementId: Id64String) => void;
  }): void {
    if (args.provenanceDb === args.provenanceSourceDb)
      return;

    if (!args.provenanceDb.containsClass(ExternalSourceAspect.classFullName)) {
      throw new IModelError(IModelStatus.BadSchema, "The BisCore schema version of the target database is too old");
    }

    const sourceDb = args.isReverseSynchronization ? args.provenanceDb : args.provenanceSourceDb;
    const targetDb = args.isReverseSynchronization ? args.provenanceSourceDb : args.provenanceDb;

    // query for provenanceDb
    const elementIdByFedGuidQuery = `
      SELECT e.ECInstanceId, FederationGuid
      FROM bis.Element e
      WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10) -- special static elements
      ORDER BY FederationGuid
    `;

    // iterate through sorted list of fed guids from both dbs to get the intersection
    // NOTE: if we exposed the native attach database support,
    // we could get the intersection of fed guids in one query, not sure if it would be faster
    // OR we could do a raw sqlite query...
    sourceDb.withStatement(elementIdByFedGuidQuery, (sourceStmt) => targetDb.withStatement(elementIdByFedGuidQuery, (targetStmt) => {
      if (sourceStmt.step() !== DbResult.BE_SQLITE_ROW)
        return;
      let sourceRow = sourceStmt.getRow() as { federationGuid?: GuidString, id: Id64String };
      if (targetStmt.step() !== DbResult.BE_SQLITE_ROW)
        return;
      let targetRow = targetStmt.getRow() as { federationGuid?: GuidString, id: Id64String };

      // NOTE: these comparisons rely upon the lowercase of the guid,
      // and the fact that '0' < '9' < a' < 'f' in ascii/utf8
      while (true) {
        const currSourceRow = sourceRow, currTargetRow = targetRow;
        if (currSourceRow.federationGuid !== undefined
          && currTargetRow.federationGuid !== undefined
          && currSourceRow.federationGuid === currTargetRow.federationGuid
        ) {
          // data flow direction is always sourceDb -> targetDb and it does not depend on where the explicit element provenance is stored
          args.fn(sourceRow.id, targetRow.id);
        }
        if (currTargetRow.federationGuid === undefined
          || (currSourceRow.federationGuid !== undefined
            && currSourceRow.federationGuid >= currTargetRow.federationGuid)
        ) {
          if (targetStmt.step() !== DbResult.BE_SQLITE_ROW)
            return;
          targetRow = targetStmt.getRow();
        }
        if (currSourceRow.federationGuid === undefined
          || (currTargetRow.federationGuid !== undefined
            && currSourceRow.federationGuid <= currTargetRow.federationGuid)
        ) {
          if (sourceStmt.step() !== DbResult.BE_SQLITE_ROW)
            return;
          sourceRow = sourceStmt.getRow();
        }
      }
    }));

    // query for provenanceDb
    const provenanceAspectsQuery = `
      SELECT esa.Identifier, Element.Id
      FROM bis.ExternalSourceAspect esa
      WHERE Scope.Id=:scopeId
        AND Kind=:kind
    `;

    // Technically this will a second time call the function (as documented) on
    // victims of the old provenance method that have both fedguids and an inserted aspect.
    // But this is a private function with one known caller where that doesn't matter
    args.provenanceDb.withPreparedStatement(provenanceAspectsQuery, (stmt): void => {
      const runFnInDataFlowDirection = (sourceId: Id64String, targetId: Id64String) =>
        args.isReverseSynchronization ? args.fn(sourceId, targetId) : args.fn(targetId, sourceId);
      stmt.bindId("scopeId", args.targetScopeElementId);
      stmt.bindString("kind", ExternalSourceAspect.Kind.Element);
      while (DbResult.BE_SQLITE_ROW === stmt.step()) {
        // ExternalSourceAspect.Identifier is of type string
        const aspectIdentifier: Id64String = stmt.getValue(0).getString();
        const elementId: Id64String = stmt.getValue(1).getId();
        runFnInDataFlowDirection(elementId, aspectIdentifier);
      }
    });
  }

  private forEachTrackedElement(fn: (sourceElementId: Id64String, targetElementId: Id64String) => void): void {
    return IModelTransformer.forEachTrackedElement({
      provenanceSourceDb: this.provenanceSourceDb,
      provenanceDb: this.provenanceDb,
      targetScopeElementId: this.targetScopeElementId,
      isReverseSynchronization: !!this._options.isReverseSynchronization,
      fn,
    });
  }

  /** Initialize the source to target Element mapping from ExternalSourceAspects in the target iModel.
   * @note This method is called from all `process*` functions and should never need to be called directly.
   * @deprecated in 3.x. call [[initialize]] instead, it does the same thing among other initialization
   * @note Passing an [[InitFromExternalSourceAspectsArgs]] is required when processing changes, to remap any elements that may have been deleted.
   *       You must await the returned promise as well in this case. The synchronous behavior has not changed but is deprecated and won't process everything.
   */
  public initFromExternalSourceAspects(args?: InitOptions): void | Promise<void> {
    this.forEachTrackedElement((sourceElementId: Id64String, targetElementId: Id64String) => {
      this.context.remapElement(sourceElementId, targetElementId);
    });

    if (args)
      return this.remapDeletedSourceEntities();
  }

  /**
   * Scan changesets for deleted entities, if in a reverse synchronization, provenance has
   * already been deleted, so we must scan for that as well.
   */
  private async remapDeletedSourceEntities() {
    // we need a connected iModel with changes to remap elements with deletions
    const notConnectedModel = this.sourceDb.iTwinId === undefined;
    const noChanges = this._synchronizationVersion.index === this.sourceDb.changeset.index;
    if (notConnectedModel || noChanges)
      return;

    this._deletedSourceRelationshipData = new Map();

    nodeAssert(this._changeSummaryIds, "change summaries should be initialized before we get here");
    if (this._changeSummaryIds.length === 0)
      return;

    const alreadyImportedElementInserts = new Set<Id64String> ();
    const alreadyImportedModelInserts = new Set<Id64String> ();
    this.exporter.sourceDbChanges?.element.insertIds.forEach((insertedSourceElementId) => {
      const targetElementId = this.context.findTargetElementId(insertedSourceElementId);
      if (Id64.isValid(targetElementId))
        alreadyImportedElementInserts.add(targetElementId);
    });
    this.exporter.sourceDbChanges?.model.insertIds.forEach((insertedSourceModelId) => {
      const targetModelId = this.context.findTargetElementId(insertedSourceModelId);
      if (Id64.isValid(targetModelId))
        alreadyImportedModelInserts.add(targetModelId);
    });

    // optimization: if we have provenance, use it to avoid more querying later
    // eventually when itwin.js supports attaching a second iModelDb in JS,
    // this won't have to be a conditional part of the query, and we can always have it by attaching
    const queryCanAccessProvenance = this.sourceDb === this.provenanceDb;

    const deletedEntitySql = `
      SELECT
        1 AS IsElemNotRel,
        ic.ChangedInstance.Id AS InstanceId,
        NULL AS InstId2, -- need these columns for relationship ends in the unioned query
        NULL AS InstId3,
        ec.FederationGuid AS FedGuid,
        NULL AS FedGuid2,
        ic.ChangedInstance.ClassId AS ClassId
        ${queryCanAccessProvenance ? `
        /*
        -- can't coalesce these due to a bug, so do it in JS
        , coalesce(
            IIF(esa.Scope.Id=:targetScopeElement, esa.Identifier, NULL),
            IIF(esac.Scope.Id=:targetScopeElement, esac.Identifier, NULL)
          ) AS Identifier1
        */
        , CASE WHEN esa.Scope.Id = ${this.targetScopeElementId} THEN esa.Identifier ELSE NULL END AS Identifier1A
        -- FIXME: using :targetScopeElement parameter in this second potential identifier breaks ecsql
        , CASE WHEN esac.Scope.Id = ${this.targetScopeElementId} THEN esac.Identifier ELSE NULL END AS Identifier1B
        , NULL AS Identifier2A
        , NULL AS Identifier2B
        ` : ""}
      FROM ecchange.change.InstanceChange ic
        LEFT JOIN bis.Element.Changes(:changeSummaryId, 'BeforeDelete') ec
          ON ic.ChangedInstance.Id=ec.ECInstanceId
        ${queryCanAccessProvenance ? `
          LEFT JOIN bis.ExternalSourceAspect esa
            ON ec.ECInstanceId=esa.Element.Id
          LEFT JOIN bis.ExternalSourceAspect.Changes(:changeSummaryId, 'BeforeDelete') esac
            ON ec.ECInstanceId=esac.Element.Id
        ` : ""
        }
      WHERE ic.OpCode=:opDelete
        AND ic.Summary.Id=:changeSummaryId
        AND ic.ChangedInstance.ClassId IS (BisCore.Element)

      UNION ALL

      SELECT
        0 AS IsElemNotRel,
        ic.ChangedInstance.Id AS InstanceId,
        coalesce(se.ECInstanceId, sec.ECInstanceId) AS InstId2,
        coalesce(te.ECInstanceId, tec.ECInstanceId) AS InstId3,
        coalesce(se.FederationGuid, sec.FederationGuid) AS FedGuid1,
        coalesce(te.FederationGuid, tec.FederationGuid) AS FedGuid2,
        ic.ChangedInstance.ClassId AS ClassId
        ${queryCanAccessProvenance ? `
        , sesa.Identifier AS Identifier1A
        , sesac.Identifier AS Identifier1B
        , tesa.Identifier AS Identifier2A
        , tesac.Identifier AS Identifier2B
        ` : ""}
      FROM ecchange.change.InstanceChange ic
        LEFT JOIN bis.ElementRefersToElements.Changes(:changeSummaryId, 'BeforeDelete') ertec
          ON ic.ChangedInstance.Id=ertec.ECInstanceId
        -- FIXME: test a deletion of both an element and a relationship at the same time
        LEFT JOIN bis.Element se
          ON se.ECInstanceId=ertec.SourceECInstanceId
        LEFT JOIN bis.Element te
          ON te.ECInstanceId=ertec.TargetECInstanceId
        LEFT JOIN bis.Element.Changes(:changeSummaryId, 'BeforeDelete') sec
          ON sec.ECInstanceId=ertec.SourceECInstanceId
        LEFT JOIN bis.Element.Changes(:changeSummaryId, 'BeforeDelete') tec
          ON tec.ECInstanceId=ertec.TargetECInstanceId
        ${queryCanAccessProvenance ? `
          -- NOTE: need to join on both se/te and sec/tec incase the element was deleted
          LEFT JOIN bis.ExternalSourceAspect sesa
            ON se.ECInstanceId=sesa.Element.Id -- don't use *esac*.Identifier because it's a string
          LEFT JOIN bis.ExternalSourceAspect.Changes(:changeSummaryId, 'BeforeDelete') sesac
            ON sec.ECInstanceId=sesac.Element.Id
          LEFT JOIN bis.ExternalSourceAspect tesa
            ON te.ECInstanceId=tesa.Element.Id
          LEFT JOIN bis.ExternalSourceAspect.Changes(:changeSummaryId, 'BeforeDelete') tesac
            ON tec.ECInstanceId=tesac.Element.Id
          ` : ""
        }
      WHERE ic.OpCode=:opDelete
        AND ic.Summary.Id=:changeSummaryId
        AND ic.ChangedInstance.ClassId IS (BisCore.ElementRefersToElements)
        ${queryCanAccessProvenance ? `
          AND (sesa.Scope.Id=:targetScopeElement OR sesa.Scope.Id IS NULL)
          AND (sesa.Kind='Relationship' OR sesa.Kind IS NULL)
          AND (sesac.Scope.Id=:targetScopeElement OR sesac.Scope.Id IS NULL)
          AND (sesac.Kind='Relationship' OR sesac.Kind IS NULL)
          AND (tesa.Scope.Id=:targetScopeElement OR tesa.Scope.Id IS NULL)
          AND (tesa.Kind='Relationship' OR tesa.Kind IS NULL)
          AND (tesac.Scope.Id=:targetScopeElement OR tesac.Scope.Id IS NULL)
          AND (tesac.Kind='Relationship' OR tesac.Kind IS NULL)
          ` : ""
        }
    `;

    for (const changeSummaryId of this._changeSummaryIds) {
      this.sourceDb.withPreparedStatement(deletedEntitySql, (stmt) => {
        stmt.bindInteger("opDelete", ChangeOpCode.Delete);
        if (queryCanAccessProvenance)
          stmt.bindId("targetScopeElement", this.targetScopeElementId);
        stmt.bindId("changeSummaryId", changeSummaryId);
        while (DbResult.BE_SQLITE_ROW === stmt.step()) {
          const isElemNotRel = stmt.getValue(0).getBoolean();
          const instId = stmt.getValue(1).getId();

          if (isElemNotRel) {
            const sourceElemFedGuid = stmt.getValue(4).getGuid();
            // "Identifier" is a string, so null value returns '' which doesn't work with ??, and I don't like ||
            let identifierValue: ECSqlValue | undefined;

            // identifier must be coalesced in JS due to an ESCQL bug, so there are multiple columns
            if (queryCanAccessProvenance)  {
              identifierValue = stmt.getValue(7);
              if (identifierValue.isNull)
                identifierValue = stmt.getValue(8);
            }

            // TODO: if I could attach the second db, will probably be much faster to get target id
            // as part of the whole query rather than with _queryElemIdByFedGuid
            const targetId =
              (queryCanAccessProvenance && identifierValue
                && !identifierValue.isNull
                && identifierValue.getString())
              // maybe batching these queries would perform better but we should
              // try to attach the second db and query both together anyway
              || (sourceElemFedGuid && this._queryElemIdByFedGuid(this.targetDb, sourceElemFedGuid))
              // FIXME<MIKE>: describe why it's safe to assume nothing has been deleted in provenanceDb
              || this._queryProvenanceForElement(instId);

            // since we are processing one changeset at a time, we can see local source deletes
            // of entities that were never synced and can be safely ignored
            const deletionNotInTarget = !targetId;
            if (deletionNotInTarget)
              continue;

            this.context.remapElement(instId, targetId);
            // If an entity insert and an entity delete both point to the same entity in target iModel, that means that entity was recreated.
            // In such case an entity update will be triggered and we no longer need to delete the entity.
            if (alreadyImportedElementInserts.has(targetId)) {
              this.exporter.sourceDbChanges?.element.deleteIds.delete(instId);
            }
            if (alreadyImportedModelInserts.has(targetId)) {
              this.exporter.sourceDbChanges?.model.deleteIds.delete(instId);
            }

          } else { // is deleted relationship
            const classFullName = stmt.getValue(6).getClassNameForClassId();
            const [sourceIdInTarget, targetIdInTarget] = [
              // identifier must be coalesced in JS due to an ESCQL bug, so there are multiple columns
              { guidColumn: 4, identifierColumns: { a: 7, b: 8 }, isTarget: false },
              { guidColumn: 5, identifierColumns: { a: 9, b: 10 }, isTarget: true },
            ].map(({ guidColumn, identifierColumns }) => {
              const fedGuid = stmt.getValue(guidColumn).getGuid();
              let identifierValue: ECSqlValue | undefined;

              // identifier must be coalesced in JS due to an ESCQL bug, so there are multiple columns
              if (queryCanAccessProvenance)  {
                identifierValue = stmt.getValue(identifierColumns.a);
                if (identifierValue.isNull)
                  identifierValue = stmt.getValue(identifierColumns.b);
              }

              return (
                (queryCanAccessProvenance && identifierValue
                  // FIXME: this is really far from idiomatic, try to undo that
                  && !identifierValue.isNull
                  && identifierValue.getString())
                // maybe batching these queries would perform better but we should
                // try to attach the second db and query both together anyway
                || (fedGuid && this._queryElemIdByFedGuid(this.targetDb, fedGuid))
              );
            });

            // since we are processing one changeset at a time, we can see local source deletes
            // of entities that were never synced and can be safely ignored
            if (sourceIdInTarget && targetIdInTarget) {
              this._deletedSourceRelationshipData!.set(instId, {
                classFullName,
                sourceIdInTarget,
                targetIdInTarget,
              });
            } else {
              // FIXME<MIKE>: describe why it's safe to assume nothing has been deleted in provenanceDb
              const relProvenance = this._queryProvenanceForRelationship(instId, {
                classFullName,
                sourceId: stmt.getValue(2).getId(),
                targetId: stmt.getValue(3).getId(),
              });
              if (relProvenance && relProvenance.relationshipId)
                this._deletedSourceRelationshipData!.set(instId, {
                  classFullName,
                  relId: relProvenance.relationshipId,
                  provenanceAspectId: relProvenance.aspectId,
                });
            }
          }
        }
        // NEXT: remap sourceId and targetId to target, get provenance there
        // NOTE: it is possible during a forward sync for the target to already have deleted
        // something that the source deleted, in which case we can safely ignore the gone provenance
      });
    }
  }

  private _queryProvenanceForElement(entityInProvenanceSourceId: Id64String): Id64String | undefined {
    return this.provenanceDb.withPreparedStatement(`
        SELECT esa.Element.Id
        FROM Bis.ExternalSourceAspect esa
        WHERE esa.Kind=?
          AND esa.Scope.Id=?
          AND esa.Identifier=?
      `, (stmt) => {
      stmt.bindString(1, ExternalSourceAspect.Kind.Element);
      stmt.bindId(2, this.targetScopeElementId);
      stmt.bindString(3, entityInProvenanceSourceId);
      if (stmt.step() === DbResult.BE_SQLITE_ROW)
        return stmt.getValue(0).getId();
      else
        return undefined;
    });
  }

  private _queryProvenanceForRelationship(
    entityInProvenanceSourceId: Id64String,
    sourceRelInfo: {
      classFullName: string;
      sourceId: Id64String;
      targetId: Id64String;
    }): {
    aspectId: Id64String;
    /** if undefined, the relationship could not be found, perhaps it was deleted */
    relationshipId: Id64String | undefined;
  } | undefined {
    return this.provenanceDb.withPreparedStatement(`
        SELECT
          ECInstanceId,
          JSON_EXTRACT(JsonProperties, '$.targetRelInstanceId'),
          JSON_EXTRACT(JsonProperties, '$.provenanceRelInstanceId')
        FROM Bis.ExternalSourceAspect
        WHERE Kind=?
          AND Scope.Id=?
          AND Identifier=?
      `, (stmt) => {
      stmt.bindString(1, ExternalSourceAspect.Kind.Relationship);
      stmt.bindId(2, this.targetScopeElementId);
      stmt.bindString(3, entityInProvenanceSourceId);
      if (stmt.step() !== DbResult.BE_SQLITE_ROW)
        return undefined;

      const aspectId = stmt.getValue(0).getId();
      const provenanceRelInstIdVal = stmt.getValue(2);
      const provenanceRelInstanceId
        = !provenanceRelInstIdVal.isNull
        ? provenanceRelInstIdVal.getString()
        : this._queryTargetRelId(sourceRelInfo);
      return {
        aspectId,
        relationshipId: provenanceRelInstanceId,
      };
    });
  }

  private _queryTargetRelId(sourceRelInfo: {
    classFullName: string;
    sourceId: Id64String;
    targetId: Id64String;
  }): Id64String | undefined {
    const targetRelInfo = {
      sourceId: this.context.findTargetElementId(sourceRelInfo.sourceId),
      targetId: this.context.findTargetElementId(sourceRelInfo.targetId),
    };
    if (targetRelInfo.sourceId === undefined || targetRelInfo.targetId === undefined)
      return undefined; // couldn't find an element, rel is invalid or deleted
    return this.targetDb.withPreparedStatement(`
      SELECT ECInstanceId
      FROM bis.ElementRefersToElements
      WHERE SourceECInstanceId=?
        AND TargetECInstanceId=?
        AND ECClassId=?
    `, (stmt) => {
      stmt.bindId(1, targetRelInfo.sourceId);
      stmt.bindId(2, targetRelInfo.targetId);
      stmt.bindId(3, this._targetClassNameToClassId(sourceRelInfo.classFullName));
      if (stmt.step() !== DbResult.BE_SQLITE_ROW)
        return undefined;
      return stmt.getValue(0).getId();
    });
  }

  private _targetClassNameToClassIdCache = new Map<string, string>();

  private _targetClassNameToClassId(classFullName: string): Id64String {
    let classId = this._targetClassNameToClassIdCache.get(classFullName);
    if (classId === undefined) {
      classId = this._getRelClassId(this.targetDb, classFullName);
      this._targetClassNameToClassIdCache.set(classFullName, classId);
    }
    return classId;
  }

  // NOTE: this doesn't handle remapped element classes,
  // but is only used for relationships rn
  private _getRelClassId(db: IModelDb, classFullName: string): Id64String {
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
  protected shouldDetectDeletes(): boolean {
    // FIXME: all synchronizations should mark this as false, but we can probably change this
    // to just follow the new deprecated option
    if (this._isFirstSynchronization)
      return false; // not necessary the first time since there are no deletes to detect

    if (this._options.isReverseSynchronization)
      return false; // not possible for a reverse synchronization since provenance will be deleted when element is deleted

    // FIXME: do any tests fail? if not, consider using @see _isSynchronization
    if (this._isForwardSynchronization)
      return false; // not possible for a reverse synchronization since provenance will be deleted when element is deleted

    return true;
  }

  /**
   * Detect Element deletes using ExternalSourceAspects in the target iModel and a *brute force* comparison against Elements
   * in the source iModel.
   * @deprecated in 1.x. Do not use this. // FIXME<MIKE>: how to better explain this?
   * This method is only called during [[processAll]] when the option
   * [[IModelTransformerOptions.forceExternalSourceAspectProvenance]] is enabled. It is not
   * necessary when using [[processChanges]] since changeset information is sufficient.
   * @note you do not need to call this directly unless processing a subset of an iModel.
   * @throws [[IModelError]] If the required provenance information is not available to detect deletes.
   */
  public async detectElementDeletes(): Promise<void> {
    const sql = `
      SELECT Identifier, Element.Id
      FROM BisCore.ExternalSourceAspect
      WHERE Scope.Id=:scopeId
        AND Kind=:kind
    `;

    nodeAssert(
      !this._options.isReverseSynchronization,
      "synchronizations with processChanges already detect element deletes, don't call detectElementDeletes"
    );

    this.provenanceDb.withPreparedStatement(sql, (stmt) => {
      stmt.bindId("scopeId", this.targetScopeElementId);
      stmt.bindString("kind", ExternalSourceAspect.Kind.Element);
      while (DbResult.BE_SQLITE_ROW === stmt.step()) {
        // ExternalSourceAspect.Identifier is of type string
        const aspectIdentifier = stmt.getValue(0).getString();
        if (!Id64.isId64(aspectIdentifier)) {
          continue;
        }
        const targetElemId = stmt.getValue(1).getId();
        const wasDeletedInSource = !EntityUnifier.exists(this.sourceDb, { entityReference: `e${aspectIdentifier}` });
        if (wasDeletedInSource)
          this.importer.deleteElement(targetElemId);
      }
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

  // if undefined, it can be initialized by calling [[this._cacheSourceChanges]]
  private _hasElementChangedCache?: Set<Id64String> = undefined;
  private _deletedSourceRelationshipData?: Map<Id64String, {
    sourceIdInTarget?: Id64String;
    targetIdInTarget?: Id64String;
    classFullName: Id64String;
    relId?: Id64String;
    provenanceAspectId?: Id64String;
  }> = undefined;

  // TODO: this is a PoC, see if we minimize memory usage
  private _cacheSourceChanges() {
    nodeAssert(this._changeSummaryIds && this._changeSummaryIds.length > 0, "should have changeset data by now");
    this._hasElementChangedCache = new Set();

    const query = `
      SELECT
        ic.ChangedInstance.Id AS InstId
      FROM ecchange.change.InstanceChange ic
      JOIN iModelChange.Changeset imc ON ic.Summary.Id=imc.Summary.Id
      -- TODO: do relationship entities also need this cache optimization?
      WHERE ic.ChangedInstance.ClassId IS (BisCore.Element)
        AND InVirtualSet(:changeSummaryIds, ic.Summary.Id)
        -- ignore deleted, we take care of those in remapDeletedSourceEntities
        -- include inserted since inserted code-colliding elements should be considered
        -- a change so that the colliding element is exported to the target
        AND ic.OpCode<>:opDelete
    `;

    // there is a single mega-query multi-join+coalescing hack that I used originally to get around
    // only being able to run table.Changes() on one changeset at once, but sqlite only supports up to 64
    // tables in a join. Need to talk to core about .Changes being able to take a set of changesets
    // You can find this version in the `federation-guid-optimization-megaquery` branch
    // I wouldn't use it unless we prove via profiling that it speeds things up significantly
    // And even then let's first try scanning the raw changesets instead of applying them as these queries
    // require
    this.sourceDb.withPreparedStatement(query,
      (stmt) => {
        stmt.bindInteger("opDelete", ChangeOpCode.Delete);
        stmt.bindIdSet("changeSummaryIds", this._changeSummaryIds!);
        while (DbResult.BE_SQLITE_ROW === stmt.step()) {
          const instId = stmt.getValue(0).getId();
          this._hasElementChangedCache!.add(instId);
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
    if (this._sourceChangeDataState === "no-changes")
        return false;
    if (this._sourceChangeDataState === "unconnected")
        return true;
    nodeAssert(this._sourceChangeDataState === "has-changes", "change data should be initialized by now");
    if (this._hasElementChangedCache === undefined)
        this._cacheSourceChanges();
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

    // eslint-disable-next-line deprecation/deprecation
    for (const referenceId of entity.getReferenceConcreteIds()) {
      // TODO: probably need to rename from 'id' to 'ref' so these names aren't so ambiguous
      const referenceIdInTarget = this.context.findTargetEntityId(referenceId);
      const alreadyProcessed = EntityReferences.isValid(referenceIdInTarget) || this._skippedEntities.has(referenceId);
      if (alreadyProcessed)
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

  public override onSkipElement(sourceElementId: Id64String): void {
    if (this.context.findTargetElementId(sourceElementId) !== Id64.invalid) {
      // element already has provenance
      return;
    }

    Logger.logInfo(loggerCategory, `Element '${sourceElementId}' won't be exported. Marking its references as resolved`);
    const elementKey: EntityKey = `e${sourceElementId}`;
    this._skippedEntities.add(elementKey);

    // Mark any existing pending references to the skipped element as resolved.
    for (const referencer of this._pendingReferences.getReferencersByEntityKey(elementKey)) {
      const key = PendingReference.from(referencer, elementKey);
      const pendingRef = this._pendingReferences.get(key);
      if (!pendingRef)
        continue;
      pendingRef.resolveReference(elementKey);
      this._pendingReferences.delete(key);
    }
  }

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
    let targetElementId: Id64String;
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

    // if an existing remapping was not yet found, check by FederationGuid
    if (this.context.isBetweenIModels && !Id64.isValid(targetElementId) && sourceElement.federationGuid !== undefined) {
      targetElementId = this._queryElemIdByFedGuid(this.targetDb, sourceElement.federationGuid) ?? Id64.invalid;
      if (Id64.isValid(targetElementId))
        this.context.remapElement(sourceElement.id, targetElementId); // record that the targetElement was found
    }

    // if an existing remapping was not yet found, check by Code as long as the CodeScope is valid (invalid means a missing reference so not worth checking)
    if (!Id64.isValidId64(targetElementId) && Id64.isValidId64(targetElementProps.code.scope)) {
      // respond the same way to undefined code value as the @see Code class, but don't use that class because it trims
      // whitespace from the value, and there are iModels out there with untrimmed whitespace that we ought not to trim
      targetElementProps.code.value = targetElementProps.code.value ?? "";
      const maybeTargetElementId = this.targetDb.elements.queryElementIdByCode(targetElementProps.code as Required<CodeProps>);
      if (undefined !== maybeTargetElementId) {
        const maybeTargetElem = this.targetDb.elements.getElement(maybeTargetElementId);
        if (maybeTargetElem.classFullName === targetElementProps.classFullName) { // ensure code remapping doesn't change the target class
          targetElementId = maybeTargetElementId;
          this.context.remapElement(sourceElement.id, targetElementId); // record that the targetElement was found by Code
        } else {
          targetElementProps.code = Code.createEmpty(); // clear out invalid code
        }
      }
    }

    if (Id64.isValid(targetElementId) && !this.hasElementChanged(sourceElement, targetElementId))
      return;

    this.collectUnmappedReferences(sourceElement);

    // targetElementId will be valid (indicating update) or undefined (indicating insert)
    targetElementProps.id
      = Id64.isValid(targetElementId)
        ? targetElementId
        : undefined;

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
    // FIXME: verify at finalization time that we don't lose provenance on new elements
    // FIXME: make public and improve `initElementProvenance` API for usage by consolidators
    if (!this._options.noProvenance) {
      let provenance: Parameters<typeof this.markLastProvenance>[0] | undefined
        = this._options.forceExternalSourceAspectProvenance || this._elementsWithExplicitlyTrackedProvenance.has(sourceElement.id)
        ? undefined
        : sourceElement.federationGuid;
      if (!provenance) {
        const aspectProps = this.initElementProvenance(sourceElement.id, targetElementProps.id!);
        const aspectId = this.queryScopeExternalSource(aspectProps).aspectId;
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
    // - If both were deleted, [[remapDeletedSourceEntities]] will find and remap the deleted element making this operation valid
    const targetModelId: Id64String = this.context.findTargetElementId(sourceModelId);

    if (!Id64.isValidId64(targetModelId))
      return;

    if (this.exporter.sourceDbChanges?.element.deleteIds.has(sourceModelId)) {
      const isDefinitionPartition = this.targetDb.withPreparedStatement(`
        SELECT 1
        FROM bis.DefinitionPartition
        WHERE ECInstanceId=?
      `, (stmt) => {
        stmt.bindId(1, targetModelId);
        const val: DbResult = stmt.step();
        switch (val) {
          case DbResult.BE_SQLITE_ROW: return true;
          case DbResult.BE_SQLITE_DONE: return false;
          default: assert(false, `unexpected db result: '${stmt}'`);
        }
      });
      if (isDefinitionPartition) {
        // Skipping model deletion because model's partition will also be deleted.
        // It expects that model will be present and will fail if it's missing.
        // Model will be deleted when its partition will be deleted.
        return;
      }
    }

    try {
      this.importer.deleteModel(targetModelId);
    } catch (error) {
      const isDeletionProhibitedErr = error instanceof IModelError && (error.errorNumber === IModelStatus.DeletionProhibited || error.errorNumber === IModelStatus.ForeignKeyConstraint);
      if (!isDeletionProhibitedErr)
        throw error;

      // Transformer tries to delete models before it deletes elements. Definition models cannot be deleted unless all of their modeled elements are deleted first.
      // In case a definition model needs to be deleted we need to skip it for now and register its modeled partition for deletion.
      // The `OnDeleteElement` calls `DeleteElementTree` Which deletes the model together with its partition after deleting all of the modeled elements.
      this.scheduleModeledPartitionDeletion(sourceModelId);
    }
  }

  /** Schedule modeled partition deletion */
  private scheduleModeledPartitionDeletion(sourceModelId: Id64String): void {
    const deletedElements = this.exporter.sourceDbChanges?.element.deleteIds as Set<Id64String>;
    if (!deletedElements.has(sourceModelId)) {
      deletedElements.add(sourceModelId);
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
   * source's changeset has been performed. Also stores all changesets that occurred
   * during the transformation as "pending synchronization changeset indices"
   *
   * You generally should not call this function yourself and use [[processChanges]] instead.
   * It is public for unsupported use cases of custom synchronization transforms.
   * @note if you are not running processChanges in this transformation, this will fail
   * without setting the `force` option to `true`
   */
  public updateSynchronizationVersion({ force = false } = {}) {
    if (!force && (this._sourceChangeDataState !== "has-changes" && !this._isFirstSynchronization))
      return;

    nodeAssert(this._targetScopeProvenanceProps);

    const sourceVersion = `${this.sourceDb.changeset.id};${this.sourceDb.changeset.index}`;
    const targetVersion = `${this.targetDb.changeset.id};${this.targetDb.changeset.index}`;

    if (this._isFirstSynchronization) {
      this._targetScopeProvenanceProps.version = sourceVersion;
      this._targetScopeProvenanceProps.jsonProperties.reverseSyncVersion = targetVersion;
    } else if (this._options.isReverseSynchronization) {
      const oldVersion = this._targetScopeProvenanceProps.jsonProperties.reverseSyncVersion;
      Logger.logInfo(loggerCategory, `updating reverse version from ${oldVersion} to ${sourceVersion}`);
      this._targetScopeProvenanceProps.jsonProperties.reverseSyncVersion = sourceVersion;
    } else if (!this._options.isReverseSynchronization) {
      Logger.logInfo(loggerCategory, `updating sync version from ${this._targetScopeProvenanceProps.version} to ${sourceVersion}`);
      this._targetScopeProvenanceProps.version = sourceVersion;
    }

    if (this._isSynchronization) {
      assert(
        this.targetDb.changeset.index !== undefined && this._startingChangesetIndices  !== undefined,
        "updateSynchronizationVersion was called without change history",
      );

      const jsonProps = this._targetScopeProvenanceProps.jsonProperties;

      Logger.logTrace(loggerCategory, `previous pendingReverseSyncChanges: ${jsonProps.pendingReverseSyncChangesetIndices}`);
      Logger.logTrace(loggerCategory, `previous pendingSyncChanges: ${jsonProps.pendingSyncChangesetIndices}`);

      const [syncChangesetsToClear, syncChangesetsToUpdate]
        = this._isReverseSynchronization
        ? [jsonProps.pendingReverseSyncChangesetIndices, jsonProps.pendingSyncChangesetIndices]
        : [jsonProps.pendingSyncChangesetIndices, jsonProps.pendingReverseSyncChangesetIndices];

      // NOTE that as documented in [[processChanges]], this assumes that right after
      // transformation finalization, the work will be saved immediately, otherwise we've
      // just marked this changeset as a synchronization to ignore, and the user can add other
      // stuff to it which would break future synchronizations
      // FIXME: force save for the user to prevent that
      for (let i = this._startingChangesetIndices.target + 1; i <= this.targetDb.changeset.index + 1; i++)
        syncChangesetsToUpdate.push(i);
      syncChangesetsToClear.length = 0;

      // if reverse sync then we may have received provenance changes which should be marked as sync changes
      if (this._isReverseSynchronization) {
        nodeAssert(this.sourceDb.changeset.index, "changeset didn't exist");
        for (let i = this._startingChangesetIndices.source + 1; i <= this.sourceDb.changeset.index + 1; i++)
          jsonProps.pendingReverseSyncChangesetIndices.push(i);
      }

      Logger.logTrace(loggerCategory, `new pendingReverseSyncChanges: ${jsonProps.pendingReverseSyncChangesetIndices}`);
      Logger.logTrace(loggerCategory, `new pendingSyncChanges: ${jsonProps.pendingSyncChangesetIndices}`);
    }

    this.provenanceDb.elements.updateAspect({
      ...this._targetScopeProvenanceProps,
      jsonProperties: JSON.stringify(this._targetScopeProvenanceProps.jsonProperties) as any,
    });
  }

  // FIXME<MIKE>: is this necessary when manually using lowlevel transform APIs? (document if so)
  private finalizeTransformation() {
    this.updateSynchronizationVersion();

    if (this._partiallyCommittedEntities.size > 0) {
      // FIXME: throw in this case if danglingReferenceBehavior === reject
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

    // TODO: ignore if we remove change cache usage
    if (!this._options.noDetachChangeCache) {
      if (ChangeSummaryManager.isChangeCacheAttached(this.sourceDb))
        ChangeSummaryManager.detachChangeCache(this.sourceDb);
    }

    // this internal is guaranteed stable for just transformer usage
    /* eslint-disable @itwin/no-internal */
    if ("codeValueBehavior" in this.sourceDb as any) {
      (this.sourceDb as any).codeValueBehavior = "trim-unicode-whitespace";
      (this.targetDb as any).codeValueBehavior = "trim-unicode-whitespace";
    }
    /* eslint-enable @itwin/no-internal */
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
    const targetRelationshipProps = this.onTransformRelationship(sourceRelationship);
    const targetRelationshipInstanceId = this.importer.importRelationship(targetRelationshipProps);

    if (!this._options.noProvenance && Id64.isValid(targetRelationshipInstanceId)) {
      let provenance: Parameters<typeof this.markLastProvenance>[0] | undefined
        = !this._options.forceExternalSourceAspectProvenance
          ? sourceFedGuid && targetFedGuid && `${sourceFedGuid}/${targetFedGuid}`
          : undefined;
      if (!provenance) {
        const aspectProps = this.initRelationshipProvenance(sourceRelationship, targetRelationshipInstanceId);
        aspectProps.id = this.queryScopeExternalSource(aspectProps).aspectId;
        if (undefined === aspectProps.id) {
          aspectProps.id = this.provenanceDb.elements.insertAspect(aspectProps);
        }
        provenance = aspectProps as MarkRequired<ExternalSourceAspectProps, "id">;
      }
      this.markLastProvenance(provenance, { isRelationship: true });
    }
  }

  /** Override of [IModelExportHandler.onDeleteRelationship]($transformer) that is called when [IModelExporter]($transformer) detects that a [Relationship]($backend) has been deleted from the source iModel.
   * This override propagates the delete to the target iModel via [IModelImporter.deleteRelationship]($transformer).
   */
  public override onDeleteRelationship(sourceRelInstanceId: Id64String): void {
    nodeAssert(this._deletedSourceRelationshipData, "should be defined at initialization by now");

    const deletedRelData = this._deletedSourceRelationshipData.get(sourceRelInstanceId);
    if (!deletedRelData) {
      // this can occur if both the source and target deleted it
      Logger.logWarning(loggerCategory, "tried to delete a relationship that wasn't in change data");
      return;
    }

    const relArg = deletedRelData.relId ?? {
      sourceId: deletedRelData.sourceIdInTarget,
      targetId: deletedRelData.targetIdInTarget,
    } as SourceAndTarget;

    // FIXME: make importer.deleteRelationship not need full props
    const targetRelationship = this.targetDb.relationships.tryGetInstance(
      deletedRelData.classFullName,
      relArg,
    );

    if (targetRelationship) {
      this.importer.deleteRelationship(targetRelationship.toJSON());
    }

    if (deletedRelData.provenanceAspectId) {
      try {
        this.provenanceDb.elements.deleteAspect(deletedRelData.provenanceAspectId);
      } catch (error: any) {
        // This aspect may no longer exist if it was deleted at some other point during the transformation. This is fine.
        if (error.errorNumber === IModelStatus.NotFound)
          return;
        throw error;
      }
    }
  }

  private _yieldManager = new YieldManager();

  /** Detect Relationship deletes using ExternalSourceAspects in the target iModel and a *brute force* comparison against relationships in the source iModel.
   * @deprecated in 1.x. Don't use this anymore
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
          // this function exists only to support some in-imodel transformations, which must
          // use the old (external source aspect) provenance method anyway so we don't need to support
          // new provenance
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

  public override shouldExportElementAspect(aspect: ElementAspect) {
    // This override is needed to ensure that aspects are not exported if their element is not exported.
    // This is needed in case DetachedExportElementAspectsStrategy is used.
    return this.context.findTargetElementId(aspect.element.id) !== Id64.invalid;
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
  private _sourceChangeDataState: ChangeDataState = "uninited";

  /**
   * Initialize prerequisites of processing, you must initialize with an [[InitOptions]] if you
   * are intending to process changes, but prefer using [[processChanges]] explicitly since it calls this.
   * @note Called by all `process*` functions implicitly.
   * Overriders must call `super.initialize()` first
   */
  public async initialize(args?: InitOptions): Promise<void> {
    if (this._initialized)
      return;

    await this.context.initialize();
    await this._tryInitChangesetData(args);

    await this.exporter.initialize(this.getExportInitOpts(args ?? {}));

    // Exporter must be initialized prior to `initFromExternalSourceAspects` in order to handle entity recreations.
    // eslint-disable-next-line deprecation/deprecation
    await this.initFromExternalSourceAspects(args);

    this._initialized = true;
  }

  private async _tryInitChangesetData(args?: InitOptions) {
    if (!args || this.sourceDb.iTwinId === undefined || this.sourceDb.changeset.index === undefined) {
      this._sourceChangeDataState = "unconnected";
      return;
    }

    const noChanges = this._synchronizationVersion.index === this.sourceDb.changeset.index;
    if (noChanges) {
      this._sourceChangeDataState = "no-changes";
      this._changeSummaryIds = [];
      return;
    }

    // NOTE: that we do NOT download the changesummary for the last transformed version, we want
    // to ignore those already processed changes
    const startChangesetIndexOrId
      = args.startChangeset?.index
      ?? args.startChangeset?.id
      ?? this._synchronizationVersion.index + 1;
    const endChangesetId = this.sourceDb.changeset.id;

    const [startChangesetIndex, endChangesetIndex] = await Promise.all(
      ([startChangesetIndexOrId, endChangesetId])
        .map(async (indexOrId) => typeof indexOrId === "number"
          ? indexOrId
          : IModelHost.hubAccess
            .queryChangeset({
              iModelId: this.sourceDb.iModelId,
              // eslint-disable-next-line deprecation/deprecation
              changeset: { id: indexOrId },
              accessToken: args.accessToken,
            })
            .then((changeset) => changeset.index)
        )
    );

    const missingChangesets = startChangesetIndex > this._synchronizationVersion.index + 1;
    if (
      !this._options.ignoreMissingChangesetsInSynchronizations
      && startChangesetIndex !== this._synchronizationVersion.index + 1
      && this._synchronizationVersion.index !== -1
    ) {
      throw Error(`synchronization is ${missingChangesets ? "missing changesets" : ""},`
        + " startChangesetId should be"
        + " exactly the first changeset *after* the previous synchronization to not miss data."
        + ` You specified '${startChangesetIndexOrId}' which is changeset #${startChangesetIndex}`
        + ` but the previous synchronization for this targetScopeElement was '${this._synchronizationVersion.id}'`
        + ` which is changeset #${this._synchronizationVersion.index}. The transformer expected`
        + ` #${this._synchronizationVersion.index + 1}.`
      );
    }

    nodeAssert(this._targetScopeProvenanceProps, "_targetScopeProvenanceProps should be set by now");

    const changesetsToSkip = this._isReverseSynchronization
      ? this._targetScopeProvenanceProps.jsonProperties.pendingReverseSyncChangesetIndices
      : this._targetScopeProvenanceProps.jsonProperties.pendingSyncChangesetIndices;

    Logger.logTrace(loggerCategory, `changesets to skip: ${changesetsToSkip}`);
    this._changesetRanges = rangesFromRangeAndSkipped(startChangesetIndex, endChangesetIndex, changesetsToSkip);
    Logger.logTrace(loggerCategory, `ranges: ${this._changesetRanges}`);

    for (const [first, end] of this._changesetRanges) {
      this._changeSummaryIds = await ChangeSummaryManager.createChangeSummaries({
        accessToken: args.accessToken,
        iModelId: this.sourceDb.iModelId,
        iTwinId: this.sourceDb.iTwinId,
        range: { first, end },
      });
    }

    ChangeSummaryManager.attachChangeCache(this.sourceDb);
    this._sourceChangeDataState = "has-changes";
  }

  /** Export everything from the source iModel and import the transformed entities into the target iModel.
 * @note [[processSchemas]] is not called automatically since the target iModel may want a different collection of schemas.
 */
  public async processAll(): Promise<void> {
    this.logSettings();
    this.initScopeProvenance();
    await this.initialize();
    await this.exporter.exportCodeSpecs();
    await this.exporter.exportFonts();
    // The RepositoryModel and root Subject of the target iModel should not be transformed.
    await this.exporter.exportChildElements(IModel.rootSubjectId); // start below the root Subject
    await this.exporter.exportModelContents(IModel.repositoryModelId, Element.classFullName, true); // after the Subject hierarchy, process the other elements of the RepositoryModel
    await this.exporter.exportSubModels(IModel.repositoryModelId); // start below the RepositoryModel
    await this.exporter["exportAllAspects"](); // eslint-disable-line @typescript-eslint/dot-notation
    await this.exporter.exportRelationships(ElementRefersToElements.classFullName);
    await this.processDeferredElements(); // eslint-disable-line deprecation/deprecation
    // FIXME: add a deprecated option to force run these, don't otherwise
    if (this.shouldDetectDeletes()) {
      await this.detectElementDeletes();
      await this.detectRelationshipDeletes();
    }

    if (this._options.optimizeGeometry)
      this.importer.optimizeGeometry(this._options.optimizeGeometry);

    this.importer.computeProjectExtents();
    this.finalizeTransformation();
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
        const isGuidOrGuidPair = entityId.includes("-");
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
   * @deprecated in 0.1.x, this is buggy, and it is now equivalently efficient to simply restart the transformation
   * from the original changeset
   *
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
   * @deprecated in 0.1.x, this is buggy, and it is now equivalently efficient to simply restart the transformation
   * from the original changeset
   *
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
   * @note the transformer assumes that you saveChanges after processing changes. You should not
   * modify the iModel after processChanges until saveChanges, failure to do so may result in corrupted
   * data loss in future branch operations
   * @param accessToken A valid access token string
   * @param startChangesetId Include changes from this changeset up through and including the current changeset.
   * @note if no startChangesetId or startChangeset option is provided, the next unsynchronized changeset
   * will automatically be determined and used
   * @note To form a range of versions to process, set `startChangesetId` for the start (inclusive) of the desired range and open the source iModel as of the end (inclusive) of the desired range.
   */
  public async processChanges(options: ProcessChangesOptions): Promise<void>;
  /**
   * @deprecated in 0.1.x, use a single [[ProcessChangesOptions]] object instead
   * This overload follows the older behavior of defaulting an undefined startChangesetId to the
   * current changeset.
   */
  public async processChanges(accessToken: AccessToken, startChangesetId?: string): Promise<void>;
  public async processChanges(optionsOrAccessToken: AccessToken | ProcessChangesOptions, startChangesetId?: string): Promise<void> {
    this._isSynchronization = true;
    this.initScopeProvenance();

    const args: ProcessChangesOptions =
      typeof optionsOrAccessToken === "string"
      ? {
          accessToken: optionsOrAccessToken,
          startChangeset: startChangesetId
            ? { id: startChangesetId }
            : { index: this._synchronizationVersion.index + 1 },
        }
      : optionsOrAccessToken
    ;

    this.logSettings();

    await this.initialize(args);
    // must wait for initialization of synchronization provenance data
    await this.exporter.exportChanges(this.getExportInitOpts(args));
    await this.processDeferredElements(); // eslint-disable-line deprecation/deprecation
    await this.exporter["exportAllAspects"](); // eslint-disable-line @typescript-eslint/dot-notation

    if (this._options.optimizeGeometry)
      this.importer.optimizeGeometry(this._options.optimizeGeometry);

    this.importer.computeProjectExtents();
    this.finalizeTransformation();

    const defaultSaveTargetChanges = () => {
      this.targetDb.saveChanges();
    };

    await (args.saveTargetChanges ?? defaultSaveTargetChanges)(this);
  }

  /** Changeset data must be initialized in order to build correct changeOptions.
   * Call [[IModelTransformer.initialize]] for initialization of synchronization provenance data
   */
  private getExportInitOpts(opts: InitOptions): ExporterInitOptions {
    if (!this._isSynchronization)
      return {};

    return {
      accessToken: opts.accessToken,
      ...this._changesetRanges
        ? { changesetRanges: this._changesetRanges }
        : opts.startChangeset
        ? { startChangeset: opts.startChangeset }
        : { startChangeset: { index: this._synchronizationVersion.index + 1 } },
    };
  }

  /** Combine an array of source elements into a single target element.
   * All source and target elements must be created before calling this method.
   * The "combine" operation is a remap and no properties from the source elements will be exported into the target
   * and provenance will be explicitly tracked by ExternalSourceAspects
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
    await this.initialize();
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
    await this.initialize();
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
    // eslint-disable-next-line deprecation/deprecation
    const referenceIds = sourceElement.getReferenceConcreteIds();
    referenceIds.forEach((referenceId) => {
      // TODO: consider going through all definition elements at once and remapping them to themselves
      if (!EntityReferences.isValid(this.context.findTargetEntityId(referenceId))) {
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
    if (sourceElement instanceof GeometricElement) {
      const is3d = sourceElement instanceof GeometricElement3d;
      const placementClass = is3d ? Placement3d : Placement2d;
      const placement = (placementClass).fromJSON((targetElementProps as GeometricElementProps).placement as any);
      if (placement.isValid) {
        nodeAssert(this._transform3d);
        placement.multiplyTransform(this._transform3d);
        (targetElementProps as GeometricElementProps).placement = placement;
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

