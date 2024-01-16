/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import {
  BriefcaseDb,
  BriefcaseManager,
  ChangedECInstance,
  ChangesetECAdaptor,
  DefinitionModel,
  ECSqlStatement,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  ElementMultiAspect,
  ElementRefersToElements,
  ElementUniqueAspect,
  GeometricElement,
  IModelDb,
  IModelHost,
  IModelJsNative,
  Model,
  PartialECChangeUnifier,
  RecipeDefinitionElement,
  Relationship,
  SqliteChangeOp,
  SqliteChangesetReader,
} from "@itwin/core-backend";
import {
  assert,
  DbResult,
  Id64String,
  IModelStatus,
  Logger,
  YieldManager,
} from "@itwin/core-bentley";
import {
  ChangesetFileProps,
  CodeSpec,
  FontProps,
  IModel,
  IModelError,
} from "@itwin/core-common";
import {
  ECVersion,
  Schema,
  SchemaKey,
  SchemaLoader,
} from "@itwin/ecschema-metadata";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import * as nodeAssert from "assert";
import {
  ElementAspectsHandler,
  ExportElementAspectsStrategy,
} from "./ExportElementAspectsStrategy";
import { ExportElementAspectsWithElementsStrategy } from "./ExportElementAspectsWithElementsStrategy";

const loggerCategory = TransformerLoggerCategory.IModelExporter;

export interface ExportChangesArgs extends InitArgs {
  accessToken?: AccessToken;
  /**
   * A changeset id or index signifiying the inclusive start of changes
   * to include. The end is implicitly the changeset of the source iModel
   * @note mutually exclusive with @see changesetRanges
   */
  startChangeset?: ChangesetIndexOrId;
  /**
   * An array of changeset index ranges, e.g. [[2,2], [4,5]] is [2,4,5]
   * @note mutually exclusive with @see startChangeset
   */
  changesetRanges?: [number, number][];
}

export interface ChangedInstanceIdsInitOptions extends ExportChangesArgs {
  iModel: BriefcaseDb;
}

/**
 * @beta
 * The (optional) result of [[IModelExportHandler.onExportSchema]]
 */
export interface ExportSchemaResult {
  /** set this property to notify subclasses where you wrote a schema for later import */
  schemaPath?: string;
}

/**
 * Arguments for [[IModelExporter.initialize]], usually in case you want to query changedata early
 * such as in the case of the IModelTransformer
 * @beta
 */
export type ExporterInitOptions = ExportChangesOptions;

/**
 * Arguments for [[IModelExporter.exportChanges]]
 * @public
 */
export type ExportChangesOptions = {
  skipPropagateChangesToRootElements?: boolean;
} /**
 * an array of ChangesetFileProps which are used to read the changesets and populate the ChangedInstanceIds using [[ChangedInstanceIds.initialize]] in [[IModelExporter.exportChanges]]
 * @note mutually exclusive with @see changesetRanges, @see startChangeset and @see changedInstanceIds, so define one of the four, never more
 */ & (
  | { csFileProps: ChangesetFileProps[] }
  /**
   * Class instance that contains modified elements between 2 versions of an iModel.
   * If this parameter is not provided, then [[ChangedInstanceIds.initialize]] in [[IModelExporter.exportChanges]]
   * will be called to discover changed elements.
   * @note mutually exclusive with @see changesetRanges, @see csFileProps and @see startChangeset, so define one of the four, never more
   */
  | { changedInstanceIds: ChangedInstanceIds }
  /**
   * An ordered array of changeset index ranges, e.g. [[2,2], [4,5]] is [2,4,5]
   * @note mutually exclusive with @see changedInstanceIds, @see csFileProps and @see startChangeset, so define one of the four, never more
   */
  | { changesetRanges: [number, number][] }
  /**
   * Include changes from this changeset up through and including the current changeset.
   * @note To form a range of versions to process, set `startChangeset` for the start (inclusive)
   * of the desired range and open the source iModel as of the end (inclusive) of the desired range.
   * @default the current changeset of the sourceDb, if undefined
   */
  | { startChangeset: { id?: string; index?: number } }
  | {}
);

/**
 * Arguments for [[IModelExporter.initialize]], usually in case you want to query changedata early
 * such as in the case of the IModelTransformer
 * @beta
 */
export type ExporterInitOptions = ExportChangesOptions;

/**
 * Arguments for [[IModelExporter.exportChanges]]
 * @public
 */
export type ExportChangesOptions = Omit<InitOptions, "startChangeset"> & (
  /**
   * an array of ChangesetFileProps which are used to read the changesets and populate the ChangedInstanceIds using [[ChangedInstanceIds.initialize]] in [[IModelExporter.exportChanges]]
   * @note mutually exclusive with @see changesetRanges, @see startChangeset and @see changedInstanceIds, so define one of the four, never more
   */
  (| { csFileProps: ChangesetFileProps[] }
    /**
     * Class instance that contains modified elements between 2 versions of an iModel.
     * If this parameter is not provided, then [[ChangedInstanceIds.initialize]] in [[IModelExporter.exportChanges]]
     * will be called to discover changed elements.
     * @note mutually exclusive with @see changesetRanges, @see csFileProps and @see startChangeset, so define one of the four, never more
     */
    | { changedInstanceIds: ChangedInstanceIds }
    /**
     * An ordered array of changeset index ranges, e.g. [[2,2], [4,5]] is [2,4,5]
     * @note mutually exclusive with @see changedInstanceIds, @see csFileProps and @see startChangeset, so define one of the four, never more
     */
    | { changesetRanges: [number, number][] }
    /**
     * Include changes from this changeset up through and including the current changeset.
     * @note To form a range of versions to process, set `startChangeset` for the start (inclusive)
     * of the desired range and open the source iModel as of the end (inclusive) of the desired range.
     * @default the current changeset of the sourceDb, if undefined
     */
    | { startChangeset: { id?: string; index?: number } }
    | {}
  );

/** Handles the events generated by IModelExporter.
 * @note Change information is available when `IModelExportHandler` methods are invoked via [IModelExporter.exportChanges]($transformer), but not available when invoked via [IModelExporter.exportAll]($transformer).
 * @note The handler is intended to be owned by (registered with) and called from the IModelExporter exclusively
 * @see [iModel Transformation and Data Exchange]($docs/learning/transformer/index.md), [IModelExporter]($transformer)
 * @beta
 */
export abstract class IModelExportHandler {
  /** If `true` is returned, then the CodeSpec will be exported.
   * @note This method can optionally be overridden to exclude an individual CodeSpec from the export. The base implementation always returns `true`.
   */
  public shouldExportCodeSpec(_codeSpec: CodeSpec): boolean {
    return true;
  }

  /** Called when a CodeSpec should be exported.
   * @param codeSpec The CodeSpec to export
   * @param isUpdate If defined, then `true` indicates an UPDATE operation while `false` indicates an INSERT operation. If not defined, then INSERT vs. UPDATE is not known.
   * @note This should be overridden to actually do the export.
   */
  public onExportCodeSpec(
    _codeSpec: CodeSpec,
    _isUpdate: boolean | undefined
  ): void {}

  /** Called when a font should be exported.
   * @param font The font to export
   * @param isUpdate If defined, then `true` indicates an UPDATE operation while `false` indicates an INSERT operation. If not defined, then INSERT vs. UPDATE is not known.
   * @note This should be overridden to actually do the export.
   */
  public onExportFont(_font: FontProps, _isUpdate: boolean | undefined): void {}

  /** Called when a model should be exported.
   * @param model The model to export
   * @param isUpdate If defined, then `true` indicates an UPDATE operation while `false` indicates an INSERT operation. If not defined, then INSERT vs. UPDATE is not known.
   * @note This should be overridden to actually do the export.
   */
  public onExportModel(_model: Model, _isUpdate: boolean | undefined): void {}

  /** Called when a model should be deleted. */
  public onDeleteModel(_modelId: Id64String): void {}

  /** If `true` is returned, then the element will be exported.
   * @note This method can optionally be overridden to exclude an individual Element (and its children and ElementAspects) from the export. The base implementation always returns `true`.
   */
  public shouldExportElement(_element: Element): boolean {
    return true;
  }

  /** Called when element is skipped instead of exported.
   * @note When an element is skipped, exporter will not export any of its child elements. Because of this, [[onSkipElement]] will not be invoked for any children of a "skipped" element.
   */
  public onSkipElement(_elementId: Id64String): void {}

  /** Called when an element should be exported.
   * @param element The element to export
   * @param isUpdate If defined, then `true` indicates an UPDATE operation while `false` indicates an INSERT operation. If not defined, then INSERT vs. UPDATE is not known.
   * @note This should be overridden to actually do the export.
   */
  public onExportElement(
    _element: Element,
    _isUpdate: boolean | undefined
  ): void {}

  /**
   * Do any asynchronous actions before exporting an element
   * @note Do not implement this handler manually, it is internal, it will be removed.
   *       This will become a part of onExportElement once that becomes async
   * @internal
   */
  public async preExportElement(_element: Element): Promise<void> {}

  /** Called when an element should be deleted. */
  public onDeleteElement(_elementId: Id64String): void {}

  /** If `true` is returned, then the ElementAspect will be exported.
   * @note This method can optionally be overridden to exclude an individual ElementAspect from the export. The base implementation always returns `true`.
   */
  public shouldExportElementAspect(_aspect: ElementAspect): boolean {
    return true;
  }

  /** Called when an ElementUniqueAspect should be exported.
   * @param aspect The ElementUniqueAspect to export
   * @param isUpdate If defined, then `true` indicates an UPDATE operation while `false` indicates an INSERT operation. If not defined, then INSERT vs. UPDATE is not known.
   * @note This should be overridden to actually do the export.
   */
  public onExportElementUniqueAspect(
    _aspect: ElementUniqueAspect,
    _isUpdate: boolean | undefined
  ): void {}

  /** Called when ElementMultiAspects should be exported.
   * @note This should be overridden to actually do the export.
   */
  public onExportElementMultiAspects(_aspects: ElementMultiAspect[]): void {}

  /** If `true` is returned, then the relationship will be exported.
   * @note This method can optionally be overridden to exclude an individual CodeSpec from the export. The base implementation always returns `true`.
   */
  public shouldExportRelationship(_relationship: Relationship): boolean {
    return true;
  }

  /** Called when a Relationship should be exported.
   * @param relationship The Relationship to export
   * @param isUpdate If defined, then `true` indicates an UPDATE operation while `false` indicates an INSERT operation. If not defined, then INSERT vs. UPDATE is not known.
   * @note This should be overridden to actually do the export.
   */
  public onExportRelationship(
    _relationship: Relationship,
    _isUpdate: boolean | undefined
  ): void {}

  /** Called when a relationship should be deleted. */
  public onDeleteRelationship(_relInstanceId: Id64String): void {}

  /** If `true` is returned, then the schema will be exported.
   * @note This method can optionally be overridden to exclude an individual schema from the export. The base implementation always returns `true`.
   */
  public shouldExportSchema(_schemaKey: SchemaKey): boolean {
    return true;
  }

  /** Called when a schema should be exported.
   * @param schema The schema to export
   * @note This should be overridden to actually do the export.
   * @note return an [[ExportSchemaResult]] with a `schemaPath` property to notify overrides that call `super`
   *       where a schema was written for import.
   */
  public async onExportSchema(
    _schema: Schema
  ): Promise<void | ExportSchemaResult> {}

  /** This method is called when IModelExporter has made incremental progress based on the [[IModelExporter.progressInterval]] setting.
   * This method is `async` to make it easier to integrate with asynchronous status and health reporting services.
   * @note A subclass may override this method to report custom progress. The base implementation does nothing.
   */
  public async onProgress(): Promise<void> {}
}

/** Base class for exporting data from an iModel.
 * @note Most uses cases will not require a custom subclass of `IModelExporter`. Instead, it is more typical to subclass/customize [IModelExportHandler]($transformer).
 * @see [iModel Transformation and Data Exchange]($docs/learning/transformer/index.md), [[registerHandler]], [IModelTransformer]($transformer), [IModelImporter]($transformer)
 * @beta
 */
export class IModelExporter {
  /** The read-only source iModel. */
  public readonly sourceDb: IModelDb;
  /** A flag that indicates whether element GeometryStreams are loaded or not.
   * @note As an optimization, exporters that don't need geometry can set this flag to `false`. The default is `true`.
   * @note The transformer by default sets this to `false` as an optimization.
   * @note This implies the `wantBRepData` option when loading elements.
   * @see [ElementLoadProps.wantGeometry]($common)
   */
  public wantGeometry: boolean = true;
  /** A flag that indicates whether template models should be exported or not. The default is `true`.
   * @note If only exporting *instances* then template models can be skipped since they are just definitions that are cloned to create new instances.
   * @see [Model.isTemplate]($backend)
   */
  public wantTemplateModels: boolean = true;
  /** A flag that indicates whether *system* schemas should be exported or not. The default is `true` (previously false).
   * This can be set to false for the legacy default behavior, but it may cause errors during schema processing in some cases.
   * @see [[exportSchemas]]
   */
  public wantSystemSchemas: boolean = true;
  /** A flag that determines whether this IModelExporter should visit Elements or not. The default is `true`.
   * @note This flag is available as an optimization when the exporter doesn't need to visit elements, so can skip loading them.
   */
  public visitElements: boolean = true;
  /** A flag that determines whether this IModelExporter should visit Relationships or not. The default is `true`.
   * @note This flag is available as an optimization when the exporter doesn't need to visit relationships, so can skip loading them.
   */
  public visitRelationships: boolean = true;
  /** The number of entities exported before incremental progress should be reported via the [[onProgress]] callback. */
  public progressInterval: number = 1000;
  /** Tracks the current total number of entities exported. */
  private _progressCounter: number = 0;
  /** Optionally cached entity change information */
  private _sourceDbChanges?: ChangedInstanceIds;
  /**
   * Retrieve the cached entity change information.
   * @note This will only be initialized after [IModelExporter.exportChanges] is invoked.
   */
  public get sourceDbChanges(): ChangedInstanceIds | undefined {
    return this._sourceDbChanges;
  }
  /** The handler called by this IModelExporter. */
  private _handler: IModelExportHandler | undefined;
  /** The handler called by this IModelExporter. */
  protected get handler(): IModelExportHandler {
    if (undefined === this._handler) {
      throw new Error("IModelExportHandler not registered");
    }

    return this._handler;
  }

  /** The set of CodeSpecs to exclude from the export. */
  private _excludedCodeSpecNames = new Set<string>();
  /** The set of specific Elements to exclude from the export. */
  private _excludedElementIds = new Set<Id64String>();
  /** The set of Categories where Elements in that Category will be excluded from transformation to the target iModel. */
  private _excludedElementCategoryIds = new Set<Id64String>();
  /** The set of classes of Elements that will be excluded (polymorphically) from transformation to the target iModel. */
  private _excludedElementClasses = new Set<typeof Element>();
  /** The set of classes of Relationships that will be excluded (polymorphically) from transformation to the target iModel. */
  private _excludedRelationshipClasses = new Set<typeof Relationship>();

  /** Strategy for how ElementAspects are exported */
  private _exportElementAspectsStrategy: ExportElementAspectsStrategy;

  /** Construct a new IModelExporter
   * @param sourceDb The source IModelDb
   * @see registerHandler
   */
  public constructor(
    sourceDb: IModelDb,
    elementAspectsStrategy: new (
      source: IModelDb,
      handler: ElementAspectsHandler
    ) => ExportElementAspectsStrategy = ExportElementAspectsWithElementsStrategy
  ) {
    this.sourceDb = sourceDb;
    this._exportElementAspectsStrategy = new elementAspectsStrategy(
      this.sourceDb,
      {
        onExportElementMultiAspects: (aspects) =>
          this.handler.onExportElementMultiAspects(aspects),
        onExportElementUniqueAspect: (aspect, isUpdate) =>
          this.handler.onExportElementUniqueAspect(aspect, isUpdate),
        shouldExportElementAspect: (aspect) =>
          this.handler.shouldExportElementAspect(aspect),
        trackProgress: async () => this.trackProgress(),
      }
    );
  }

  /**
   * Initialize prerequisites of exporting. This is implicitly done by any `export*` calls that need initialization
   * which is currently just `exportChanges`.
   * Prefer to not call this explicitly (e.g. just call [[IModelExporter.exportChanges]])
   * @note that if you do call this explicitly, you must do so with the same options that
   * you pass to [[IModelExporter.exportChanges]]
   */
  public async initialize(options: ExporterInitOptions): Promise<void> {
    if (!this.sourceDb.isBriefcaseDb() || this._sourceDbChanges) return;

    this._sourceDbChanges = await ChangedInstanceIds.initialize({
      iModel: this.sourceDb,
      ...options,
    });
    if (this._sourceDbChanges === undefined) return;

    this._exportElementAspectsStrategy.setAspectChanges(
      this._sourceDbChanges.aspect
    );
  }

  /**
   * Initialize prerequisites of exporting. This is implicitly done by any `export*` calls that need initialization
   * which is currently just `exportChanges`.
   * Prefer to not call this explicitly (e.g. just call [[IModelExporter.exportChanges]])
   * @note that if you do call this explicitly, you must do so with the same options that
   * you pass to [[IModelExporter.exportChanges]]
   */
  public async initialize(options: ExporterInitOptions): Promise<void> {
    if (!this.sourceDb.isBriefcaseDb() || this._sourceDbChanges) return;

    this._sourceDbChanges = options.changedInstanceIds
      ?? await ChangedInstanceIds.initialize({ iModel: this.sourceDb, ...options });
  }

  /** Register the handler that will be called by IModelExporter. */
  public registerHandler(handler: IModelExportHandler): void {
    this._handler = handler;
  }

  /** Add a rule to exclude a CodeSpec */
  public excludeCodeSpec(codeSpecName: string): void {
    this._excludedCodeSpecNames.add(codeSpecName);
  }

  /** Add a rule to exclude a specific Element. */
  public excludeElement(elementId: Id64String): void {
    this._excludedElementIds.add(elementId);
  }

  /** Add a rule to exclude all Elements in a specified Category. */
  public excludeElementsInCategory(categoryId: Id64String): void {
    this._excludedElementCategoryIds.add(categoryId);
  }

  /** Add a rule to exclude all Elements of a specified class. */
  public excludeElementClass(classFullName: string): void {
    this._excludedElementClasses.add(
      this.sourceDb.getJsClass<typeof Element>(classFullName)
    );
  }

  /** Add a rule to exclude all ElementAspects of a specified class. */
  public excludeElementAspectClass(classFullName: string): void {
    this._exportElementAspectsStrategy.excludeElementAspectClass(classFullName);
  }

  /** Add a rule to exclude all Relationships of a specified class. */
  public excludeRelationshipClass(classFullName: string): void {
    this._excludedRelationshipClasses.add(
      this.sourceDb.getJsClass<typeof Relationship>(classFullName)
    );
  }

  /** Export all entity instance types from the source iModel.
   * @note [[exportSchemas]] must be called separately.
   */
  public async exportAll(): Promise<void> {
    await this.initialize({});

    await this.exportCodeSpecs();
    await this.exportFonts();
    await this.exportModel(IModel.repositoryModelId);
    await this.exportRelationships(ElementRefersToElements.classFullName);
  }

  /** Export changes from the source iModel.
   * Inserts, updates, and deletes are determined by inspecting the changeset(s).
   * @note To form a range of versions to process, set `startChangesetId` for the start (inclusive) of the desired
   *       range and open the source iModel as of the end (inclusive) of the desired range.
   * @note the changedInstanceIds are just for this call to exportChanges, so you must continue to pass it in
   *       for consecutive calls
   */
  public async exportChanges(args?: ExportChangesOptions): Promise<void> {
    if (!this.sourceDb.isBriefcaseDb())
      throw new IModelError(
        IModelStatus.BadRequest,
        "Must be a briefcase to export changes"
      );

    if ("" === this.sourceDb.changeset.id) {
      await this.exportAll(); // no changesets, so revert to exportAll
      return;
    }

    const startChangeset =
      args && "startChangeset" in args ? args.startChangeset : undefined;

    const initOpts: ExporterInitOptions = {
      startChangeset: { id: startChangeset?.id },
    };

    await this.initialize(initOpts);
    // _sourceDbChanges are initialized in this.initialize
    nodeAssert(
      this._sourceDbChanges !== undefined,
      "sourceDbChanges must be initialized."
    );

    await this.exportCodeSpecs();
    await this.exportFonts();
    if (initOpts.skipPropagateChangesToRootElements) {
      await this.exportModelContents(IModel.repositoryModelId);
      await this.exportSubModels(IModel.repositoryModelId);
    } else {
      await this.exportModel(IModel.repositoryModelId);
    }
    await this.exportAllAspects();
    await this.exportRelationships(ElementRefersToElements.classFullName);

    // handle deletes
    if (this.visitElements) {
      // must delete models first since they have a constraint on the submodeling element which may also be deleted
      for (const modelId of this._sourceDbChanges.model.deleteIds) {
        this.handler.onDeleteModel(modelId);
      }
      for (const elementId of this._sourceDbChanges.element.deleteIds) {
        // We don't know how the handler wants to handle deletions, and we don't have enough information
        // to know if deleted entities were related, so when processing changes, ignore errors from deletion.
        // Technically, to keep the ignored error scope small, we ignore only the error of looking up a missing element,
        // that approach works at least for the IModelTransformer.
        // In the future, the handler may be responsible for doing the work of finding out which elements were cascade deleted,
        // and returning them for the exporter to use to avoid double-deleting with error ignoring
        try {
          this.handler.onDeleteElement(elementId);
        } catch (err: unknown) {
          const isMissingErr =
            err instanceof IModelError &&
            err.errorNumber === IModelStatus.NotFound;
          if (!isMissingErr) throw err;
        }
      }
    }

    if (this.visitRelationships) {
      for (const relInstanceId of this._sourceDbChanges.relationship
        .deleteIds) {
        this.handler.onDeleteRelationship(relInstanceId);
      }
    }

    // Enable consecutive exportChanges runs without the need to re-instantiate the exporter.
    // You can counteract the obvious impact of losing this expensive data by always calling
    // exportChanges with the [[ExportChangesOptions.changedInstanceIds]] option set to
    // whatever you want
    if (this._resetChangeDataOnExport) this._sourceDbChanges = undefined;
  }

  private _resetChangeDataOnExport = true;

  /** Export schemas from the source iModel.
   * @note This must be called separately from [[exportAll]] or [[exportChanges]].
   */
  public async exportSchemas(): Promise<void> {
    /* eslint-disable @typescript-eslint/indent */
    const sql = `
      SELECT s.Name, s.VersionMajor, s.VersionWrite, s.VersionMinor
      FROM ECDbMeta.ECSchemaDef s
      ${
        this.wantSystemSchemas
          ? ""
          : `
      WHERE ECInstanceId >= (SELECT ECInstanceId FROM ECDbMeta.ECSchemaDef WHERE Name='BisCore')
      `
      }
      ORDER BY ECInstanceId
    `;
    /* eslint-enable @typescript-eslint/indent */
    const schemaNamesToExport: string[] = [];
    this.sourceDb.withPreparedStatement(sql, (statement: ECSqlStatement) => {
      while (DbResult.BE_SQLITE_ROW === statement.step()) {
        const schemaName = statement.getValue(0).getString();
        const versionMajor = statement.getValue(1).getInteger();
        const versionWrite = statement.getValue(2).getInteger();
        const versionMinor = statement.getValue(3).getInteger();
        const schemaKey = new SchemaKey(
          schemaName,
          new ECVersion(versionMajor, versionWrite, versionMinor)
        );
        if (this.handler.shouldExportSchema(schemaKey)) {
          schemaNamesToExport.push(schemaName);
        }
      }
    });

    if (schemaNamesToExport.length === 0) return;

    const schemaLoader = new SchemaLoader((name: string) =>
      this.sourceDb.getSchemaProps(name)
    );
    await Promise.all(
      schemaNamesToExport.map(async (schemaName) => {
        const schema = schemaLoader.getSchema(schemaName);
        Logger.logTrace(loggerCategory, `exportSchema(${schemaName})`);
        return this.handler.onExportSchema(schema);
      })
    );
  }

  /** For logging, indicate the change type if known. */
  private getChangeOpSuffix(isUpdate: boolean | undefined): string {
    return isUpdate ? " UPDATE" : undefined === isUpdate ? "" : " INSERT";
  }

  /** Export all CodeSpecs from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportCodeSpecs(): Promise<void> {
    Logger.logTrace(loggerCategory, "exportCodeSpecs()");
    const sql = "SELECT Name FROM BisCore:CodeSpec ORDER BY ECInstanceId";
    await this.sourceDb.withPreparedStatement(
      sql,
      async (statement: ECSqlStatement): Promise<void> => {
        while (DbResult.BE_SQLITE_ROW === statement.step()) {
          const codeSpecName: string = statement.getValue(0).getString();
          await this.exportCodeSpecByName(codeSpecName);
        }
      }
    );
  }

  /** Export a single CodeSpec from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportCodeSpecByName(codeSpecName: string): Promise<void> {
    const codeSpec: CodeSpec = this.sourceDb.codeSpecs.getByName(codeSpecName);
    let isUpdate: boolean | undefined;
    if (undefined !== this._sourceDbChanges) {
      // is changeset information available?
      if (this._sourceDbChanges.codeSpec.insertIds.has(codeSpec.id)) {
        isUpdate = false;
      } else if (this._sourceDbChanges.codeSpec.updateIds.has(codeSpec.id)) {
        isUpdate = true;
      } else {
        return; // not in changeset, don't export
      }
    }
    // passed changeset test, now apply standard exclusion rules
    if (this._excludedCodeSpecNames.has(codeSpec.name)) {
      Logger.logInfo(loggerCategory, `Excluding CodeSpec: ${codeSpec.name}`);
      return;
    }
    // CodeSpec has passed standard exclusion rules, now give handler a chance to accept/reject export
    if (this.handler.shouldExportCodeSpec(codeSpec)) {
      Logger.logTrace(
        loggerCategory,
        `exportCodeSpec(${codeSpecName})${this.getChangeOpSuffix(isUpdate)}`
      );
      this.handler.onExportCodeSpec(codeSpec, isUpdate);
      return this.trackProgress();
    }
  }

  /** Export a single CodeSpec from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportCodeSpecById(codeSpecId: Id64String): Promise<void> {
    const codeSpec: CodeSpec = this.sourceDb.codeSpecs.getById(codeSpecId);
    return this.exportCodeSpecByName(codeSpec.name);
  }

  /** Export all fonts from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportFonts(): Promise<void> {
    Logger.logTrace(loggerCategory, "exportFonts()");
    for (const font of this.sourceDb.fontMap.fonts.values()) {
      await this.exportFontByNumber(font.id);
    }
  }

  /** Export a single font from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportFontByName(fontName: string): Promise<void> {
    Logger.logTrace(loggerCategory, `exportFontByName(${fontName})`);
    const font: FontProps | undefined = this.sourceDb.fontMap.getFont(fontName);
    if (undefined !== font) {
      await this.exportFontByNumber(font.id);
    }
  }

  /** Export a single font from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportFontByNumber(fontNumber: number): Promise<void> {
    /** sourceDbChanges now works by using TS ChangesetECAdaptor which doesn't pick up changes to fonts since fonts is not an ec table.
     * So lets always export fonts for the time being by always setting isUpdate = true.
     * It is very rare and even problematic for the font table to reach a large size, so it is not a bottleneck in transforming changes.
     * See https://github.com/iTwin/imodel-transformer/pull/135 for removed code.
     */
    const isUpdate = true;
    Logger.logTrace(loggerCategory, `exportFontById(${fontNumber})`);
    const font: FontProps | undefined =
      this.sourceDb.fontMap.getFont(fontNumber);
    if (undefined !== font) {
      this.handler.onExportFont(font, isUpdate);
      return this.trackProgress();
    }
  }

  /** Export the model container, contents, and sub-models from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportModel(modeledElementId: Id64String): Promise<void> {
    const model: Model = this.sourceDb.models.getModel(modeledElementId);
    if (model.isTemplate && !this.wantTemplateModels) {
      return;
    }
    const modeledElement: Element = this.sourceDb.elements.getElement({
      id: modeledElementId,
      wantGeometry: this.wantGeometry,
      wantBRepData: this.wantGeometry,
    });
    Logger.logTrace(loggerCategory, `exportModel(${modeledElementId})`);
    if (this.shouldExportElement(modeledElement)) {
      await this.exportModelContainer(model);
      if (this.visitElements) {
        await this.exportModelContents(modeledElementId);
      }
      await this.exportSubModels(modeledElementId);
    }
  }

  /** Export the model (the container only) from the source iModel. */
  private async exportModelContainer(model: Model): Promise<void> {
    let isUpdate: boolean | undefined;
    if (undefined !== this._sourceDbChanges) {
      // is changeset information available?
      if (this._sourceDbChanges.model.insertIds.has(model.id)) {
        isUpdate = false;
      } else if (this._sourceDbChanges.model.updateIds.has(model.id)) {
        isUpdate = true;
      } else {
        return; // not in changeset, don't export
      }
    }
    this.handler.onExportModel(model, isUpdate);
    return this.trackProgress();
  }

  private _yieldManager = new YieldManager();

  /** Export the model contents.
   * @param modelId The only required parameter
   * @param elementClassFullName Can be optionally specified if the goal is to export a subset of the model contents
   * @param skipRootSubject Decides whether or not to export the root Subject. It is normally left undefined except for internal implementation purposes.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportModelContents(
    modelId: Id64String,
    elementClassFullName: string = Element.classFullName,
    skipRootSubject?: boolean
  ): Promise<void> {
    if (skipRootSubject) {
      // NOTE: IModelTransformer.processAll should skip the root Subject since it is specific to the individual iModel and is not part of the changes that need to be synchronized
      // NOTE: IModelExporter.exportAll should not skip the root Subject since the goal is to export everything
      assert(modelId === IModel.repositoryModelId); // flag is only relevant when processing the RepositoryModel
    }
    if (!this.visitElements) {
      Logger.logTrace(
        loggerCategory,
        `visitElements=false, skipping exportModelContents(${modelId})`
      );
      return;
    }
    if (undefined !== this._sourceDbChanges) {
      // is changeset information available?
      if (
        !this._sourceDbChanges.model.insertIds.has(modelId) &&
        !this._sourceDbChanges.model.updateIds.has(modelId)
      ) {
        return; // this optimization assumes that the Model changes (LastMod) any time an Element in the Model changes
      }
    }
    Logger.logTrace(loggerCategory, `exportModelContents(${modelId})`);
    let sql: string;
    if (skipRootSubject) {
      sql = `SELECT ECInstanceId FROM ${elementClassFullName} WHERE Parent.Id IS NULL AND Model.Id=:modelId AND ECInstanceId!=:rootSubjectId ORDER BY ECInstanceId`;
    } else {
      sql = `SELECT ECInstanceId FROM ${elementClassFullName} WHERE Parent.Id IS NULL AND Model.Id=:modelId ORDER BY ECInstanceId`;
    }
    await this.sourceDb.withPreparedStatement(
      sql,
      async (statement: ECSqlStatement): Promise<void> => {
        statement.bindId("modelId", modelId);
        if (skipRootSubject) {
          statement.bindId("rootSubjectId", IModel.rootSubjectId);
        }
        while (DbResult.BE_SQLITE_ROW === statement.step()) {
          await this.exportElement(statement.getValue(0).getId());
          await this._yieldManager.allowYield();
        }
      }
    );
  }

  /** Export the sub-models directly below the specified model.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportSubModels(parentModelId: Id64String): Promise<void> {
    Logger.logTrace(loggerCategory, `exportSubModels(${parentModelId})`);
    const definitionModelIds: Id64String[] = [];
    const otherModelIds: Id64String[] = [];
    const sql = `SELECT ECInstanceId FROM ${Model.classFullName} WHERE ParentModel.Id=:parentModelId ORDER BY ECInstanceId`;
    this.sourceDb.withPreparedStatement(
      sql,
      (statement: ECSqlStatement): void => {
        statement.bindId("parentModelId", parentModelId);
        while (DbResult.BE_SQLITE_ROW === statement.step()) {
          const modelId: Id64String = statement.getValue(0).getId();
          const model: Model = this.sourceDb.models.getModel(modelId);
          if (model instanceof DefinitionModel) {
            definitionModelIds.push(modelId);
          } else {
            otherModelIds.push(modelId);
          }
        }
      }
    );
    // export DefinitionModels before other types of Models
    for (const definitionModelId of definitionModelIds) {
      await this.exportModel(definitionModelId);
    }
    for (const otherModelId of otherModelIds) {
      await this.exportModel(otherModelId);
    }
  }

  /** Returns true if the specified element should be exported.
   * This considers the standard IModelExporter exclusion rules plus calls [IModelExportHandler.shouldExportElement]($transformer) for any custom exclusion rules.
   * @note This method is called from within [[exportChanges]] and [[exportAll]], so usually does not need to be called directly.
   */
  public shouldExportElement(element: Element): boolean {
    if (this._excludedElementIds.has(element.id)) {
      Logger.logInfo(loggerCategory, `Excluded element ${element.id} by Id`);
      return false;
    }
    if (element instanceof GeometricElement) {
      if (this._excludedElementCategoryIds.has(element.category)) {
        Logger.logInfo(
          loggerCategory,
          `Excluded element ${element.id} by Category`
        );
        return false;
      }
    }
    if (
      !this.wantTemplateModels &&
      element instanceof RecipeDefinitionElement
    ) {
      Logger.logInfo(
        loggerCategory,
        `Excluded RecipeDefinitionElement ${element.id} because wantTemplate=false`
      );
      return false;
    }
    for (const excludedElementClass of this._excludedElementClasses) {
      if (element instanceof excludedElementClass) {
        Logger.logInfo(
          loggerCategory,
          `Excluded element ${element.id} by class: ${excludedElementClass.classFullName}`
        );
        return false;
      }
    }
    // element has passed standard exclusion rules, now give handler a chance to accept/reject
    return this.handler.shouldExportElement(element);
  }

  /** Export the specified element, its child elements (if applicable), and any owned ElementAspects.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportElement(elementId: Id64String): Promise<void> {
    if (!this.visitElements) {
      Logger.logTrace(
        loggerCategory,
        `visitElements=false, skipping exportElement(${elementId})`
      );
      return;
    }

    // Return early if the elementId is already in the excludedElementIds, that way we don't need to load the element from the db.
    if (this._excludedElementIds.has(elementId)) {
      Logger.logInfo(loggerCategory, `Excluded element ${elementId} by Id`);
      this.handler.onSkipElement(elementId);
      return;
    }

    // are we processing changes?
    const isUpdate = this._sourceDbChanges?.element.insertIds.has(elementId)
      ? false
      : this._sourceDbChanges?.element.updateIds.has(elementId)
        ? true
        : undefined;

    const element = this.sourceDb.elements.getElement({
      id: elementId,
      wantGeometry: this.wantGeometry,
      wantBRepData: this.wantGeometry,
    });
    Logger.logTrace(
      loggerCategory,
      `exportElement(${
        element.id
      }, "${element.getDisplayLabel()}")${this.getChangeOpSuffix(isUpdate)}`
    );
    // the order and `await`ing of calls beyond here is depended upon by the IModelTransformer for a current bug workaround
    if (this.shouldExportElement(element)) {
      await this.handler.preExportElement(element);
      this.handler.onExportElement(element, isUpdate);
      await this.trackProgress();
      await this._exportElementAspectsStrategy.exportElementAspectsForElement(
        elementId
      );
      return this.exportChildElements(elementId);
    } else {
      this.handler.onSkipElement(element.id);
    }
  }

  /** Export the child elements of the specified element from the source iModel.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportChildElements(elementId: Id64String): Promise<void> {
    if (!this.visitElements) {
      Logger.logTrace(
        loggerCategory,
        `visitElements=false, skipping exportChildElements(${elementId})`
      );
      return;
    }
    const childElementIds: Id64String[] =
      this.sourceDb.elements.queryChildren(elementId);
    if (childElementIds.length > 0) {
      Logger.logTrace(loggerCategory, `exportChildElements(${elementId})`);
      for (const childElementId of childElementIds) {
        await this.exportElement(childElementId);
      }
    }
  }

  /** Exports all aspects present in the iModel.
   */
  private async exportAllAspects(): Promise<void> {
    return this._exportElementAspectsStrategy.exportAllElementAspects();
  }

  /** Exports all relationships that subclass from the specified base class.
   * @note This method is called from [[exportChanges]] and [[exportAll]], so it only needs to be called directly when exporting a subset of an iModel.
   */
  public async exportRelationships(
    baseRelClassFullName: string
  ): Promise<void> {
    if (!this.visitRelationships) {
      Logger.logTrace(
        loggerCategory,
        "visitRelationships=false, skipping exportRelationships()"
      );
      return;
    }
    Logger.logTrace(
      loggerCategory,
      `exportRelationships(${baseRelClassFullName})`
    );
    const sql = `SELECT r.ECInstanceId, r.ECClassId FROM ${baseRelClassFullName} r
                  JOIN bis.Element s ON s.ECInstanceId = r.SourceECInstanceId
                  JOIN bis.Element t ON t.ECInstanceId = r.TargetECInstanceId
                  WHERE s.ECInstanceId IS NOT NULL AND t.ECInstanceId IS NOT NULL`;
    await this.sourceDb.withPreparedStatement(
      sql,
      async (statement: ECSqlStatement): Promise<void> => {
        while (DbResult.BE_SQLITE_ROW === statement.step()) {
          const relationshipId = statement.getValue(0).getId();
          const relationshipClass = statement
            .getValue(1)
            .getClassNameForClassId();
          await this.exportRelationship(relationshipClass, relationshipId); // must call exportRelationship using the actual classFullName, not baseRelClassFullName
          await this._yieldManager.allowYield();
        }
      }
    );
  }

  /** Export a relationship from the source iModel. */
  public async exportRelationship(
    relClassFullName: string,
    relInstanceId: Id64String
  ): Promise<void> {
    if (!this.visitRelationships) {
      Logger.logTrace(
        loggerCategory,
        `visitRelationships=false, skipping exportRelationship(${relClassFullName}, ${relInstanceId})`
      );
      return;
    }
    let isUpdate: boolean | undefined;
    if (undefined !== this._sourceDbChanges) {
      // is changeset information available?
      if (this._sourceDbChanges.relationship.insertIds.has(relInstanceId)) {
        isUpdate = false;
      } else if (
        this._sourceDbChanges.relationship.updateIds.has(relInstanceId)
      ) {
        isUpdate = true;
      } else {
        return; // not in changeset, don't export
      }
    }
    // passed changeset test, now apply standard exclusion rules
    Logger.logTrace(
      loggerCategory,
      `exportRelationship(${relClassFullName}, ${relInstanceId})`
    );
    const relationship: Relationship = this.sourceDb.relationships.getInstance(
      relClassFullName,
      relInstanceId
    );
    for (const excludedRelationshipClass of this._excludedRelationshipClasses) {
      if (relationship instanceof excludedRelationshipClass) {
        Logger.logInfo(
          loggerCategory,
          `Excluded relationship by class: ${excludedRelationshipClass.classFullName}`
        );
        return;
      }
    }
    // relationship has passed standard exclusion rules, now give handler a chance to accept/reject export
    if (this.handler.shouldExportRelationship(relationship)) {
      this.handler.onExportRelationship(relationship, isUpdate);
      await this.trackProgress();
    }
  }

  /** Tracks incremental progress */
  private async trackProgress(): Promise<void> {
    this._progressCounter++;
    if (0 === this._progressCounter % this.progressInterval) {
      return this.handler.onProgress();
    }
  }
}

/**
 * Arguments for [[ChangedInstanceIds.initialize]]
 * @public
 */
export type ChangedInstanceIdsInitOptions = ExportChangesOptions & {
  iModel: BriefcaseDb;
};

/** Class for holding change information.
 * @public
 */
export class ChangedInstanceOps {
  public insertIds = new Set<Id64String>();
  public updateIds = new Set<Id64String>();
  public deleteIds = new Set<Id64String>();

  /** Initializes the object from IModelJsNative.ChangedInstanceOpsProps. */
  public addFromJson(
    val: IModelJsNative.ChangedInstanceOpsProps | undefined
  ): void {
    if (undefined !== val) {
      if (undefined !== val.insert && Array.isArray(val.insert))
        val.insert.forEach((id: Id64String) => this.insertIds.add(id));

      if (undefined !== val.update && Array.isArray(val.update))
        val.update.forEach((id: Id64String) => this.updateIds.add(id));

      if (undefined !== val.delete && Array.isArray(val.delete))
        val.delete.forEach((id: Id64String) => this.deleteIds.add(id));
    }
  }
}

/**
 * Class for discovering modified elements between 2 versions of an iModel.
 * @public
 */
export class ChangedInstanceIds {
  public codeSpec = new ChangedInstanceOps();
  public model = new ChangedInstanceOps();
  public element = new ChangedInstanceOps();
  public aspect = new ChangedInstanceOps();
  public relationship = new ChangedInstanceOps();
  public font = new ChangedInstanceOps();
  private _codeSpecSubclassIds?: Set<string>;
  private _modelSubclassIds?: Set<string>;
  private _elementSubclassIds?: Set<string>;
  private _aspectSubclassIds?: Set<string>;
  private _relationshipSubclassIds?: Set<string>;
  private _db: IModelDb;
  public constructor(db: IModelDb) {
    this._db = db;
  }

  private async setupECClassIds(): Promise<void> {
    this._codeSpecSubclassIds = new Set<string>();
    this._modelSubclassIds = new Set<string>();
    this._elementSubclassIds = new Set<string>();
    this._aspectSubclassIds = new Set<string>();
    this._relationshipSubclassIds = new Set<string>();

    const addECClassIdsToSet = async (
      setToModify: Set<string>,
      baseClass: string
    ) => {
      for await (const row of this._db.createQueryReader(
        `SELECT ECInstanceId FROM ECDbMeta.ECClassDef where ECInstanceId IS (${baseClass})`
      )) {
        setToModify.add(row.ECInstanceId);
      }
    };
    const promises = [
      addECClassIdsToSet(this._codeSpecSubclassIds, "BisCore.CodeSpec"),
      addECClassIdsToSet(this._modelSubclassIds, "BisCore.Model"),
      addECClassIdsToSet(this._elementSubclassIds, "BisCore.Element"),
      addECClassIdsToSet(
        this._aspectSubclassIds,
        "BisCore.ElementUniqueAspect"
      ),
      addECClassIdsToSet(this._aspectSubclassIds, "BisCore.ElementMultiAspect"),
      addECClassIdsToSet(
        this._relationshipSubclassIds,
        "BisCore.ElementRefersToElements"
      ),
    ];
    await Promise.all(promises);
  }

  private get _ecClassIdsInitialized() {
    return (
      this._codeSpecSubclassIds &&
      this._modelSubclassIds &&
      this._elementSubclassIds &&
      this._aspectSubclassIds &&
      this._relationshipSubclassIds
    );
  }

  private isRelationship(ecClassId: string) {
    return this._relationshipSubclassIds?.has(ecClassId);
  }

  private isCodeSpec(ecClassId: string) {
    return this._codeSpecSubclassIds?.has(ecClassId);
  }

  private isAspect(ecClassId: string) {
    return this._aspectSubclassIds?.has(ecClassId);
  }

  private isModel(ecClassId: string) {
    return this._modelSubclassIds?.has(ecClassId);
  }

  private isElement(ecClassId: string) {
    return this._elementSubclassIds?.has(ecClassId);
  }

  /**
   * Adds the provided [[ChangedECInstance]] to the appropriate set of changes by class type (codeSpec, model, element, aspect, or relationship) maintained by this instance of ChangedInstanceIds.
   * If the same ECInstanceId is seen multiple times, the changedInstanceIds will be modified accordingly, i.e. if an id 'x' was updated but now we see 'x' was deleted, we will remove 'x'
   * from the set of updatedIds and add it to the set of deletedIds for the appropriate class type.
   * @param change ChangedECInstance which has the ECInstanceId, changeType (insert, update, delete) and ECClassId of the changed entity
   */
  public async addChange(change: ChangedECInstance): Promise<void> {
    if (!this._ecClassIdsInitialized) await this.setupECClassIds();
    const ecClassId = change.ECClassId ?? change.$meta?.fallbackClassId;
    if (ecClassId === undefined)
      throw new Error(
        `ECClassId was not found for id: ${change.ECInstanceId}! Table is : ${change?.$meta?.tables}`
      );
    const changeType: SqliteChangeOp | undefined = change.$meta?.op;
    if (changeType === undefined)
      throw new Error(
        `ChangeType was undefined for id: ${change.ECInstanceId}.`
      );

    if (this.isRelationship(ecClassId))
      this.handleChange(this.relationship, changeType, change.ECInstanceId);
    else if (this.isCodeSpec(ecClassId))
      this.handleChange(this.codeSpec, changeType, change.ECInstanceId);
    else if (this.isAspect(ecClassId))
      this.handleChange(this.aspect, changeType, change.ECInstanceId);
    else if (this.isModel(ecClassId))
      this.handleChange(this.model, changeType, change.ECInstanceId);
    else if (this.isElement(ecClassId))
      this.handleChange(this.element, changeType, change.ECInstanceId);
  }

  private handleChange(
    changedInstanceOps: ChangedInstanceOps,
    changeType: SqliteChangeOp,
    id: Id64String
  ) {
    // if changeType is a delete and we already have the id in the inserts then we can remove the id from the inserts.
    // if changeType is a delete and we already have the id in the updates then we can remove the id from the updates AND add it to the deletes.
    // if changeType is an insert and we already have the id in the deletes then we can remove the id from the deletes AND add it to the inserts.
    if (changeType === "Inserted") {
      changedInstanceOps.insertIds.add(id);
      changedInstanceOps.deleteIds.delete(id);
    } else if (changeType === "Updated") {
      if (!changedInstanceOps.insertIds.has(id))
        changedInstanceOps.updateIds.add(id);
    } else if (changeType === "Deleted") {
      // If we've inserted the entity at some point already and now we're seeing a delete. We can simply remove the entity from our inserted ids without adding it to deletedIds.
      if (changedInstanceOps.insertIds.has(id))
        changedInstanceOps.insertIds.delete(id);
      else {
        changedInstanceOps.updateIds.delete(id);
        changedInstanceOps.deleteIds.add(id);
      }
    }
  }

  /**
   * Initializes a new ChangedInstanceIds object with information taken from a range of changesets.
   * @public
   */
  public static async initialize(
    opts: ChangedInstanceIdsInitOptions
  ): Promise<ChangedInstanceIds | undefined> {
    if ("changedInstanceIds" in opts) return opts.changedInstanceIds;

    const iModelId = opts.iModel.iModelId;

    const startChangeset =
      "startChangeset" in opts ? opts.startChangeset : undefined;
    const changesetRanges =
      startChangeset !== undefined
        ? [
            [
              startChangeset.index ??
                (
                  await IModelHost.hubAccess.queryChangeset({
                    iModelId,
                    changeset: {
                      id: startChangeset.id ?? opts.iModel.changeset.id,
                    },
                  })
                ).index,
              opts.iModel.changeset.index ??
                (
                  await IModelHost.hubAccess.queryChangeset({
                    iModelId,
                    changeset: { id: opts.iModel.changeset.id },
                  })
                ).index,
            ],
          ]
        : "changesetRanges" in opts
          ? opts.changesetRanges
          : undefined;
    const csFileProps =
      changesetRanges !== undefined
        ? (
            await Promise.all(
              changesetRanges.map(async ([first, end]) =>
                IModelHost.hubAccess.downloadChangesets({
                  iModelId,
                  range: { first, end },
                  targetDir: BriefcaseManager.getChangeSetsPath(iModelId),
                })
              )
            )
          ).flat()
        : "csFileProps" in opts
          ? opts.csFileProps
          : undefined;

    if (csFileProps === undefined) return undefined;

    const changedInstanceIds = new ChangedInstanceIds(opts.iModel);
    const relationshipECClassIdsToSkip = new Set<string>();
    for await (const row of opts.iModel.createQueryReader(
      "SELECT ECInstanceId FROM ECDbMeta.ECClassDef where ECInstanceId IS (BisCore.ElementDrivesElement)"
    )) {
      relationshipECClassIdsToSkip.add(row.ECInstanceId);
    }

    for (const csFile of csFileProps) {
      const csReader = SqliteChangesetReader.openFile({
        fileName: csFile.pathname,
        db: opts.iModel,
        disableSchemaCheck: true,
      });
      const csAdaptor = new ChangesetECAdaptor(csReader);
      const ecChangeUnifier = new PartialECChangeUnifier();
      while (csAdaptor.step()) {
        ecChangeUnifier.appendFrom(csAdaptor);
      }
      const changes: ChangedECInstance[] = [...ecChangeUnifier.instances];

      for (const change of changes) {
        if (
          change.ECClassId !== undefined &&
          relationshipECClassIdsToSkip.has(change.ECClassId)
        )
          continue;
        await changedInstanceIds.addChange(change);
      }
      csReader.close();
    }
    return changedInstanceIds;
  }
}
