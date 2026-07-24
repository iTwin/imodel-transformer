/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import {
  GuidString,
  Id64String,
  ITwinError,
  Logger,
} from "@itwin/core-bentley";
import {
  EditTxn,
  ElementOwnsExternalSourceAspects,
  type Entity,
  ExternalSource,
  ExternalSourceAspect,
  ExternalSourceAttachment,
  FolderLink,
  IModelDb,
  SynchronizationConfigLink,
} from "@itwin/core-backend";
import {
  ChangesetIndexAndId,
  ExternalSourceAspectProps,
  IModel,
  QueryBinder,
} from "@itwin/core-common";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import type {
  IModelTransformOptions,
  TargetScopeProvenanceJsonProps,
} from "./IModelTransformer";
import type { SyncTypeResolver } from "./SyncTypeResolver";
import type { IModelCloneContext } from "./IModelCloneContext";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError";

const loggerCategory: string = TransformerLoggerCategory.IModelTransformer;

/**
 * Manages provenance scope aspects and synchronization versioning.
 * Encapsulates all ESA scope management, sync version tracking, and
 * provenance DB direction logic extracted from IModelTransformer.
 * @internal
 */
export class ProvenanceManager {
  public readonly context: IModelCloneContext;

  private readonly _targetScopeElementId: Id64String;
  private readonly _transformerOptions: IModelTransformOptions;
  private readonly _syncTypeResolver: SyncTypeResolver;
  private readonly _startingChangesetIndices?: {
    target: number;
    source: number;
  };

  /** NOTE: the json properties must be converted to string before insertion */
  private _targetScopeProvenanceProps:
    | (Omit<ExternalSourceAspectProps, "jsonProperties"> & {
        jsonProperties: TargetScopeProvenanceJsonProps;
      })
    | undefined = undefined;

  private _cachedSynchronizationVersion: ChangesetIndexAndId | undefined =
    undefined;

  private _targetClassNameToClassIdCache = new Map<string, string>();

  private readonly _targetEditTxn: EditTxn;
  private readonly _sourceEditTxn?: EditTxn;

  public constructor(
    targetScopeElementId: Id64String,
    transformerOptions: IModelTransformOptions,
    syncTypeResolver: SyncTypeResolver,
    targetEditTxn: EditTxn,
    sourceEditTxn?: EditTxn
  ) {
    this._targetScopeElementId = targetScopeElementId;
    this._transformerOptions = transformerOptions;
    this._syncTypeResolver = syncTypeResolver;
    this._targetEditTxn = targetEditTxn;
    this._sourceEditTxn = sourceEditTxn;
    this.context = this._syncTypeResolver.context;

    const sourceDb = this.context.sourceDb;
    const targetDb = this.context.targetDb;
    if (sourceDb.isBriefcase && targetDb.isBriefcase) {
      if (
        sourceDb.changeset.index === undefined ||
        targetDb.changeset.index === undefined
      )
        ITwinError.throwError({
          iTwinErrorId: {
            scope: IModelTransformerErrorScope,
            key: IModelTransformerError.ChangesetIndexUnavailable,
          },
          message: "database has no changeset index",
        });
      this._startingChangesetIndices = {
        target: targetDb.changeset.index,
        source: sourceDb.changeset.index,
      };
    }
  }

  private async _isReverseSynchronization(): Promise<boolean> {
    return (await this._syncTypeResolver.getSyncType()) === "reverse";
  }

  private async _queryTargetRelId(sourceRelInfo: {
    classFullName: string;
    sourceId: Id64String;
    targetId: Id64String;
  }): Promise<Id64String | undefined> {
    const targetRelInfo = {
      sourceId: this.context.findTargetElementId(sourceRelInfo.sourceId),
      targetId: this.context.findTargetElementId(sourceRelInfo.targetId),
    };
    if (
      targetRelInfo.sourceId === undefined ||
      targetRelInfo.targetId === undefined
    )
      return undefined;
    const sql = `
      select ecinstanceid
      from bis.elementreferstoelements
      where sourceecinstanceid=?
        and targetecinstanceid=?
        and ecclassid=?
    `;
    const params = new QueryBinder();
    params.bindId(1, targetRelInfo.sourceId);
    params.bindId(2, targetRelInfo.targetId);
    params.bindId(
      3,
      await this._targetClassNameToClassId(sourceRelInfo.classFullName)
    );
    const result = this.context.targetDb.createQueryReader(sql, params, {
      usePrimaryConn: true,
    });
    if (await result.step()) return result.current.id;
    else return undefined;
  }

  private async _targetClassNameToClassId(
    classFullName: string
  ): Promise<Id64String> {
    let classId = this._targetClassNameToClassIdCache.get(classFullName);
    if (classId === undefined) {
      classId = await this._getRelClassId(this.context.targetDb, classFullName);
      this._targetClassNameToClassIdCache.set(classFullName, classId);
    }
    return classId;
  }

  private async _getRelClassId(
    db: IModelDb,
    classFullName: string
  ): Promise<Id64String> {
    const sql = `
      SELECT c.ECInstanceId
      FROM ECDbMeta.ECClassDef c
      JOIN ECDbMeta.ECSchemaDef s ON c.Schema.Id=s.ECInstanceId
      WHERE s.Name=? AND c.Name=?
    `;
    const [schemaName, className] =
      classFullName.indexOf(".") !== -1
        ? classFullName.split(".")
        : classFullName.split(":");
    const params = new QueryBinder();
    params.bindString(1, schemaName);
    params.bindString(2, className);
    const result = db.createQueryReader(sql, params, { usePrimaryConn: true });
    if (await result.step()) return result.current.id;
    ITwinError.throwError({
      iTwinErrorId: {
        scope: IModelTransformerErrorScope,
        key: IModelTransformerError.RelationshipClassNotFound,
      },
      message: `Could not find class ${classFullName} in the db`,
    });
  }

  // ── Static provenance metadata ──────────────────────────────────────────

  /** The element classes that are considered to define provenance in the iModel */
  public static get provenanceElementClasses(): (typeof Entity)[] {
    return [
      FolderLink,
      SynchronizationConfigLink,
      ExternalSource,
      ExternalSourceAttachment,
    ];
  }

  /** The element aspect classes that are considered to define provenance in the iModel */
  public static get provenanceElementAspectClasses(): (typeof Entity)[] {
    return [ExternalSourceAspect];
  }

  // ── Static provenance queries ──────────────────────────────────────────

  /**
   * Iterate all matching federation guids and ExternalSourceAspects in the provenance iModel (target unless reverse sync)
   * and call a function for each one.
   * @note provenance is done by federation guids where possible
   * @note this may execute on each element more than once! Only use in cases where that is handled
   */
  public static async forEachTrackedElement(args: {
    provenanceSourceDb: IModelDb;
    provenanceDb: IModelDb;
    targetScopeElementId: Id64String;
    isReverseSynchronization: boolean;
    fn: (sourceElementId: Id64String, targetElementId: Id64String) => void;
    skipPropagateChangesToRootElements: boolean;
  }): Promise<void> {
    if (args.provenanceDb === args.provenanceSourceDb) return;

    if (!args.provenanceDb.containsClass(ExternalSourceAspect.classFullName)) {
      ITwinError.throwError({
        iTwinErrorId: {
          scope: IModelTransformerErrorScope,
          key: IModelTransformerError.ProvenanceSchemaUnsupported,
        },
        message: "The BisCore schema version of the target database is too old",
      });
    }

    const sourceDb = args.isReverseSynchronization
      ? args.provenanceDb
      : args.provenanceSourceDb;
    const targetDb = args.isReverseSynchronization
      ? args.provenanceSourceDb
      : args.provenanceDb;

    // query for provenanceDb
    const elementIdByFedGuidQuery = `
      SELECT e.ECInstanceId, FederationGuid
      FROM bis.Element e
      ${
        args.skipPropagateChangesToRootElements
          ? "WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10) -- special static elements"
          : ""
      }
      ORDER BY FederationGuid
    `;

    // iterate through sorted list of fed guids from both dbs to get the intersection
    // NOTE: if we exposed the native attach database support,
    // we could get the intersection of fed guids in one query, not sure if it would be faster
    // OR we could do a raw sqlite query...

    const sourceReader = sourceDb.createQueryReader(
      elementIdByFedGuidQuery,
      undefined,
      { usePrimaryConn: true }
    );
    const targetReader = targetDb.createQueryReader(
      elementIdByFedGuidQuery,
      undefined,
      { usePrimaryConn: true }
    );
    let hasSourceRow = await sourceReader.step();
    let hasTargetRow = await targetReader.step();
    while (hasSourceRow && hasTargetRow) {
      const sourceFedGuid = sourceReader.current.federationGuid as
        | GuidString
        | undefined;
      const targetFedGuid = targetReader.current.federationGuid as
        | GuidString
        | undefined;
      if (
        sourceFedGuid !== undefined &&
        targetFedGuid !== undefined &&
        sourceFedGuid === targetFedGuid
      ) {
        // data flow direction is always sourceDb -> targetDb and it does not depend on where the explicit element provenance is stored
        args.fn(
          sourceReader.current.id as Id64String,
          targetReader.current.id as Id64String
        );
      }
      if (
        targetFedGuid === undefined ||
        (sourceFedGuid !== undefined && sourceFedGuid >= targetFedGuid)
      ) {
        hasTargetRow = await targetReader.step();
      }
      if (
        sourceFedGuid === undefined ||
        (targetFedGuid !== undefined && sourceFedGuid <= targetFedGuid)
      ) {
        hasSourceRow = await sourceReader.step();
      }
    }

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

    const runFnInDataFlowDirection = (
      sourceId: Id64String,
      targetId: Id64String
    ) =>
      args.isReverseSynchronization
        ? args.fn(sourceId, targetId)
        : args.fn(targetId, sourceId);
    const params = new QueryBinder();
    params.bindId("scopeId", args.targetScopeElementId);
    params.bindString("kind", ExternalSourceAspect.Kind.Element);
    const provenanceReader = args.provenanceDb.createQueryReader(
      provenanceAspectsQuery,
      params,
      { usePrimaryConn: true }
    );
    for await (const row of provenanceReader) {
      // ExternalSourceAspect.Identifier is of type string
      const aspectIdentifier: Id64String = row[0];
      const elementId: Id64String = row.id;
      runFnInDataFlowDirection(elementId, aspectIdentifier);
    }
  }

  /**
   * Queries for an ESA which matches the props in the provided aspectProps.
   * @param dbToQuery db to run the query on
   * @param aspectProps aspectProps to search for
   */
  public static async queryScopeExternalSourceAspect(
    dbToQuery: IModelDb,
    aspectProps: ExternalSourceAspectProps
  ): Promise<
    | {
        aspectId: Id64String;
        version?: string;
        /** stringified json */
        jsonProperties?: string;
      }
    | undefined
  > {
    const sql = `
      SELECT ECInstanceId, Version, JsonProperties
      FROM ${ExternalSourceAspect.classFullName}
      WHERE Element.Id=:elementId
        AND Scope.Id=:scopeId
        AND Kind=:kind
        AND Identifier=:identifier
      LIMIT 1
    `;

    if (aspectProps.scope === undefined) return undefined;

    const params = new QueryBinder()
      .bindId("elementId", aspectProps.element.id)
      .bindId("scopeId", aspectProps.scope.id)
      .bindString("kind", aspectProps.kind)
      .bindString("identifier", aspectProps.identifier);

    return dbToQuery.withQueryReader(
      sql,
      (reader) => {
        if (!reader.step()) return undefined;
        const aspectId = reader.current[0] as Id64String;
        const version = reader.current[1] as string | undefined;
        const jsonProperties = reader.current[2] as string | undefined;
        return { aspectId, version, jsonProperties };
      },
      params
    );
  }

  // ── Provenance DB direction ────────────────────────────────────────────

  /** Return the IModelDb where provenance is stored (for reads/queries).
   * This will be targetDb except when it is a reverse synchronization, in which case it will be sourceDb.
   */
  private async getProvenanceDb(): Promise<IModelDb> {
    return (await this._isReverseSynchronization())
      ? this.context.sourceDb
      : this.context.targetDb;
  }

  /** Return the EditTxn for writing provenance.
   * This will be targetEditTxn except when it is a reverse synchronization, in which case it will be sourceEditTxn.
   */
  public async getProvenanceEditTxn(): Promise<EditTxn> {
    if (await this._isReverseSynchronization()) {
      if (!this._sourceEditTxn) {
        ITwinError.throwError({
          iTwinErrorId: {
            scope: IModelTransformerErrorScope,
            key: IModelTransformerError.SourceEditTxnRequired,
          },
          message:
            "A reverse synchronization requires a sourceEditTxn to write provenance back to the source iModel. " +
            "Pass sourceEditTxn in IModelTransformOptions.",
        });
      }
      return this._sourceEditTxn;
    }
    return this._targetEditTxn;
  }

  /** Return the IModelDb where entities referred to by stored provenance live.
   * This will be sourceDb except when it is a reverse synchronization, in which case it will be targetDb.
   */
  public async getProvenanceSourceDb(): Promise<IModelDb> {
    return (await this._isReverseSynchronization())
      ? this.context.targetDb
      : this.context.sourceDb;
  }

  // ── Scope aspect management ────────────────────────────────────────────

  /**
   * @returns provenance scope aspect if it exists in the provenanceDb.
   * Provenance scope aspect is created and inserted into provenanceDb when [[initScopeProvenance]] is invoked.
   */
  public async tryGetProvenanceScopeAspect(): Promise<
    ExternalSourceAspect | undefined
  > {
    const scopeProvenanceAspectProps =
      await ProvenanceManager.queryScopeExternalSourceAspect(
        await this.getProvenanceDb(),
        {
          id: undefined,
          classFullName: ExternalSourceAspect.classFullName,
          scope: { id: IModel.rootSubjectId },
          kind: ExternalSourceAspect.Kind.Scope,
          element: {
            id: this._targetScopeElementId ?? IModel.rootSubjectId,
          },
          identifier: (await this.getProvenanceSourceDb()).iModelId,
        }
      );

    return scopeProvenanceAspectProps !== undefined
      ? ((await this.getProvenanceDb()).elements.getAspect(
          scopeProvenanceAspectProps.aspectId
        ) as ExternalSourceAspect)
      : undefined;
  }

  /**
   * Make sure there are no conflicting other scope-type external source aspects on the target scope element.
   * If there are none at all, insert one (this must be a first synchronization).
   */
  public async initScopeProvenance(): Promise<void> {
    const provenanceDb = await this.getProvenanceDb();
    const provenanceEditTxn = await this.getProvenanceEditTxn();
    const sourceProvenanceDb = await this.getProvenanceSourceDb();
    const aspectProps = {
      id: undefined as string | undefined,
      version: undefined as string | undefined,
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: this._targetScopeElementId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: IModel.rootSubjectId },
      identifier: sourceProvenanceDb.iModelId,
      kind: ExternalSourceAspect.Kind.Scope,
      jsonProperties: undefined as TargetScopeProvenanceJsonProps | undefined,
    };

    const foundEsaProps =
      await ProvenanceManager.queryScopeExternalSourceAspect(
        provenanceDb,
        aspectProps
      );

    if (foundEsaProps === undefined) {
      aspectProps.version = "";
      aspectProps.jsonProperties = {
        pendingReverseSyncChangesetIndices: [],
        pendingSyncChangesetIndices: [],
        reverseSyncVersion: "",
      };

      // query without "identifier" to find possible conflicts
      const sql = `
        SELECT ECInstanceId
        FROM ${ExternalSourceAspect.classFullName}
        WHERE Element.Id=:elementId
          AND Scope.Id=:scopeId
          AND Kind=:kind
        LIMIT 1
      `;

      const params = new QueryBinder();
      params.bindId("elementId", aspectProps.element.id);
      params.bindId("scopeId", aspectProps.scope.id);
      params.bindString("kind", aspectProps.kind);
      const reader = provenanceDb.createQueryReader(sql, params, {
        usePrimaryConn: true,
      });
      const hasConflictingScope = await reader.step();

      if (hasConflictingScope) {
        ITwinError.throwError({
          iTwinErrorId: {
            scope: IModelTransformerErrorScope,
            key: IModelTransformerError.ProvenanceScopeConflict,
          },
          message: "Provenance scope conflict",
        });
      }
      if (!this._transformerOptions.noProvenance) {
        const id = provenanceEditTxn.insertAspect({
          ...aspectProps,
          jsonProperties: JSON.stringify(aspectProps.jsonProperties) as any,
        });
        aspectProps.id = id;
        this.clearCachedSynchronizationVersion();
      }
    } else {
      aspectProps.id = foundEsaProps.aspectId;
      aspectProps.version = foundEsaProps.version;
      aspectProps.jsonProperties = foundEsaProps.jsonProperties
        ? JSON.parse(foundEsaProps.jsonProperties)
        : undefined;
      const oldProps = JSON.parse(JSON.stringify(aspectProps));
      if (this.handleUnsafeMigrate(aspectProps)) {
        Logger.logInfo(
          loggerCategory,
          "Unsafe migrate made a change to the target scope's external source aspect. Updating aspect in database.",
          { oldProps, newProps: aspectProps }
        );
        provenanceEditTxn.updateAspect({
          ...aspectProps,
          jsonProperties: JSON.stringify(aspectProps.jsonProperties) as any,
        });
        this.clearCachedSynchronizationVersion();
      }
    }

    this._targetScopeProvenanceProps =
      aspectProps as typeof this._targetScopeProvenanceProps;
  }

  /** Returns true if a change was made to the aspectProps. */
  private handleUnsafeMigrate(aspectProps: {
    version?: string;
    jsonProperties?: TargetScopeProvenanceJsonProps;
  }): boolean {
    let madeChange = false;
    if (
      this._transformerOptions.branchRelationshipDataBehavior !==
      "unsafe-migrate"
    )
      return madeChange;
    const fallbackSyncVersionToUse =
      this._transformerOptions.argsForProcessChanges
        ?.unsafeFallbackSyncVersion ?? "";
    const fallbackReverseSyncVersionToUse =
      this._transformerOptions.argsForProcessChanges
        ?.unsafeFallbackReverseSyncVersion ?? "";

    if (
      aspectProps.version === undefined ||
      (aspectProps.version === "" &&
        aspectProps.version !== fallbackSyncVersionToUse)
    ) {
      aspectProps.version = fallbackSyncVersionToUse;
      madeChange = true;
    }

    if (aspectProps.jsonProperties === undefined) {
      aspectProps.jsonProperties = {
        pendingReverseSyncChangesetIndices: [],
        pendingSyncChangesetIndices: [],
        reverseSyncVersion: fallbackReverseSyncVersionToUse,
      };
      madeChange = true;
    } else if (
      aspectProps.jsonProperties.reverseSyncVersion === undefined ||
      (aspectProps.jsonProperties.reverseSyncVersion === "" &&
        aspectProps.jsonProperties.reverseSyncVersion !==
          fallbackReverseSyncVersionToUse)
    ) {
      aspectProps.jsonProperties.reverseSyncVersion =
        fallbackReverseSyncVersionToUse;
      madeChange = true;
    }

    if (
      aspectProps.jsonProperties.pendingReverseSyncChangesetIndices ===
      undefined
    ) {
      Logger.logWarning(
        loggerCategory,
        "Property pendingReverseSyncChangesetIndices missing on the jsonProperties of the scoping ESA. Setting to []."
      );
      aspectProps.jsonProperties.pendingReverseSyncChangesetIndices = [];
      madeChange = true;
    }
    if (aspectProps.jsonProperties.pendingSyncChangesetIndices === undefined) {
      Logger.logWarning(
        loggerCategory,
        "Property pendingSyncChangesetIndices missing on the jsonProperties of the scoping ESA. Setting to []."
      );
      aspectProps.jsonProperties.pendingSyncChangesetIndices = [];
      madeChange = true;
    }
    return madeChange;
  }

  // ── Synchronization version ────────────────────────────────────────────

  /**
   * We cache the synchronization version to avoid querying the target scoping ESA multiple times.
   * Clears the cached value so the next call to getSynchronizationVersion re-queries.
   */
  public clearCachedSynchronizationVersion() {
    this._cachedSynchronizationVersion = undefined;
  }

  /** The changeset version in the scoping element's source version found for this transformation.
   * @note the version depends on whether this is a reverse synchronization or not.
   * @note empty string and -1 for changeset and index if it has never been transformed.
   */
  public async getSynchronizationVersion(): Promise<ChangesetIndexAndId> {
    if (this._cachedSynchronizationVersion === undefined) {
      const provenanceScopeAspect = await this.tryGetProvenanceScopeAspect();
      if (!provenanceScopeAspect) {
        return { index: -1, id: "" };
      }

      const version = (await this._isReverseSynchronization())
        ? (
            JSON.parse(
              provenanceScopeAspect.jsonProperties ?? "{}"
            ) as TargetScopeProvenanceJsonProps
          ).reverseSyncVersion
        : provenanceScopeAspect.version;
      if (
        !version &&
        this._transformerOptions.branchRelationshipDataBehavior ===
          "unsafe-migrate"
      ) {
        return { index: -1, id: "" };
      }
      if (version === undefined) {
        ITwinError.throwError({
          iTwinErrorId: {
            scope: IModelTransformerErrorScope,
            key: IModelTransformerError.SynchronizationVersionMissing,
          },
          message: `Could not find synchronization version in scope aspect. This may be due to the last successful run of the transformer being done with an older version.
         Consider running the transformer with branchRelationshipDataBehavior set to 'unsafe-migrate'`,
        });
      }
      const [id, index] = version === "" ? ["", -1] : version.split(";");
      if (Number.isNaN(Number(index)))
        throw new Error("Could not parse version data from scope aspect");
      this._cachedSynchronizationVersion = { index: Number(index), id };
    }
    return this._cachedSynchronizationVersion;
  }

  /**
   * Returns the pending changeset indices to skip for the current synchronization direction.
   * Used by changeset initialization to determine which changesets have already been processed.
   */
  public async getChangesetsToSkip(): Promise<number[]> {
    if (this._targetScopeProvenanceProps === undefined)
      throw new Error("_targetScopeProvenanceProps should be set by now");
    const props = this._targetScopeProvenanceProps;
    return (await this._isReverseSynchronization())
      ? props.jsonProperties.pendingReverseSyncChangesetIndices
      : props.jsonProperties.pendingSyncChangesetIndices;
  }

  /**
   * Updates the synchronization version on the scope ESA.
   *
   * Called at the end of a transformation, updates the target scope element to record
   * that transformation up through the source's changeset has been performed. Also stores
   * all changesets that occurred during the transformation as "pending synchronization
   * changeset indices" @see TargetScopeProvenanceJsonProps
   *
   * @param initializeReverseSyncVersion When true, saves the reverse sync version as the
   * current changeset of the targetDb. This is typically used for the first transformation
   * between a master and branch iModel. Setting this to true has the effect of making it so
   * any changesets in the branch iModel at the time of the first transformation will be
   * ignored during any future reverse synchronizations from the branch to the master iModel.
   *
   * Note that typically, the reverseSyncVersion is saved as the last changeset merged from
   * the branch into master. Setting initializeReverseSyncVersion to true during a forward
   * transformation could overwrite this correct reverseSyncVersion and should only be done
   * during the first transformation between a master and branch iModel.
   *
   * @param shouldUpdateSynchronizationVersion Whether the source synchronization version
   * should be updated after processing.
   */
  public async updateSynchronizationVersion({
    initializeReverseSyncVersion = false,
    shouldUpdateSynchronizationVersion,
  }: {
    initializeReverseSyncVersion?: boolean;
    shouldUpdateSynchronizationVersion: boolean;
  }) {
    const shouldSkipSyncVersionUpdate =
      !initializeReverseSyncVersion && !shouldUpdateSynchronizationVersion;
    if (shouldSkipSyncVersionUpdate) return;

    // If noProvenance is set, there's no scope ESA to update
    if (this._transformerOptions.noProvenance) return;

    if (this._targetScopeProvenanceProps === undefined)
      throw new Error("_targetScopeProvenanceProps should be set by now");
    const scopeProps = this._targetScopeProvenanceProps;

    const sourceVersion = `${this.context.sourceDb.changeset.id};${this.context.sourceDb.changeset.index}`;
    const targetVersion = `${this.context.targetDb.changeset.id};${this.context.targetDb.changeset.index}`;

    if (await this._isReverseSynchronization()) {
      const oldVersion = scopeProps.jsonProperties.reverseSyncVersion;

      Logger.logInfo(
        loggerCategory,
        `updating reverse version from ${oldVersion} to ${sourceVersion}`
      );
      scopeProps.jsonProperties.reverseSyncVersion = sourceVersion;
    } else {
      Logger.logInfo(
        loggerCategory,
        `updating sync version from ${scopeProps.version} to ${sourceVersion}`
      );
      scopeProps.version = sourceVersion;

      if (initializeReverseSyncVersion) {
        Logger.logInfo(
          loggerCategory,
          `updating reverse sync version from ${scopeProps.jsonProperties.reverseSyncVersion} to ${targetVersion}`
        );
        scopeProps.jsonProperties.reverseSyncVersion = targetVersion;
      }
    }

    const startingChangesetIndices = this._startingChangesetIndices;
    if (
      !!this._transformerOptions.argsForProcessChanges ||
      (startingChangesetIndices && initializeReverseSyncVersion)
    ) {
      if (
        this.context.targetDb.changeset.index === undefined ||
        startingChangesetIndices === undefined
      )
        throw new Error(
          "updateSynchronizationVersion was called without change history"
        );

      const jsonProps = scopeProps.jsonProperties;

      Logger.logTrace(
        loggerCategory,
        `previous pendingReverseSyncChanges: ${String(jsonProps.pendingReverseSyncChangesetIndices)}`
      );
      Logger.logTrace(
        loggerCategory,
        `previous pendingSyncChanges: ${String(jsonProps.pendingSyncChangesetIndices)}`
      );

      const pendingSyncChangesetIndicesKey =
        "pendingSyncChangesetIndices" as const;
      const pendingReverseSyncChangesetIndicesKey =
        "pendingReverseSyncChangesetIndices" as const;

      let syncChangesetsToClearKey;
      let syncChangesetsToUpdateKey;

      if (await this._isReverseSynchronization()) {
        syncChangesetsToClearKey = pendingReverseSyncChangesetIndicesKey;
        syncChangesetsToUpdateKey = pendingSyncChangesetIndicesKey;
      } else {
        syncChangesetsToClearKey = pendingSyncChangesetIndicesKey;
        syncChangesetsToUpdateKey = pendingReverseSyncChangesetIndicesKey;
      }

      for (
        let i = startingChangesetIndices.target + 1;
        i <= this.context.targetDb.changeset.index + 1;
        i++
      )
        jsonProps[syncChangesetsToUpdateKey].push(i);
      jsonProps[syncChangesetsToClearKey] = jsonProps[
        syncChangesetsToClearKey
      ].filter((csIndex) => {
        return csIndex > startingChangesetIndices.source;
      });

      if (await this._isReverseSynchronization()) {
        if (this.context.sourceDb.changeset.index === undefined)
          throw new Error("changeset didn't exist");
        for (
          let i = startingChangesetIndices.source + 1;
          i <= this.context.sourceDb.changeset.index + 1;
          i++
        )
          jsonProps.pendingReverseSyncChangesetIndices.push(i);
      }

      Logger.logTrace(
        loggerCategory,
        `new pendingReverseSyncChanges: ${String(jsonProps.pendingReverseSyncChangesetIndices)}`
      );
      Logger.logTrace(
        loggerCategory,
        `new pendingSyncChanges: ${String(jsonProps.pendingSyncChangesetIndices)}`
      );
    }

    (await this.getProvenanceEditTxn()).updateAspect({
      ...scopeProps,
      jsonProperties: JSON.stringify(scopeProps.jsonProperties) as any,
    });
    this.clearCachedSynchronizationVersion();
  }

  // ── Element/Relationship provenance creation ───────────────────────────

  /** Create ExternalSourceAspectProps for an element in an iModel → iModel transformation. */
  public static initElementProvenanceOptions(
    sourceElementId: Id64String,
    targetElementId: Id64String,
    args: {
      sourceDb: IModelDb;
      targetDb: IModelDb;
      isReverseSynchronization: boolean;
      targetScopeElementId: Id64String;
    }
  ): ExternalSourceAspectProps {
    const elementId = args.isReverseSynchronization
      ? sourceElementId
      : targetElementId;
    const version = args.isReverseSynchronization
      ? args.targetDb.elements.queryLastModifiedTime(targetElementId)
      : args.sourceDb.elements.queryLastModifiedTime(sourceElementId);
    const aspectIdentifier = args.isReverseSynchronization
      ? targetElementId
      : sourceElementId;
    const aspectProps: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: elementId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: args.targetScopeElementId },
      identifier: aspectIdentifier,
      kind: ExternalSourceAspect.Kind.Element,
      version,
    };
    return aspectProps;
  }

  /** Create ExternalSourceAspectProps for a relationship in an iModel → iModel transformation. */
  public static async initRelationshipProvenanceOptions(
    sourceRelInstanceId: Id64String,
    targetRelInstanceId: Id64String,
    args: {
      sourceDb: IModelDb;
      targetDb: IModelDb;
      isReverseSynchronization: boolean;
      targetScopeElementId: Id64String;
      forceOldRelationshipProvenanceMethod: boolean;
    }
  ): Promise<ExternalSourceAspectProps> {
    const provenanceDb = args.isReverseSynchronization
      ? args.sourceDb
      : args.targetDb;
    const aspectIdentifier = args.isReverseSynchronization
      ? targetRelInstanceId
      : sourceRelInstanceId;
    const provenanceRelInstanceId = args.isReverseSynchronization
      ? sourceRelInstanceId
      : targetRelInstanceId;

    const sql =
      "SELECT SourceECInstanceId FROM bis.ElementRefersToElements WHERE ECInstanceId=?";
    const params = new QueryBinder().bindId(1, provenanceRelInstanceId);
    const reader = provenanceDb.createQueryReader(sql, params, {
      usePrimaryConn: true,
    });
    if (!(await reader.step()))
      ITwinError.throwError({
        iTwinErrorId: {
          scope: IModelTransformerErrorScope,
          key: IModelTransformerError.RelationshipProvenanceNotFound,
        },
        message: "relationship provenance query returned no rows",
      });
    const elementId = reader.current[0];

    const jsonProperties = args.forceOldRelationshipProvenanceMethod
      ? { targetRelInstanceId }
      : { provenanceRelInstanceId };

    const aspectProps: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: elementId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: args.targetScopeElementId },
      identifier: aspectIdentifier,
      kind: ExternalSourceAspect.Kind.Relationship,
      jsonProperties: JSON.stringify(jsonProperties),
    };

    return aspectProps;
  }

  /** Create an ExternalSourceAspectProps for an element using this manager's context. */
  public async initElementProvenance(
    sourceElementId: Id64String,
    targetElementId: Id64String
  ): Promise<ExternalSourceAspectProps> {
    return ProvenanceManager.initElementProvenanceOptions(
      sourceElementId,
      targetElementId,
      {
        isReverseSynchronization: await this._isReverseSynchronization(),
        targetScopeElementId: this._targetScopeElementId,
        sourceDb: this.context.sourceDb,
        targetDb: this.context.targetDb,
      }
    );
  }

  /** Create an ExternalSourceAspectProps for a relationship using this manager's context. */
  public async initRelationshipProvenance(
    sourceRelInstanceId: Id64String,
    targetRelInstanceId: Id64String,
    forceOldRelationshipProvenanceMethod: boolean
  ): Promise<ExternalSourceAspectProps> {
    return ProvenanceManager.initRelationshipProvenanceOptions(
      sourceRelInstanceId,
      targetRelInstanceId,
      {
        sourceDb: this.context.sourceDb,
        targetDb: this.context.targetDb,
        isReverseSynchronization: await this._isReverseSynchronization(),
        targetScopeElementId: this._targetScopeElementId,
        forceOldRelationshipProvenanceMethod,
      }
    );
  }

  // ── Provenance queries ─────────────────────────────────────────────────

  /**
   * Queries the provenanceDb for an ESA whose identifier matches the provided element ID.
   * @param entityInProvenanceSourceId ID of the element in the provenanceSourceDb
   */
  public async queryProvenanceForElement(
    entityInProvenanceSourceId: Id64String
  ): Promise<Id64String | undefined> {
    const sql = `
        SELECT esa.Element.Id
        FROM Bis.ExternalSourceAspect esa
        WHERE esa.Kind=?
          AND esa.Scope.Id=?
          AND esa.Identifier=?
      `;
    const params = new QueryBinder();
    params.bindString(1, ExternalSourceAspect.Kind.Element);
    params.bindId(2, this._targetScopeElementId);
    params.bindString(3, entityInProvenanceSourceId);
    const result = (await this.getProvenanceDb()).createQueryReader(
      sql,
      params,
      {
        usePrimaryConn: true,
      }
    );
    if (await result.step()) {
      return result.current.id;
    } else return undefined;
  }

  /**
   * Queries the provenanceDb for an ESA whose identifier matches the provided relationship ID.
   * @param entityInProvenanceSourceId ID of the relationship in the provenanceSourceDb
   * @param sourceRelInfo Source relationship class and endpoint info (for legacy fallback)
   */
  public async queryProvenanceForRelationship(
    entityInProvenanceSourceId: Id64String,
    sourceRelInfo: {
      classFullName: string;
      sourceId: Id64String;
      targetId: Id64String;
    }
  ): Promise<
    | {
        aspectId: Id64String;
        /** if undefined, the relationship could not be found, perhaps it was deleted */
        relationshipId: Id64String | undefined;
      }
    | undefined
  > {
    const sql = `
      SELECT
        ECInstanceId,
        JSON_EXTRACT(JsonProperties, '$.provenanceRelInstanceId') AS provenanceRelInstId
      FROM Bis.ExternalSourceAspect
      WHERE Kind=?
        AND Scope.Id=?
        AND Identifier=?
    `;
    const params = new QueryBinder();
    params.bindString(1, ExternalSourceAspect.Kind.Relationship);
    params.bindId(2, this._targetScopeElementId);
    params.bindString(3, entityInProvenanceSourceId);
    const result = (await this.getProvenanceDb()).createQueryReader(
      sql,
      params,
      {
        usePrimaryConn: true,
      }
    );
    if (await result.step()) {
      const aspectId = result.current.id;
      const provenanceRelInstId = result.current.provenanceRelInstId;
      const provenanceRelInstanceId =
        provenanceRelInstId !== undefined
          ? (provenanceRelInstId as string)
          : await this._queryTargetRelId(sourceRelInfo);
      return {
        aspectId,
        relationshipId: provenanceRelInstanceId,
      };
    } else return undefined;
  }

  /** Instance convenience that calls the static forEachTrackedElement with this manager's context. */
  public async forEachTrackedElement(
    fn: (sourceElementId: Id64String, targetElementId: Id64String) => void
  ): Promise<void> {
    return ProvenanceManager.forEachTrackedElement({
      provenanceSourceDb: await this.getProvenanceSourceDb(),
      provenanceDb: await this.getProvenanceDb(),
      targetScopeElementId: this._targetScopeElementId,
      isReverseSynchronization: await this._isReverseSynchronization(),
      fn,
      skipPropagateChangesToRootElements:
        this._transformerOptions.skipPropagateChangesToRootElements ?? true,
    });
  }
}
