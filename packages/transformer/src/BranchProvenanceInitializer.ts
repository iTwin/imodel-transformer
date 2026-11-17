/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import {
  BriefcaseDb,
  EditTxn,
  ElementOwnsExternalSourceAspects,
  ExternalSource,
  ExternalSourceAspect,
  ExternalSourceIsInRepository,
  IModelDb,
  RepositoryLink,
  StandaloneDb,
} from "@itwin/core-backend";
import {
  DbResult,
  Id64String,
  ITwinError,
  Logger,
  OpenMode,
} from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceProps,
  IModel,
  QueryBinder,
  RepositoryLinkProps,
} from "@itwin/core-common";
import { strict as assert } from "node:assert";
import { ProvenanceManager } from "./ProvenanceManager.js";
import { pathToFileURL } from "node:url";
import { transformerPackageMetadata } from "./TransformerPackageMetadata.js";
import type { TargetScopeProvenanceJsonProps } from "./IModelTransformer.js";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError.js";

/**
 * @alpha
 */
export interface ProvenanceInitArgs {
  /** the master iModel which is the source of the provenance */
  master: IModelDb;
  /** the canonical url of the master iModel */
  masterUrl?: string;
  /** the description of the master iModel */
  masterDescription?: string;
  /**
   * @param {IModelDb} branchDb - the branch iModel which is the container of the provenance
   *                              Must be opened Read/Write
   */
  branch: IModelDb;
  /**
   * insert Federation Guids in all lacking elements in the master database, which will prevent
   * needing to insert External Source Aspects for provenance tracking
   * @note requires a read/write master
   * @note closes both the master and branch iModels to reset caches, so you must reopen them.
   *       If you pass `"keep-reopened-db"`, this object's `master` and `branch` properties will
   *       be set to new, open databases.
   */
  createFedGuidsForMaster?: true | false | "keep-reopened-db";
  /**
   * An existing element in the branch that represents the master and scopes the branch's provenance.
   * Pass the same value as [[IModelTransformOptions.targetScopeElementId]] in later synchronizations.
   * If omitted, a new [RepositoryLink]($backend) and [ExternalSource]($backend) that represent the master are inserted,
   * and the ExternalSource is used.
   * @note The element must not already own a provenance scope aspect.
   */
  targetScopeElementId?: Id64String;
}

/**
 * @alpha
 */
export interface ProvenanceInitResult {
  /** The element that scopes the branch's provenance. Pass it as [[IModelTransformOptions.targetScopeElementId]] in later synchronizations. */
  targetScopeElementId: Id64String;
  /** The inserted ExternalSource that represents the master. `undefined` if [[ProvenanceInitArgs.targetScopeElementId]] was passed. */
  masterExternalSourceId?: Id64String;
  /** The inserted RepositoryLink that represents the master. `undefined` if [[ProvenanceInitArgs.targetScopeElementId]] was passed. */
  masterRepositoryLinkId?: Id64String;
}

/**
 * Initializes a branch iModel that was created as a copy of a master iModel so that it can be synchronized with
 * [[IModelTransformer]] using [[IModelTransformOptions.argsForProcessChanges]] in either direction.
 *
 * This establishes the master-branch relationship:
 * - it inserts the provenance scope aspect on the target scope element, recording the master's current changeset as the
 *   forward synchronization version and the branch's current changeset as the reverse synchronization version;
 * - for briefcases, it records the branch's next changeset as one that reverse synchronizations skip, so push the
 *   branch's changes from this call as a single changeset before making other changes to the branch;
 * - it inserts [ExternalSourceAspect]($backend) provenance only for elements and relationships that can't be correlated by
 *   FederationGuid. Use [[ProvenanceInitArgs.createFedGuidsForMaster]] to avoid inserting those aspects.
 * @note This function creates and ends its own [EditTxn]($backend) on the branch.
 * @alpha
 */
export async function initializeBranchProvenance(
  args: ProvenanceInitArgs
): Promise<ProvenanceInitResult> {
  if (
    args.master.isBriefcase &&
    args.branch.isBriefcase &&
    (args.master.changeset.index === undefined ||
      args.branch.changeset.index === undefined)
  )
    ITwinError.throwError({
      iTwinErrorId: {
        scope: IModelTransformerErrorScope,
        key: IModelTransformerError.ChangesetIndexUnavailable,
      },
      message: "database has no changeset index",
    });

  if (args.targetScopeElementId !== undefined)
    await throwIfScopeProvenanceExists(args.branch, args.targetScopeElementId);

  let editTxn = new EditTxn(args.branch, "initializeBranchProvenance");
  editTxn.start();

  if (args.createFedGuidsForMaster) {
    // FIXME<LOW>: Consider enforcing that the master and branch dbs passed as part of ProvenanceInitArgs to this function
    // are identical. https://github.com/iTwin/imodel-transformer/issues/138
    args.master.withSqliteStatement(
      `
        UPDATE bis_Element
        SET FederationGuid=randomblob(16)
        WHERE FederationGuid IS NULL
      `,
      (s) =>
        assert(s.step() === DbResult.BE_SQLITE_DONE, args.master.getLastError())
    );
    const masterPath = args.master.pathName;
    const reopenMaster = makeDbReopener(args.master);
    args.master.close(); // prevent busy
    args.branch.withSqliteStatement(
      `ATTACH DATABASE '${pathToFileURL(`${masterPath}`)}?mode=ro' AS master`,
      (s) =>
        assert(s.step() === DbResult.BE_SQLITE_DONE, args.branch.getLastError())
    );
    args.branch.withSqliteStatement(
      `
      UPDATE main.bis_Element
      SET FederationGuid = (
        SELECT m.FederationGuid
        FROM master.bis_Element m
        WHERE m.Id=main.bis_Element.Id
      )`,

      (s) =>
        assert(s.step() === DbResult.BE_SQLITE_DONE, args.branch.getLastError())
    );
    args.branch.clearCaches(); // statements write lock attached db (clearing statement cache does not fix this)
    editTxn.saveChanges();
    editTxn.end();
    args.branch.withSqliteStatement("DETACH DATABASE master", (s) => {
      const res = s.step();
      if (res !== DbResult.BE_SQLITE_DONE)
        Logger.logTrace(
          "initializeBranchProvenance",
          `Error detaching db (we will close anyway): ${args.branch.getLastError()}`
        );
      // this is the case until native side changes
      assert(res === DbResult.BE_SQLITE_ERROR, args.branch.getLastError());
    });
    args.branch.performCheckpoint();

    const reopenBranch = makeDbReopener(args.branch);
    // close dbs because element cache could be invalid
    args.branch.close();
    [args.master, args.branch] = await Promise.all([
      reopenMaster(),
      reopenBranch(),
    ]);
    // Recreate editTxn on the reopened branch db
    editTxn = new EditTxn(args.branch, "initializeBranchProvenance");
    editTxn.start();
  }

  let masterRepoLinkId: Id64String | undefined;
  let masterExternalSourceId: Id64String | undefined;
  let targetScopeElementId = args.targetScopeElementId;
  if (targetScopeElementId === undefined) {
    // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
    masterRepoLinkId = editTxn.insertElement({
      classFullName: RepositoryLink.classFullName,
      code: RepositoryLink.createCode(
        args.branch,
        IModelDb.repositoryModelId,
        "example-code-value"
      ),
      model: IModelDb.repositoryModelId,
      url: args.masterUrl,
      format: "iModel",
      repositoryGuid: args.master.iModelId,
      description: args.masterDescription,
    } as RepositoryLinkProps);

    masterExternalSourceId = editTxn.insertElement({
      classFullName: ExternalSource.classFullName,
      model: IModelDb.rootSubjectId,
      code: Code.createEmpty(),
      repository: new ExternalSourceIsInRepository(masterRepoLinkId),
      connectorName: transformerPackageMetadata.name,
      connectorVersion: transformerPackageMetadata.version,
    } as ExternalSourceProps);
    targetScopeElementId = masterExternalSourceId;
  }

  editTxn.insertAspect(createScopeProvenance(args, targetScopeElementId));

  const fedGuidlessElemsSql = `
    SELECT ECInstanceId AS id
    FROM Bis.Element
    WHERE FederationGuid IS NULL
      AND ECInstanceId NOT IN (0x1, 0xe, 0x10) /* ignore special elems */
  `;
  const elemReader = args.branch.createQueryReader(
    fedGuidlessElemsSql,
    undefined,
    { usePrimaryConn: true }
  );
  for await (const row of elemReader) {
    const id: string = row.id;
    const aspectProps = ProvenanceManager.initElementProvenanceOptions(id, id, {
      isReverseSynchronization: false,
      targetScopeElementId,
      sourceDb: args.master,
      targetDb: args.branch,
    });
    editTxn.insertAspect(aspectProps);
  }

  const fedGuidlessRelsSql = `
    SELECT erte.ECInstanceId as id
    FROM Bis.ElementRefersToElements erte
    JOIN bis.Element se
      ON se.ECInstanceId=erte.SourceECInstanceId
    JOIN bis.Element te
      ON te.ECInstanceId=erte.TargetECInstanceId
      WHERE se.FederationGuid IS NULL
      OR te.FederationGuid IS NULL`;
  const relReader = args.branch.createQueryReader(
    fedGuidlessRelsSql,
    undefined,
    { usePrimaryConn: true }
  );
  for await (const row of relReader) {
    const id: string = row.id;
    const aspectProps =
      await ProvenanceManager.initRelationshipProvenanceOptions(id, id, {
        isReverseSynchronization: false,
        targetScopeElementId,
        sourceDb: args.master,
        targetDb: args.branch,
        forceOldRelationshipProvenanceMethod: false,
      });
    editTxn.insertAspect(aspectProps);
  }

  editTxn.saveChanges();
  editTxn.end();

  if (args.createFedGuidsForMaster === true) {
    args.master.close();
    args.branch.close();
  }

  return {
    targetScopeElementId,
    masterExternalSourceId,
    masterRepositoryLinkId: masterRepoLinkId,
  };
}

/**
 * The scope aspect records the master's current changeset as the forward synchronization version and the branch's
 * current changeset as the reverse synchronization version. For briefcases, the branch changeset that will contain
 * this initialization is pending, so reverse synchronizations skip it.
 */
function createScopeProvenance(
  args: ProvenanceInitArgs,
  targetScopeElementId: Id64String
) {
  const { master, branch } = args;
  const jsonProperties: TargetScopeProvenanceJsonProps = {
    pendingReverseSyncChangesetIndices:
      master.isBriefcase &&
      branch.isBriefcase &&
      branch.changeset.index !== undefined
        ? [branch.changeset.index + 1]
        : [],
    pendingSyncChangesetIndices: [],
    reverseSyncVersion: `${branch.changeset.id};${branch.changeset.index}`,
  };
  return {
    classFullName: ExternalSourceAspect.classFullName,
    element: {
      id: targetScopeElementId,
      relClassName: ElementOwnsExternalSourceAspects.classFullName,
    },
    scope: { id: IModel.rootSubjectId },
    identifier: master.iModelId,
    kind: ExternalSourceAspect.Kind.Scope,
    version: `${master.changeset.id};${master.changeset.index}`,
    jsonProperties: JSON.stringify(jsonProperties) as any,
  };
}

async function throwIfScopeProvenanceExists(
  branch: IModelDb,
  targetScopeElementId: Id64String
): Promise<void> {
  const params = new QueryBinder()
    .bindId("elementId", targetScopeElementId)
    .bindId("scopeId", IModel.rootSubjectId)
    .bindString("kind", ExternalSourceAspect.Kind.Scope);
  const reader = branch.createQueryReader(
    `
      SELECT ECInstanceId
      FROM ${ExternalSourceAspect.classFullName}
      WHERE Element.Id=:elementId
        AND Scope.Id=:scopeId
        AND Kind=:kind
      LIMIT 1
    `,
    params,
    { usePrimaryConn: true }
  );
  if (await reader.step())
    ITwinError.throwError({
      iTwinErrorId: {
        scope: IModelTransformerErrorScope,
        key: IModelTransformerError.ProvenanceScopeConflict,
      },
      message: "Provenance scope conflict",
    });
}

function makeDbReopener(db: IModelDb) {
  const originalMode = db.isReadonly ? OpenMode.Readonly : OpenMode.ReadWrite;
  const dbPath = db.pathName;
  let reopenDb: (mode?: OpenMode) => IModelDb | Promise<IModelDb>;
  if (db instanceof BriefcaseDb)
    reopenDb = async (mode = originalMode) =>
      BriefcaseDb.open({
        fileName: dbPath,
        readonly: mode === OpenMode.Readonly,
      });
  else if (db instanceof StandaloneDb)
    reopenDb = (mode = originalMode) => StandaloneDb.openFile(dbPath, mode);
  else assert(false, `db type '${db.constructor.name}' not supported`);
  return reopenDb;
}
