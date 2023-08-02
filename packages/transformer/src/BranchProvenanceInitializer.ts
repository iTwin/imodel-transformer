
import { BriefcaseDb, ExternalSource, ExternalSourceIsInRepository, IModelDb, Relationship, RepositoryLink, SnapshotDb, StandaloneDb } from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";
import { Code, DbRequestKind } from "@itwin/core-common";
import assert = require("assert");
import { IModelTransformer } from "./IModelTransformer";

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
   * @note requires a read/write master, and will close your copy so you may have to reopen
   */
  createFedGuidsForMaster?: boolean;
}

interface ProvenanceInitResult {
  targetScopeElementId: Id64String;
}

/**
 * @alpha
 */
export async function initializeBranchProvenance(args: ProvenanceInitArgs): Promise<ProvenanceInitResult> {
  if (args.createFedGuidsForMaster) {
    args.master.withSqliteStatement(
      `ATTACH DATABASE '${args.branch.pathName}' AS branch`,
      (s) => assert(s.step() === DbResult.BE_SQLITE_DONE),
    );
    args.master.withSqliteStatement(`
        UPDATE bis_Element
        SET FederationGuid=randomblob(16)
        WHERE FederationGuid IS NULL
          AND Id NOT IN (0x1, 0xe, 0x10) -- ignore special elems
      `,
      (s) => assert(s.step() === DbResult.BE_SQLITE_DONE),
    );
    args.master.withSqliteStatement(`
        UPDATE branch.bis_Element
          SET FederationGuid=me.FederationGuid
        FROM bis_Element me
        WHERE me.Id=branch.bis_Element.Id
      `,
      (s) => assert(s.step() === DbResult.BE_SQLITE_DONE),
    );

    const reopenMaster = makeDbReopener(args.master);
    const reopenBranch = makeDbReopener(args.branch);
    args.master.close();
    args.branch.close();
    [args.master, args.branch] = await Promise.all([reopenMaster(), reopenBranch()]);
  }

  // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
  const masterLinkRepoId = new RepositoryLink({
    classFullName: RepositoryLink.classFullName,
    code: RepositoryLink.createCode(args.branch, IModelDb.repositoryModelId, "example-code-value"),
    model: IModelDb.repositoryModelId,
    url: args.masterUrl,
    format: "iModel",
    repositoryGuid: args.master.iModelId,
    description: args.masterDescription,
  }, args.branch).insert();

  const masterExternalSourceId = new ExternalSource({
    classFullName: ExternalSource.classFullName,
    model: IModelDb.rootSubjectId,
    code: Code.createEmpty(),
    repository: new ExternalSourceIsInRepository(masterLinkRepoId),
    /* eslint-disable @typescript-eslint/no-var-requires */
    connectorName: require("../../package.json").name,
    connectorVersion: require("../../package.json").version,
    /* eslint-enable @typescript-eslint/no-var-requires */
  }, args.branch).insert();

  const fedGuidLessElemsSql = `
    SELECT ECInstanceId AS id
    FROM Bis.Element
    WHERE FederationGuid IS NULL
      AND ECInstanceId NOT IN (0x1, 0xe, 0x10) -- ignore special elems
  `;
  const elemReader = args.branch.createQueryReader(fedGuidLessElemsSql);
  while (await elemReader.step()) {
    const id: string = elemReader.current.toRow().id;
    const aspectProps = IModelTransformer.initElementProvenanceOptions(id, id, {
      isReverseSynchronization: false,
      targetScopeElementId: masterExternalSourceId,
      sourceDb: args.master,
    });
    args.branch.elements.insertAspect(aspectProps);
  }

  const fedGuidLessRelsSql = `
    SELECT erte.ECInstanceId as id
    FROM Bis.ElementRefersToElements erte
    JOIN bis.Element se
      ON se.ECInstanceId=erte.SourceECInstanceId
    JOIN bis.Element te
      ON te.ECInstanceId=erte.TargetECInstanceId
      WHERE se.FederationGuid IS NULL
      OR te.FederationGuid IS NULL`;
  const relReader = args.branch.createQueryReader(fedGuidLessRelsSql);
  while (await relReader.step()) {
    const id: string = relReader.current.toRow().id;
    const aspectProps = IModelTransformer.initRelationshipProvenanceOptions(id, id, {
      isReverseSynchronization: false,
      targetScopeElementId: masterExternalSourceId,
      sourceDb: args.master,
      targetDb: args.branch,
      forceOldRelationshipProvenanceMethod: false
    });
    args.branch.elements.insertAspect(aspectProps);
  }

  // prevent leak
  if (args.createFedGuidsForMaster)
    args.master.close();

  return {
    targetScopeElementId: masterExternalSourceId,
  };
}

function makeDbReopener(db: IModelDb) {
  const dbPath = db.pathName;
  let reopenDb: () => IModelDb | Promise<IModelDb>;
  if (db instanceof BriefcaseDb)
    reopenDb = async () => BriefcaseDb.open({ fileName: dbPath });
  else if (db instanceof StandaloneDb)
    reopenDb = () => StandaloneDb.openFile(dbPath);
  else
    assert(false, `db db type '${db.constructor.name}' not supported`);
  return reopenDb;
}

