import { ExternalSource, ExternalSourceIsInRepository, IModelDb, RepositoryLink } from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";
import { Code, ExternalSourceProps, RepositoryLinkProps } from "@itwin/core-common";
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
   * @note requires a read/write master
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
    // FIXME: elements in the cache could be wrong after this so need to purge cache somehow, maybe close the iModel
    args.master.withSqliteStatement("UPDATE bis_Element SET FederationGuid=randomblob(16) WHERE FederationGuid IS NULL", (s) => {
      assert(s.step() === DbResult.BE_SQLITE_DONE);
    });
  }

  // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
  const masterLinkRepoId = args.branch.elements.insertElement({
    classFullName: RepositoryLink.classFullName,
    code: RepositoryLink.createCode(args.branch, IModelDb.repositoryModelId, "example-code-value"),
    model: IModelDb.repositoryModelId,
    url: args.masterUrl,
    format: "iModel",
    repositoryGuid: args.master.iModelId,
    description: args.masterDescription,
  } as RepositoryLinkProps);

  const masterExternalSourceId = args.branch.elements.insertElement({
    classFullName: ExternalSource.classFullName,
    model: IModelDb.rootSubjectId,
    code: Code.createEmpty(),
    repository: new ExternalSourceIsInRepository(masterLinkRepoId),
    /* eslint-disable @typescript-eslint/no-var-requires */
    connectorName: require("../../package.json").name,
    connectorVersion: require("../../package.json").version,
    /* eslint-enable @typescript-eslint/no-var-requires */
  } as ExternalSourceProps);

  const fedGuidLessElemsSql = "SELECT ECInstanceId as id FROM Bis.Element WHERE FederationGuid IS NULL";
  const reader = args.branch.createQueryReader(fedGuidLessElemsSql);
  while (await reader.step()) {
    const id: string = reader.current.toRow().id;
    IModelTransformer.initElementProvenanceOptions(id, id, {
      isReverseSynchronization: false,
      targetScopeElementId: masterExternalSourceId,
      sourceDb: args.master,
      targetDb: args.branch,
    });
  }

  return {
    targetScopeElementId: masterExternalSourceId,
  };
}

