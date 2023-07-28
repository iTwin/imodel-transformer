import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { BriefcaseDb, ElementGroupsMembers, ExternalSource, ExternalSourceAspect, ExternalSourceIsInRepository, IModelDb, IModelHost, PhysicalModel, PhysicalObject, RepositoryLink, SnapshotDb, SpatialCategory, StandaloneDb } from "@itwin/core-backend";
import { initializeBranchProvenance } from "../../BranchProvenanceInitializer";
import { IModelTransformerTestUtils, assertIdentityTransformation } from "../IModelTransformerUtils";
import * as TestUtils from "../TestUtils";
import { BriefcaseIdValue, Code } from "@itwin/core-common";
import { IModelTransformer } from "../../IModelTransformer";
import { OpenMode, Guid, DbResult } from "@itwin/core-bentley";
import { assert } from "chai";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";

interface InsertParams {
  pathName: string;
  physModelId: string;
  categoryId: string;
}

describe("compare imodels from BranchProvenanceInitializer and traditional branch init", () => {

  for (const sourceHasFedguid of [true, false]){
    let index = 0;
    for (const targetHasFedguid of [true, false]) {
      it.only(`elements have fed guid, Source:'${sourceHasFedguid}',  Target:'${targetHasFedguid}'`, async () => {
        const sourceFileName: string = `Source-${sourceHasFedguid}-Target-${targetHasFedguid}.bim`;
        const insertData = generateEmptyIModel(sourceFileName);
        const pathName = insertData.pathName;

        const sourceElem = insertElementToImodel(insertData, sourceHasFedguid, index, 0);
        const targetElem = insertElementToImodel(insertData, targetHasFedguid, index, 1);
        insertRelationship(pathName, sourceElem, targetElem);

        // should have an extra source aspect on the source elem if sourceHasFedGuid && targetHasFedGuid
        let sourceDb = StandaloneDb.openFile(insertData.pathName, OpenMode.ReadWrite);
        sourceDb.close();

        const transformerForkPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", `Transfromer-Source-${sourceHasFedguid}-Target-${targetHasFedguid}.bim`);
        fs.copyFileSync(pathName, transformerForkPath);
        setToStandalone(transformerForkPath);
        const transformerForkDb = StandaloneDb.openFile(transformerForkPath);
    
        sourceDb = StandaloneDb.openFile(insertData.pathName, OpenMode.ReadWrite);
        await classicalTransformerBranchInit(sourceDb, transformerForkDb);
    
        const noTransformerForkPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", `Transformerless-${sourceHasFedguid}-Target-${targetHasFedguid}.bim`);
        fs.copyFileSync(pathName, noTransformerForkPath);
        setToStandalone(noTransformerForkPath);
        const noTransformerForkDb = StandaloneDb.openFile(noTransformerForkPath);
      
    
        await initializeBranchProvenance({
          master: sourceDb,
          branch: noTransformerForkDb,
        });

        const sourceNumElem = noTransformerForkDb.elements.getAspects(sourceElem, ExternalSourceAspect.classFullName).length;
        const targetNumElem = noTransformerForkDb.elements.getAspects(targetElem, ExternalSourceAspect.classFullName).length;
        if (targetHasFedguid && sourceHasFedguid)
          assert(sourceNumElem === 0 && targetNumElem === 0, 
            `Expected External Source Aspects for Source Element and Target Element: 0-0, Received: ${sourceNumElem}-${targetNumElem}`
          );
        if (!sourceHasFedguid && targetHasFedguid)
          assert(sourceNumElem === 2 && targetNumElem === 0,
            `Expected External Source Aspects for Source Element and Target Element: 2-0, Received: ${sourceNumElem}-${targetNumElem}`
          );
        if (sourceHasFedguid && !targetHasFedguid)
          assert(sourceNumElem === 1 && targetNumElem === 1,
            `Expected External Source Aspects for Source Element and Target Element: 1-1, Received: ${sourceNumElem}-${targetNumElem}`
          );
        if (!sourceHasFedguid && !targetHasFedguid)
          assert(sourceNumElem === 2 && targetNumElem === 1,
            `Expected External Source Aspects for Source Element and Target Element: 2-1, Received: ${sourceNumElem}-${targetNumElem}`
          );
      

        assertIdentityTransformation(transformerForkDb, noTransformerForkDb);

        sourceDb.close();
        transformerForkDb.close();
        noTransformerForkDb.close();
      });
    }
    index ++;
  }
});

function setToStandalone(iModelName: string) {
  const nativeDb = new IModelHost.platform.DgnDb();
  nativeDb.openIModel(iModelName, OpenMode.ReadWrite);
  nativeDb.setITwinId(Guid.empty); // empty iTwinId means "standalone"
  nativeDb.saveChanges(); // save change to iTwinId
  nativeDb.deleteAllTxns(); // necessary before resetting briefcaseId
  nativeDb.resetBriefcaseId(BriefcaseIdValue.Unassigned); // standalone iModels should always have BriefcaseId unassigned
  nativeDb.saveLocalValue("StandaloneEdit", JSON.stringify({ txns: true }));
  nativeDb.saveChanges(); // save change to briefcaseId
  nativeDb.closeIModel();
}

async function classicalTransformerBranchInit(sourceDb: StandaloneDb, branchDb: StandaloneDb,) {
  // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
  const masterLinkRepoId = new RepositoryLink({
    classFullName: RepositoryLink.classFullName,
    code: RepositoryLink.createCode(branchDb, IModelDb.repositoryModelId, "test-imodel"),
    model: IModelDb.repositoryModelId,
    // url: "https://wherever-you-got-your-imodel.net",
    format: "iModel",
    repositoryGuid: sourceDb.iModelId,
    description: "master iModel repository",
  }, branchDb).insert();

  const masterExternalSourceId = new ExternalSource({
    classFullName: ExternalSource.classFullName,
    model: IModelDb.rootSubjectId,
    code: Code.createEmpty(),
    repository: new ExternalSourceIsInRepository(masterLinkRepoId),
    connectorName: "iModel Transformer",
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    connectorVersion: require("../../../../package.json").version,
  }, branchDb).insert();

  // initialize the branch provenance
  const branchInitializer = new IModelTransformer(sourceDb, branchDb, {
    // tells the transformer that we have a raw copy of a source and the target should receive
    // provenance from the source that is necessary for performing synchronizations in the future
    wasSourceIModelCopiedToTarget: true,
    // store the synchronization provenance in the scope of our representation of the external source, master
    targetScopeElementId: masterExternalSourceId,
  });

  await branchInitializer.processAll();
  // save+push our changes to whatever hub we're using
  const description = "initialized branch iModel";
  branchDb.saveChanges(description);

  // branchDb.close(); 
  branchInitializer.dispose();
}

export function generateEmptyIModel(fileName: string): InsertParams {
  const sourcePath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", fileName);
  if (fs.existsSync(sourcePath))
    fs.unlinkSync(sourcePath);

  let sourceDb = StandaloneDb.createEmpty(sourcePath, { rootSubject: { name: fileName }});

  const physModelId = PhysicalModel.insert(sourceDb, IModelDb.rootSubjectId, "physical model");
  const categoryId = SpatialCategory.insert(sourceDb, IModelDb.dictionaryId, "spatial category", {});


  const pathName = sourceDb.pathName;
  sourceDb.saveChanges();
  sourceDb.close();
  setToStandalone(pathName);
  const insertData: InsertParams = {
    pathName,
    physModelId,
    categoryId
  };
  return insertData;
}

function insertElementToImodel(insertData: InsertParams, withFedGuid: boolean, index: number, code: number): string {

  const sourceDb = StandaloneDb.openFile(insertData.pathName, OpenMode.ReadWrite);

  const fedGuid = withFedGuid ? undefined : Guid.empty;
  const elem = new PhysicalObject({
    classFullName: PhysicalObject.classFullName,
    category: insertData.categoryId,
    geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
    placement: {
      origin: Point3d.create(1, 1, 1),
      angles: YawPitchRollAngles.createDegrees(1, 1, 1),
    },
    model: insertData.physModelId,
    code: new Code({ spec: IModelDb.rootSubjectId, scope: IModelDb.rootSubjectId, value: `${2*index + code}`}),
    userLabel: `${2*index + code}`,
    federationGuid: fedGuid, // Guid.empty = 00000000-0000-0000-0000-000000000000
  }, sourceDb).insert();

  sourceDb.saveChanges();
  sourceDb.close();
  return elem;
}

function insertRelationship(sourceDbpath: string, elem1: string, elem2: string){
  const sourceDb = StandaloneDb.openFile(sourceDbpath, OpenMode.ReadWrite);

  const rel = new ElementGroupsMembers({
    classFullName: ElementGroupsMembers.classFullName,
    sourceId: elem1,
    targetId: elem2,
    memberPriority: 1,
  }, sourceDb);

  rel.insert();
  sourceDb.saveChanges();
  sourceDb.close();
}
