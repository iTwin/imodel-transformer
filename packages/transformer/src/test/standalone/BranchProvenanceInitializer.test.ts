import * as fs from "fs";
import { ElementGroupsMembers, ExternalSource, ExternalSourceAspect, ExternalSourceIsInRepository, IModelDb, IModelHost, PhysicalModel, PhysicalObject, RepositoryLink, SpatialCategory, StandaloneDb } from "@itwin/core-backend";
import { ProvenanceInitArgs, ProvenanceInitResult, initializeBranchProvenance } from "../../BranchProvenanceInitializer";
import { IModelTransformerTestUtils, assertIdentityTransformation } from "../IModelTransformerUtils";
import { BriefcaseIdValue, Code } from "@itwin/core-common";
import { IModelTransformer } from "../../IModelTransformer";
import { Guid, OpenMode, TupleKeyedMap } from "@itwin/core-bentley";
import { expect } from "chai";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";

interface InsertParams {
  db: StandaloneDb;
  physModelId: string;
  categoryId: string;
}

describe("compare imodels from BranchProvenanceInitializer and traditional branch init", () => {
  // truth table (sourceHasFedGuid, targetHasFedGuid, forceCreateFedGuidsForMaster) -> (relSourceAspectNum, relTargetAspectNum)
  const sourceTargetFedGuidToAspectCountMap = new TupleKeyedMap([
    [[false, false, false], [2, 1]],
    // "keep-reopened-db" is truthy but also equal to an optimized optional argument that we use
    [[false, false, "keep-reopened-db"], [0, 0]],
    [[false, true, false], [2, 0]],
    [[false, true, "keep-reopened-db"], [0, 0]],
    [[true, false, false], [1, 1]],
    [[true, false, "keep-reopened-db"], [0, 0]],
    [[true, true, false], [0, 0]],
    [[true, true, "keep-reopened-db"], [0, 0]],
  ]);

  let index = 0;
  // FIXME: don't use a separate iModel for each loop iteration, just add more element pairs
  // to the one iModel. That will be much faster
  for (const sourceHasFedguid of [true, false]) {
    for (const targetHasFedguid of [true, false]) {
      for (const createFedGuidsForMaster of ["keep-reopened-db", false] as const) {
        it(`branch provenance init with ${[
          sourceHasFedguid && "relSourceHasFedGuid",
          targetHasFedguid && "relTargetHasFedGuid",
          createFedGuidsForMaster && "createFedGuidsForMaster",
        ].filter(Boolean)
         .join(",")
        }`, async () => {
          let transformerMasterDb!: StandaloneDb;
          let noTransformerMasterDb!: StandaloneDb;
          let transformerForkDb!: StandaloneDb;
          let noTransformerForkDb!: StandaloneDb;

          try {
            const suffixName = (s: string) => `${s}_${sourceHasFedguid ? "S" : "_"}${targetHasFedguid ? "T" : "_"}${createFedGuidsForMaster ? "C" : "_"}.bim`;
            const sourceFileName = suffixName("ProvInitSource");
            const generatedIModel = generateEmptyIModel(sourceFileName);

            const sourceElem = insertElementToImodel(generatedIModel, sourceHasFedguid, index);
            const targetElem = insertElementToImodel(generatedIModel, targetHasFedguid, index);
            const rel = new ElementGroupsMembers({
              classFullName: ElementGroupsMembers.classFullName,
              sourceId: sourceElem,
              targetId: targetElem,
              memberPriority: 1,
            }, generatedIModel.db);
            rel.insert();
            generatedIModel.db.saveChanges();
            generatedIModel.db.performCheckpoint();

            const transformerMasterPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("TransformerMaster"));
            const transformerForkPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("TransformerFork.bim"));
            const noTransformerMasterPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("NoTransformerMaster"));
            const noTransformerForkPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("NoTransformerFork"));
            await Promise.all([
              fs.promises.copyFile(generatedIModel.db.pathName, transformerForkPath),
              fs.promises.copyFile(generatedIModel.db.pathName, transformerMasterPath),
              fs.promises.copyFile(generatedIModel.db.pathName, noTransformerForkPath),
              fs.promises.copyFile(generatedIModel.db.pathName, noTransformerMasterPath),
            ]);
            setToStandalone(transformerForkPath);
            setToStandalone(transformerMasterPath);
            setToStandalone(noTransformerForkPath);
            setToStandalone(noTransformerMasterPath);
            const masterMode = createFedGuidsForMaster ? OpenMode.ReadWrite : OpenMode.Readonly;
            transformerMasterDb = StandaloneDb.openFile(transformerMasterPath, masterMode);
            transformerForkDb = StandaloneDb.openFile(transformerForkPath, OpenMode.ReadWrite);
            noTransformerMasterDb = StandaloneDb.openFile(noTransformerMasterPath, masterMode);
            noTransformerForkDb = StandaloneDb.openFile(noTransformerForkPath, OpenMode.ReadWrite);

            const baseInitProvenanceArgs = {
              createFedGuidsForMaster,
              masterDescription: "master iModel repository",
              masterUrl: "https://example.com/mytest",
            };

            const transformerBranchInitResult = await classicalTransformerBranchInit({
              ...baseInitProvenanceArgs,
              master: transformerMasterDb,
              branch: transformerForkDb,
            });

            const initProvenanceArgs: ProvenanceInitArgs = {
              ...baseInitProvenanceArgs,
              master: noTransformerMasterDb,
              branch: noTransformerForkDb,
            };
            const noTransformerBranchInitResult = await initializeBranchProvenance(initProvenanceArgs);
            // initializeBranchProvenance can reset the passed in databases when we use "keep-reopened-db"
            noTransformerMasterDb = initProvenanceArgs.master as StandaloneDb;
            noTransformerForkDb = initProvenanceArgs.branch as StandaloneDb;

            noTransformerForkDb.saveChanges();

            const sourceNumAspects = noTransformerForkDb.elements.getAspects(sourceElem, ExternalSourceAspect.classFullName).length;
            const targetNumAspects = noTransformerForkDb.elements.getAspects(targetElem, ExternalSourceAspect.classFullName).length;

            expect(sourceTargetFedGuidToAspectCountMap.get([sourceHasFedguid, targetHasFedguid, createFedGuidsForMaster]))
              .to.deep.equal([sourceNumAspects, targetNumAspects]);

            if (!createFedGuidsForMaster) {
              // logical tests
              const relHasFedguidProvenance = sourceHasFedguid && targetHasFedguid;
              const expectedSourceAspectNum
                = (sourceHasFedguid ? 0 : 1)
                + (relHasFedguidProvenance ? 0 : 1);
              const expectedTargetAspectNum = targetHasFedguid ? 0 : 1;

              expect(sourceNumAspects).to.equal(expectedSourceAspectNum);
              expect(targetNumAspects).to.equal(expectedTargetAspectNum);

              await assertIdentityTransformation(transformerForkDb, noTransformerForkDb, undefined, {
                allowPropChange(inSourceElem, inTargetElem, propName) {
                  if (propName !== "federationGuid")
                    return undefined;

                  if (inTargetElem.id === noTransformerBranchInitResult.masterRepositoryLinkId
                   && inSourceElem.id === transformerBranchInitResult.masterRepositoryLinkId)
                      return true;
                  if (inTargetElem.id === noTransformerBranchInitResult.masterExternalSourceId
                   && inSourceElem.id === transformerBranchInitResult.masterExternalSourceId)
                      return true;

                  return undefined;
                },
              });
            }
          } finally {
            transformerMasterDb?.close();
            transformerForkDb?.close();
            noTransformerMasterDb?.close();
            noTransformerForkDb?.close();
          }
        });
        ++index;
      }
    }
  }
});

async function classicalTransformerBranchInit(args: ProvenanceInitArgs): Promise<ProvenanceInitResult> {
  // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
  const masterLinkRepoId = new RepositoryLink({
    classFullName: RepositoryLink.classFullName,
    code: RepositoryLink.createCode(args.branch, IModelDb.repositoryModelId, "test-imodel"),
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
    connectorName: require("../../../../package.json").name,
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    connectorVersion: require("../../../../package.json").version,
  }, args.branch).insert();

  // initialize the branch provenance
  const branchInitializer = new IModelTransformer(args.master, args.branch, {
    // tells the transformer that we have a raw copy of a source and the target should receive
    // provenance from the source that is necessary for performing synchronizations in the future
    wasSourceIModelCopiedToTarget: true,
    // store the synchronization provenance in the scope of our representation of the external source, master
    targetScopeElementId: masterExternalSourceId,
  });

  await branchInitializer.processAll();
  // save+push our changes to whatever hub we're using
  const description = "initialized branch iModel";
  args.branch.saveChanges(description);

  branchInitializer.dispose();

  return {
    masterExternalSourceId,
    targetScopeElementId: masterExternalSourceId,
    masterRepositoryLinkId: masterLinkRepoId,
  };
}

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

export function generateEmptyIModel(fileName: string): InsertParams {
  const sourcePath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", fileName);
  if (fs.existsSync(sourcePath))
    fs.unlinkSync(sourcePath);

  const db = StandaloneDb.createEmpty(sourcePath, { rootSubject: { name: fileName }});

  const physModelId = PhysicalModel.insert(db, IModelDb.rootSubjectId, "physical model");
  const categoryId = SpatialCategory.insert(db, IModelDb.dictionaryId, "spatial category", {});

  return {
    db,
    physModelId,
    categoryId,
  };
}

function insertElementToImodel(insertData: InsertParams, withFedGuid: boolean, index: number): string {
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
    code: Code.createEmpty(),
    userLabel: `${2*index}`,
    federationGuid: fedGuid, // Guid.empty = 00000000-0000-0000-0000-000000000000
  }, insertData.db).insert();

  insertData.db.saveChanges();
  return elem;
}
