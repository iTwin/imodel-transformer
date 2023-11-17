import * as fs from "fs";
import { ElementGroupsMembers, ExternalSource, ExternalSourceAspect, ExternalSourceIsInRepository, IModelDb, IModelHost, PhysicalModel, PhysicalObject, RepositoryLink, SpatialCategory, StandaloneDb } from "@itwin/core-backend";
import { initializeBranchProvenance, ProvenanceInitArgs, ProvenanceInitResult } from "../../BranchProvenanceInitializer";
import { assertIdentityTransformation, IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { BriefcaseIdValue, Code } from "@itwin/core-common";
import { IModelTransformer } from "../../IModelTransformer";
import { Guid, OpenMode, TupleKeyedMap } from "@itwin/core-bentley";
import { assert, expect } from "chai";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests

describe.only("compare imodels from BranchProvenanceInitializer and traditional branch init", async () => {
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

  const sourceTargetFedGuidToElemIds = new TupleKeyedMap<[boolean, boolean], [string, string]>();
    // [
    //   [[false, false], [sourceElem, targetElem]] ?
    // ])

  const sourceFileName = "ProvInitSource_STC";
  const sourcePath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", sourceFileName);
  if (fs.existsSync(sourcePath))
    fs.unlinkSync(sourcePath);

  const generatedIModel = StandaloneDb.createEmpty(sourcePath, { rootSubject: { name: sourceFileName }});

  for (const sourceHasFedGuid of [true, false]) {
    for (const targetHasFedGuid of [true, false]) {
        const physModelId = PhysicalModel.insert(generatedIModel, IModelDb.rootSubjectId, "physical model");
        const categoryId = SpatialCategory.insert(generatedIModel, IModelDb.dictionaryId, "spatial category", {});

        const baseProps = {
          classFullName: PhysicalObject.classFullName,
          category: categoryId,
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(1, 1, 1),
            angles: YawPitchRollAngles.createDegrees(1, 1, 1),
          },
          model: physModelId,
        };

        const sourceFedGuid = sourceHasFedGuid ? undefined : Guid.empty;
        const sourceElem = new PhysicalObject({
          ...baseProps,
          code: Code.createEmpty(),
          federationGuid: sourceFedGuid,
        }, generatedIModel).insert();

        const targetFedGuid = targetHasFedGuid ? undefined : Guid.empty;
        const targetElem = new PhysicalObject({
          ...baseProps,
          code: Code.createEmpty(),
          federationGuid: targetFedGuid,
        }, generatedIModel).insert();

        generatedIModel.saveChanges();

        sourceTargetFedGuidToElemIds.set([sourceHasFedGuid, targetHasFedGuid], [sourceElem, targetElem]);

        const rel = new ElementGroupsMembers({
          classFullName: ElementGroupsMembers.classFullName,
          sourceId: sourceElem,
          targetId: targetElem,
          memberPriority: 1,
        }, generatedIModel);
        rel.insert();
        generatedIModel.saveChanges();
        generatedIModel.performCheckpoint();
    }
  }

  let transformerBranchInitResult: ProvenanceInitResult | undefined;
  let noTransformerBranchInitResult: ProvenanceInitResult | undefined;
  let transformerForkDb: StandaloneDb | undefined;
  let noTransformerForkDb: StandaloneDb | undefined;

  for (const doBranchProv of [true, false]) {
    for (const createFedGuidsForMaster of ["keep-reopened-db", false] as const) {
      const masterPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", `${doBranchProv ? "noTransformer" : "Transformer"}_${createFedGuidsForMaster ?? "createFedGuids"}Master_STC`);
      const forkPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", `${doBranchProv ? "noTransformer" : "Transformer"}_${createFedGuidsForMaster ?? "createFedGuids"}Fork_STC`);

      await Promise.all([
        fs.promises.copyFile(generatedIModel.pathName, masterPath),
        fs.promises.copyFile(generatedIModel.pathName, forkPath),
      ]);

      setToStandalone(masterPath);
      setToStandalone(forkPath);
      const masterMode = createFedGuidsForMaster ? OpenMode.ReadWrite : OpenMode.Readonly;
      let masterDb = StandaloneDb.openFile(masterPath, masterMode);
      let forkDb = StandaloneDb.openFile(forkPath, OpenMode.ReadWrite);

      const baseInitProvenanceArgs = {
        createFedGuidsForMaster,
        masterDescription: "master iModel repository",
        masterUrl: "https://example.com/mytest",
      };

      const initProvenanceArgs: ProvenanceInitArgs = {
        ...baseInitProvenanceArgs,
        master: masterDb,
        branch: forkDb,
      };

      if (doBranchProv) {
        noTransformerBranchInitResult = await initializeBranchProvenance(initProvenanceArgs);
        // initializeBranchProvenance resets the passed in databases when we use "keep-reopened-db"
        masterDb = initProvenanceArgs.master as StandaloneDb;
        forkDb = initProvenanceArgs.branch as StandaloneDb;
        forkDb.saveChanges();
        noTransformerForkDb = forkDb;

        for (const [key, value] of sourceTargetFedGuidToElemIds) { // This loop sort of has some of the test cases hidden in it I guess? not sure if I can do something with this but maybe.
          // for an example, here we hit four scenarios. (true, true), (true, false), (false, true), (false, false)
          // relSourceHasFedGuid, relTargetHasFedGuid
          // relSourceHasFedGuid
          //                    , relTargetHasFedGuid
          // no fed guids.
          // ALL with branch provenance AKA no transformer
          // Seems agnostic to createFedGuidsForMaster (makes sense bvecause we have entries for this in our giga map). so this tests on all branchProvenance runs. 
          //
          const [sourceHasFedGuid, targetHasFedGuid] = key;
          const [sourceElem, targetElem] = value;
          it(`branch provenance init with ${[
            sourceHasFedGuid && "relSourceHasFedGuid",
            targetHasFedGuid && "relTargetHasFedGuid",
            createFedGuidsForMaster && "createFedGuidsForMaster",
          ].filter(Boolean)
           .join(",")
          }`, async () => {
            const sourceNumAspects = forkDb.elements.getAspects(sourceElem, ExternalSourceAspect.classFullName).length;
            const targetNumAspects = forkDb.elements.getAspects(targetElem, ExternalSourceAspect.classFullName).length;
            expect([sourceNumAspects, targetNumAspects])
            .to.deep.equal(sourceTargetFedGuidToAspectCountMap.get([sourceHasFedGuid, targetHasFedGuid, createFedGuidsForMaster]));

            // Weirdly though this piece is supposedly not agnostic to createFedGuidsForMaster. I'm guessing it was intentional
            // Probably because if createFedGuidsForMaster true, then expectedSourceAspectNum and expectedTargetAspectNum are 0 always which mike didn't see as worth testing. I sort of see that.
            // Because it would just complicate the conditional a bit.. not horrible though I would say.
            // I think this piece will fail without my extra commented out conditions. Would be nice to verify / validate that before I add the conditions in.
            const relHasFedguidProvenance = sourceHasFedGuid && targetHasFedGuid; // || createFedGuidsForMaster
            const expectedSourceAspectNum
                = (sourceHasFedGuid ? 0 : 1) // sourceHasFedGuid ? 0 : createFedGuidsForMaster ? 0 : 1
                + (relHasFedguidProvenance ? 0 : 1);
            const expectedTargetAspectNum = targetHasFedGuid ? 0 : 1;
            expect(sourceNumAspects).to.equal(expectedSourceAspectNum);
            expect(targetNumAspects).to.equal(expectedTargetAspectNum);
          });
        }
      } else {
        transformerBranchInitResult = await classicalTransformerBranchInit({
          ...baseInitProvenanceArgs,
          master: masterDb,
          branch: forkDb,
        });
        forkDb.saveChanges();
        transformerForkDb = forkDb;
      }
    }
  }

  // This tests !createFedGuidsForMaster approach. Comparing branchProv to classic
  // in old test setup we run the assert in these below cases
  //  relSourceHasFedGuid, relTargetHasFedGuid branchProv<->classic
  //  relSourceHasFedGuid branchProv<->classic
  //  relTargetHasFedGuid branchProv<->classic
  //  nofedGuids branchProv<->classic
  // This makes me wonder did some of these permutations even do anything special??
  // Now we essentially have branchProv<->classic. ALL cases built in.
  it(`should produce identityTransformation with branchProvenance and classic transformer provenance`, async () => {
    assert(transformerForkDb !== undefined && noTransformerForkDb !== undefined && transformerBranchInitResult !== undefined && noTransformerBranchInitResult !== undefined);
    await assertIdentityTransformation(transformerForkDb, noTransformerForkDb, undefined, {
      allowPropChange(inSourceElem, inTargetElem, propName) {
        if (propName !== "federationGuid")
          return undefined;

        if (inTargetElem.id === noTransformerBranchInitResult!.masterRepositoryLinkId
          && inSourceElem.id === transformerBranchInitResult!.masterRepositoryLinkId)
            return true;
        if (inTargetElem.id === noTransformerBranchInitResult!.masterExternalSourceId
          && inSourceElem.id === transformerBranchInitResult!.masterExternalSourceId)
            return true;

        return undefined;
      },
    });
    });
});
  // FIXME: don't use a separate iModel for each loop iteration, just add more element pairs
  // to the one iModel. That will be much faster

  // Somehow each loop has to know the sourceElem and targetElem associated with its respective boolean tuple or whatever atleast with my current approach.
  // I could also just do the insert and the test in the same loop I guess? and just make sure Im not surprised by the extra elements?
  // well problem is we currently dupe the file each permutation. and we only need to dupe the file once(twice for non transformer vs transformer) which is why im leaning towards 4th inner loop. 
  // A fourth inner loop over "nonTransformer", "transformer" could also work I think. possibly should visit this after I setup the rest.. 
  // there are comparisons made between nontransformer and transformer within the loop so 4th inner loop doesn't work that easily.
  // Maybe I do a single loop after the quadra loop where i do the assert
//   for (const sourceHasFedguid of [true, false]) {
//     for (const targetHasFedguid of [true, false]) {
//       for (const createFedGuidsForMaster of ["keep-reopened-db", false] as const) {
//         it(`branch provenance init with ${[
//           sourceHasFedguid && "relSourceHasFedGuid",
//           targetHasFedguid && "relTargetHasFedGuid",
//           createFedGuidsForMaster && "createFedGuidsForMaster",
//         ].filter(Boolean)
//          .join(",")
//         }`, async () => {
//           let transformerMasterDb!: StandaloneDb;
//           let noTransformerMasterDb!: StandaloneDb;
//           let transformerForkDb!: StandaloneDb;
//           let noTransformerForkDb!: StandaloneDb;

//           try {
//             const suffixName = (s: string) => `${s}_${sourceHasFedguid ? "S" : "_"}${targetHasFedguid ? "T" : "_"}${createFedGuidsForMaster ? "C" : "_"}.bim`;
//             const sourceFileName = suffixName("ProvInitSource");
//             const sourcePath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", sourceFileName);
//             if (fs.existsSync(sourcePath))
//               fs.unlinkSync(sourcePath);

//             const generatedIModel = StandaloneDb.createEmpty(sourcePath, { rootSubject: { name: sourceFileName }});

//             const physModelId = PhysicalModel.insert(generatedIModel, IModelDb.rootSubjectId, "physical model");
//             const categoryId = SpatialCategory.insert(generatedIModel, IModelDb.dictionaryId, "spatial category", {});

//             const baseProps = {
//               classFullName: PhysicalObject.classFullName,
//               category: categoryId,
//               geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
//               placement: {
//                 origin: Point3d.create(1, 1, 1),
//                 angles: YawPitchRollAngles.createDegrees(1, 1, 1),
//               },
//               model: physModelId,
//             };

//             const sourceFedGuid = sourceHasFedguid ? undefined : Guid.empty;
//             const sourceElem = new PhysicalObject({
//               ...baseProps,
//               code: Code.createEmpty(),
//               federationGuid: sourceFedGuid,
//             }, generatedIModel).insert();

//             const targetFedGuid = targetHasFedguid ? undefined : Guid.empty;
//             const targetElem = new PhysicalObject({
//               ...baseProps,
//               code: Code.createEmpty(),
//               federationGuid: targetFedGuid,
//             }, generatedIModel).insert();

//             generatedIModel.saveChanges();

//             const rel = new ElementGroupsMembers({
//               classFullName: ElementGroupsMembers.classFullName,
//               sourceId: sourceElem,
//               targetId: targetElem,
//               memberPriority: 1,
//             }, generatedIModel);
//             rel.insert();
//             generatedIModel.saveChanges();
//             generatedIModel.performCheckpoint();

//             const transformerMasterPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("TransformerMaster"));
//             const transformerForkPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("TransformerFork.bim"));
//             const noTransformerMasterPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("NoTransformerMaster"));
//             const noTransformerForkPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", suffixName("NoTransformerFork"));
//             await Promise.all([
//               fs.promises.copyFile(generatedIModel.pathName, transformerForkPath),
//               fs.promises.copyFile(generatedIModel.pathName, transformerMasterPath),
//               fs.promises.copyFile(generatedIModel.pathName, noTransformerForkPath),
//               fs.promises.copyFile(generatedIModel.pathName, noTransformerMasterPath),
//             ]);
//             setToStandalone(transformerForkPath);
//             setToStandalone(transformerMasterPath);
//             setToStandalone(noTransformerForkPath);
//             setToStandalone(noTransformerMasterPath);
//             const masterMode = createFedGuidsForMaster ? OpenMode.ReadWrite : OpenMode.Readonly;
//             transformerMasterDb = StandaloneDb.openFile(transformerMasterPath, masterMode);
//             transformerForkDb = StandaloneDb.openFile(transformerForkPath, OpenMode.ReadWrite);
//             noTransformerMasterDb = StandaloneDb.openFile(noTransformerMasterPath, masterMode);
//             noTransformerForkDb = StandaloneDb.openFile(noTransformerForkPath, OpenMode.ReadWrite);

//             const baseInitProvenanceArgs = {
//               createFedGuidsForMaster,
//               masterDescription: "master iModel repository",
//               masterUrl: "https://example.com/mytest",
//             };

//             const initProvenanceArgs: ProvenanceInitArgs = {
//               ...baseInitProvenanceArgs,
//               master: noTransformerMasterDb,
//               branch: noTransformerForkDb,
//             };
//             const noTransformerBranchInitResult = await initializeBranchProvenance(initProvenanceArgs);
//             // initializeBranchProvenance resets the passed in databases when we use "keep-reopened-db"
//             noTransformerMasterDb = initProvenanceArgs.master as StandaloneDb;
//             noTransformerForkDb = initProvenanceArgs.branch as StandaloneDb;

//             noTransformerForkDb.saveChanges();

//             const transformerBranchInitResult = await classicalTransformerBranchInit({
//               ...baseInitProvenanceArgs,
//               master: transformerMasterDb,
//               branch: transformerForkDb,
//             });
//             transformerForkDb.saveChanges();

//             const sourceNumAspects = noTransformerForkDb.elements.getAspects(sourceElem, ExternalSourceAspect.classFullName).length;
//             const targetNumAspects = noTransformerForkDb.elements.getAspects(targetElem, ExternalSourceAspect.classFullName).length;

//             expect([sourceNumAspects, targetNumAspects])
//               .to.deep.equal(sourceTargetFedGuidToAspectCountMap.get([sourceHasFedguid, targetHasFedguid, createFedGuidsForMaster]));

//             if (!createFedGuidsForMaster) {
//               // logical tests
//               const relHasFedguidProvenance = sourceHasFedguid && targetHasFedguid;
//               const expectedSourceAspectNum
//                 = (sourceHasFedguid ? 0 : 1)
//                 + (relHasFedguidProvenance ? 0 : 1);
//               const expectedTargetAspectNum = targetHasFedguid ? 0 : 1;

//               expect(sourceNumAspects).to.equal(expectedSourceAspectNum); // This only compares with notransformerdb, what about above chunk though? pretty much same. because we only
//               // get aspects from the notransformerforkdb. 
//               expect(targetNumAspects).to.equal(expectedTargetAspectNum);

//               // okay so now I need to after my 2nd nested for loop. I need to run assertIdentityTransformation 
//               // but only if !createFedGuidsForMaster. So I need to save off the dbs and the fork result of both provenance approaches if !createFedGuidsForMaster? 
//               await assertIdentityTransformation(transformerForkDb, noTransformerForkDb, undefined, { 
//                 allowPropChange(inSourceElem, inTargetElem, propName) {
//                   if (propName !== "federationGuid")
//                     return undefined;

//                   if (inTargetElem.id === noTransformerBranchInitResult.masterRepositoryLinkId
//                    && inSourceElem.id === transformerBranchInitResult.masterRepositoryLinkId)
//                       return true;
//                   if (inTargetElem.id === noTransformerBranchInitResult.masterExternalSourceId
//                    && inSourceElem.id === transformerBranchInitResult.masterExternalSourceId)
//                       return true;

//                   return undefined;
//                 },
//               });
//             }
//           } finally {
//             transformerMasterDb?.close();
//             transformerForkDb?.close();
//             noTransformerMasterDb?.close();
//             noTransformerForkDb?.close();
//           }
//         });
//       }
//     }
//   }
// });

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
    // eslint-disable-next-line @typescript-eslint/no-var-requires
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
