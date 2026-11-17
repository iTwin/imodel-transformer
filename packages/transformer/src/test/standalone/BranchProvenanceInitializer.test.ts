/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as fs from "node:fs";
import {
  ElementGroupsMembers,
  ExternalSource,
  ExternalSourceAspect,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  RepositoryLink,
  SpatialCategory,
  StandaloneDb,
  withEditTxn,
} from "@itwin/core-backend";
import {
  initializeBranchProvenance,
  ProvenanceInitArgs,
} from "../../BranchProvenanceInitializer.js";
import {
  expectTransformerError,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { Code, IModel } from "@itwin/core-common";
import { TargetScopeProvenanceJsonProps } from "../../IModelTransformer.js";
import { IModelTransformerError } from "../../IModelTransformerError.js";
import { SyncTypeResolver } from "../../SyncTypeResolver.js";
import { Guid, Id64String, OpenMode, TupleKeyedMap } from "@itwin/core-bentley";
import { expect } from "vitest";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";

describe("BranchProvenanceInitializer", () => {
  // truth table (sourceHasFedGuid, targetHasFedGuid, forceCreateFedGuidsForMaster) -> (relSourceAspectNum, relTargetAspectNum)
  const sourceTargetFedGuidToAspectCountMap = new TupleKeyedMap([
    [
      [false, false, false],
      [2, 1],
    ],
    // "keep-reopened-db" is truthy but also equal to an optimized optional argument that we use
    [
      [false, false, "keep-reopened-db"],
      [0, 0],
    ],
    [
      [false, true, false],
      [2, 0],
    ],
    [
      [false, true, "keep-reopened-db"],
      [0, 0],
    ],
    [
      [true, false, false],
      [1, 1],
    ],
    [
      [true, false, "keep-reopened-db"],
      [0, 0],
    ],
    [
      [true, true, false],
      [0, 0],
    ],
    [
      [true, true, "keep-reopened-db"],
      [0, 0],
    ],
  ]);

  let generatedIModel: StandaloneDb;
  let sourceTargetFedGuidsToElemIds: TupleKeyedMap<
    [boolean, boolean],
    [string, string]
  >;

  beforeAll(async () => {
    [generatedIModel, sourceTargetFedGuidsToElemIds] = setupIModel();
  });

  afterAll(() => {
    generatedIModel.close();
  });

  function copyMasterAndBranch(
    name: string,
    masterMode = OpenMode.Readonly
  ): { masterDb: StandaloneDb; branchDb: StandaloneDb } {
    const masterPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      `${name}Master_STC.bim`
    );
    const branchPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      `${name}Branch_STC.bim`
    );
    fs.copyFileSync(generatedIModel.pathName, masterPath);
    fs.copyFileSync(generatedIModel.pathName, branchPath);
    setToStandalone(masterPath);
    setToStandalone(branchPath);
    return {
      masterDb: StandaloneDb.openFile(masterPath, masterMode),
      branchDb: StandaloneDb.openFile(branchPath, OpenMode.ReadWrite),
    };
  }

  function getScopeAspects(db: IModelDb, elementId: Id64String) {
    return db.elements
      .getAspects(elementId, ExternalSourceAspect.classFullName)
      .filter(
        (aspect) =>
          (aspect as ExternalSourceAspect).kind ===
          ExternalSourceAspect.Kind.Scope
      ) as ExternalSourceAspect[];
  }

  for (const createFedGuidsForMaster of ["keep-reopened-db", false] as const) {
    it(`inserts element and relationship provenance${
      createFedGuidsForMaster ? " with createFedGuidsForMaster" : ""
    }`, async () => {
      let { masterDb, branchDb } = copyMasterAndBranch(
        `elemProv_${createFedGuidsForMaster}`,
        createFedGuidsForMaster ? OpenMode.ReadWrite : OpenMode.Readonly
      );

      const initProvenanceArgs: ProvenanceInitArgs = {
        createFedGuidsForMaster,
        masterDescription: "master iModel repository",
        masterUrl: "https://example.com/mytest",
        master: masterDb,
        branch: branchDb,
      };
      // __PUBLISH_EXTRACT_START__ EditTxnInTransformer.initialize-branch-provenance
      // initializeBranchProvenance creates and ends its own EditTxn.
      const result = await initializeBranchProvenance(initProvenanceArgs);
      // __PUBLISH_EXTRACT_END__
      // initializeBranchProvenance resets the passed in databases when we use "keep-reopened-db"
      masterDb = initProvenanceArgs.master as StandaloneDb;
      branchDb = initProvenanceArgs.branch as StandaloneDb;

      try {
        expect(result.masterExternalSourceId).to.equal(
          result.targetScopeElementId
        );
        expect(
          branchDb.elements.getElement(result.masterExternalSourceId!)
        ).to.be.instanceOf(ExternalSource);
        expect(
          branchDb.elements.getElement(result.masterRepositoryLinkId!)
        ).to.be.instanceOf(RepositoryLink);

        // Assert all 4 permutations of sourceHasFedGuid,targetHasFedGuid matches our expectations
        for (const sourceHasFedGuid of [true, false]) {
          for (const targetHasFedGuid of [true, false]) {
            const logMessage = () => {
              return `Expected the createFedGuidsForMaster: ${createFedGuidsForMaster} element pair: sourceHasFedGuid: ${sourceHasFedGuid}, targetHasFedGuid: ${targetHasFedGuid}`;
            };
            const [sourceElem, targetElem] = sourceTargetFedGuidsToElemIds.get([
              sourceHasFedGuid,
              targetHasFedGuid,
            ])!;
            const sourceNumAspects = branchDb.elements.getAspects(
              sourceElem,
              ExternalSourceAspect.classFullName
            ).length;
            const targetNumAspects = branchDb.elements.getAspects(
              targetElem,
              ExternalSourceAspect.classFullName
            ).length;
            const expectedNumAspects = sourceTargetFedGuidToAspectCountMap.get([
              sourceHasFedGuid,
              targetHasFedGuid,
              createFedGuidsForMaster,
            ])!;
            expect(
              [sourceNumAspects, targetNumAspects],
              `${logMessage()} to have sourceNumAspects: ${
                expectedNumAspects[0]
              } got ${sourceNumAspects}, targetNumAspects: ${
                expectedNumAspects[1]
              } got ${targetNumAspects}`
            ).to.deep.equal(expectedNumAspects);
          }
        }
      } finally {
        masterDb.close();
        branchDb.close();
      }
    });
  }

  it("inserts scope provenance that establishes the master-branch relationship", async () => {
    const { masterDb, branchDb } = copyMasterAndBranch("scopeProv");
    try {
      await expect(
        SyncTypeResolver.determineSyncType(
          masterDb,
          branchDb,
          IModel.rootSubjectId
        )
      ).rejects.toThrow(SyncTypeResolver.noEsaSyncDirectionErrorMessage);

      const result = await initializeBranchProvenance({
        master: masterDb,
        branch: branchDb,
      });

      const scopeAspects = getScopeAspects(
        branchDb,
        result.targetScopeElementId
      );
      expect(scopeAspects).to.have.length(1);
      const [scopeAspect] = scopeAspects;
      expect(scopeAspect.identifier).to.equal(masterDb.iModelId);
      expect(scopeAspect.scope?.id).to.equal(IModel.rootSubjectId);
      expect(scopeAspect.version).to.equal(
        `${masterDb.changeset.id};${masterDb.changeset.index}`
      );
      expect(
        JSON.parse(
          scopeAspect.jsonProperties as unknown as string
        ) as TargetScopeProvenanceJsonProps
      ).to.deep.equal({
        // the branch's next changeset holds these initialization changes
        pendingReverseSyncChangesetIndices: [branchDb.changeset.index! + 1],
        pendingSyncChangesetIndices: [],
        reverseSyncVersion: `${branchDb.changeset.id};${branchDb.changeset.index}`,
      });

      expect(
        await SyncTypeResolver.determineSyncType(
          masterDb,
          branchDb,
          result.targetScopeElementId
        )
      ).to.equal("forward");
      expect(
        await SyncTypeResolver.determineSyncType(
          branchDb,
          masterDb,
          result.targetScopeElementId
        )
      ).to.equal("reverse");
    } finally {
      masterDb.close();
      branchDb.close();
    }
  });

  it("uses an existing targetScopeElementId", async () => {
    const { masterDb, branchDb } = copyMasterAndBranch("existingScope");
    try {
      const elementCount = (db: IModelDb) =>
        db.withPreparedSqliteStatement(
          "SELECT COUNT(*) FROM bis_Element",
          (stmt) => {
            stmt.step();
            return stmt.getValueInteger(0);
          }
        );
      const elementCountBefore = elementCount(branchDb);

      const result = await initializeBranchProvenance({
        master: masterDb,
        branch: branchDb,
        targetScopeElementId: IModel.rootSubjectId,
      });

      expect(result).to.deep.equal({
        targetScopeElementId: IModel.rootSubjectId,
        masterExternalSourceId: undefined,
        masterRepositoryLinkId: undefined,
      });
      expect(elementCount(branchDb)).to.equal(elementCountBefore);
      expect(getScopeAspects(branchDb, IModel.rootSubjectId)).to.have.length(1);

      const [fedGuidlessSourceElem] = sourceTargetFedGuidsToElemIds.get([
        false,
        true,
      ])!;
      const elementAspects = branchDb.elements.getAspects(
        fedGuidlessSourceElem,
        ExternalSourceAspect.classFullName
      ) as ExternalSourceAspect[];
      expect(elementAspects).to.not.be.empty;
      for (const aspect of elementAspects)
        expect(aspect.scope?.id).to.equal(IModel.rootSubjectId);

      expect(
        await SyncTypeResolver.determineSyncType(
          masterDb,
          branchDb,
          IModel.rootSubjectId
        )
      ).to.equal("forward");
    } finally {
      masterDb.close();
      branchDb.close();
    }
  });

  it("throws if the targetScopeElementId already has scope provenance", async () => {
    const { masterDb, branchDb } = copyMasterAndBranch("scopeConflict");
    try {
      await initializeBranchProvenance({
        master: masterDb,
        branch: branchDb,
        targetScopeElementId: IModel.rootSubjectId,
      });
      const aspectCount = (db: IModelDb) =>
        db.withPreparedSqliteStatement(
          "SELECT COUNT(*) FROM bis_ElementMultiAspect",
          (stmt) => {
            stmt.step();
            return stmt.getValueInteger(0);
          }
        );
      const aspectCountBefore = aspectCount(branchDb);

      await expectTransformerError(
        initializeBranchProvenance({
          master: masterDb,
          branch: branchDb,
          targetScopeElementId: IModel.rootSubjectId,
        }),
        IModelTransformerError.ProvenanceScopeConflict,
        "Provenance scope conflict"
      );

      expect(aspectCount(branchDb)).to.equal(aspectCountBefore);
      // the failed call must not leave an EditTxn active on the branch
      withEditTxn(branchDb, "after conflict", () => {});
    } finally {
      masterDb.close();
      branchDb.close();
    }
  });
});

/**
 * setupIModel populates an empty StandaloneDb with four different element pairs.
 * Whats different about these 4 pairs is whether or not the elements within the pair have fedguids defined on them.
 * This gives us 4 pairs by permuting over sourceHasFedGuid (true/false) and targetHasFedGuid (true/false).
 * Each pair is also part of a relationship ElementGroupsMembers.
 * @returns a tuple containing the IModel and a TupleKeyedMap where the key is [boolean,boolean] (sourceHasFedGuid, targetHasFedGuid) and the value is [string,string] (sourceId, targetId).
 */
function setupIModel(): [
  StandaloneDb,
  TupleKeyedMap<[boolean, boolean], [string, string]>,
] {
  const sourceTargetFedGuidToElemIds = new TupleKeyedMap<
    [boolean, boolean],
    [string, string]
  >();
  const sourceFileName = "ProvInitSource_STC.bim";
  const sourcePath = IModelTransformerTestUtils.prepareOutputFile(
    "IModelTransformer",
    sourceFileName
  );
  if (fs.existsSync(sourcePath)) fs.unlinkSync(sourcePath);

  const generatedIModel = StandaloneDb.createEmpty(sourcePath, {
    rootSubject: { name: sourceFileName },
  });
  const { physModelId, categoryId } = withEditTxn(
    generatedIModel,
    "insert model and category",
    (txn) => ({
      physModelId: PhysicalModel.insert(
        txn,
        IModelDb.rootSubjectId,
        "physical model"
      ),
      categoryId: SpatialCategory.insert(
        txn,
        IModelDb.dictionaryId,
        "spatial category",
        {}
      ),
    })
  );

  for (const sourceHasFedGuid of [true, false]) {
    for (const targetHasFedGuid of [true, false]) {
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

      const { sourceElem, targetElem } = withEditTxn(
        generatedIModel,
        "insert physical objects",
        (txn) => {
          const sourceFedGuid = sourceHasFedGuid ? undefined : Guid.empty;
          const srcElem = new PhysicalObject(
            {
              ...baseProps,
              code: Code.createEmpty(),
              federationGuid: sourceFedGuid,
            },
            generatedIModel
          ).insert(txn);

          const targetFedGuid = targetHasFedGuid ? undefined : Guid.empty;
          const tgtElem = new PhysicalObject(
            {
              ...baseProps,
              code: Code.createEmpty(),
              federationGuid: targetFedGuid,
            },
            generatedIModel
          ).insert(txn);

          return { sourceElem: srcElem, targetElem: tgtElem };
        }
      );

      sourceTargetFedGuidToElemIds.set(
        [sourceHasFedGuid, targetHasFedGuid],
        [sourceElem, targetElem]
      );

      withEditTxn(generatedIModel, "insert relationship", (txn) => {
        const rel = new ElementGroupsMembers(
          {
            classFullName: ElementGroupsMembers.classFullName,
            sourceId: sourceElem,
            targetId: targetElem,
            memberPriority: 1,
          },
          generatedIModel
        );
        rel.insert(txn);
      });
      generatedIModel.performCheckpoint();
    }
  }
  return [generatedIModel, sourceTargetFedGuidToElemIds];
}

function setToStandalone(iModelName: string) {
  StandaloneDb.convertToStandalone(iModelName);
}
