/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import * as TestUtils from "../TestUtils";
import {
  BriefcaseDb,
  BriefcaseManager,
  EditTxn,
  ExternalSource,
  ExternalSourceIsInRepository,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  RepositoryLink,
  SnapshotDb,
  SpatialCategory,
} from "@itwin/core-backend";
import {
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { AccessToken } from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceProps,
  IModel,
  PhysicalElementProps,
  QueryBinder,
  RepositoryLinkProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelTransformer } from "../../IModelTransformer";
// Allow this example to use the monorepo's development versions of the core libraries.
process.env.TRANSFORMER_NO_STRICT_DEP_CHECK = "1";

// This example reads package metadata with require, so disable the rule for this file.
/* eslint-disable @typescript-eslint/no-require-imports */

async function initializeBranch(
  myITwinId: string,
  masterIModelId: string,
  myAccessToken: AccessToken
) {
  // __PUBLISH_EXTRACT_START__ IModelBranchingOperations_initialize
  // Download and open the master iModel.
  const masterDbProps = await BriefcaseManager.downloadBriefcase({
    accessToken: myAccessToken,
    iTwinId: myITwinId,
    iModelId: masterIModelId,
  });
  const masterDb = await BriefcaseDb.open({ fileName: masterDbProps.fileName });

  // Create the branch from the master's current file as its initial version.
  const branchIModelId = await transformerTestHub.createNewIModel({
    accessToken: myAccessToken,
    iTwinId: myITwinId,
    iModelName: "my-branch-imodel",
    version0: masterDb.pathName,
    // Enable locks when the application requires briefcase lock coordination.
    noLocks: true,
  });

  // Download and open the branch iModel.
  const branchDbProps = await BriefcaseManager.downloadBriefcase({
    accessToken: myAccessToken,
    iTwinId: myITwinId,
    iModelId: branchIModelId,
  });
  const branchDb = await BriefcaseDb.open({ fileName: branchDbProps.fileName });
  const branchEditTxn = new EditTxn(branchDb, "initialize branch iModel");
  branchEditTxn.start();

  // Create the repository link and external source that identify the master as the
  // branch's Target Scope Element.
  const masterLinkRepoId = branchDb
    .constructEntity<RepositoryLink, RepositoryLinkProps>({
      classFullName: RepositoryLink.classFullName,
      code: RepositoryLink.createCode(
        branchDb,
        IModelDb.repositoryModelId,
        "example-code-value"
      ),
      model: IModelDb.repositoryModelId,
      url: "https://wherever-you-got-your-imodel.net",
      format: "iModel",
      repositoryGuid: masterDb.iModelId,
      description: "master iModel repository",
    })
    .insert(branchEditTxn);

  const masterExternalSourceId = branchDb
    .constructEntity<ExternalSource, ExternalSourceProps>({
      classFullName: ExternalSource.classFullName,
      model: IModelDb.rootSubjectId,
      code: Code.createEmpty(),
      repository: new ExternalSourceIsInRepository(masterLinkRepoId),
      connectorName: "iModel Transformer",
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      connectorVersion: require("@itwin/imodel-transformer/package.json")
        .version,
    })
    .insert(branchEditTxn);

  // Initialize provenance for the branch.
  const branchInitializer = new IModelTransformer(
    { source: masterDb, target: branchEditTxn },
    {
      // The branch is a raw copy of the source, so record the provenance required for
      // future synchronization.
      wasSourceIModelCopiedToTarget: true,
      // Store synchronization provenance under the ExternalSource that represents the master.
      targetScopeElementId: masterExternalSourceId,
    }
  );
  await branchInitializer.process();
  branchInitializer.dispose();
  branchEditTxn.end("save");

  // Push the branch changes to the hub.
  const description = "initialized branch iModel";
  await branchDb.pushChanges({
    accessToken: myAccessToken,
    description,
  });
  // __PUBLISH_EXTRACT_END__

  return { masterDb, branchDb };
}

// The master and branch databases are already open (see the initialization example).
async function forwardSyncMasterToBranch(
  masterDb: BriefcaseDb,
  branchDb: BriefcaseDb,
  myAccessToken: AccessToken
) {
  // __PUBLISH_EXTRACT_START__ IModelBranchingOperations_forwardSync
  const repositoryLinkId = branchDb.elements.queryElementIdByCode(
    RepositoryLink.createCode(
      masterDb,
      IModelDb.repositoryModelId,
      "example-code-value"
    )
  );
  let masterExternalSourceId;
  for await (const row of branchDb.createQueryReader(
    `SELECT ECInstanceId FROM ${ExternalSource.classFullName} WHERE Repository.Id=:id`,
    QueryBinder.from({ id: repositoryLinkId })
  )) {
    masterExternalSourceId = row.ECInstanceId;
  }
  const branchEditTxn = new EditTxn(branchDb, "synchronize master changes");
  branchEditTxn.start();
  const synchronizer = new IModelTransformer(
    { source: masterDb, target: branchEditTxn },
    {
      // Read synchronization provenance for the ExternalSource that represents the master.
      targetScopeElementId: masterExternalSourceId,
      // Pass an empty argsForProcessChanges object to process the next unsynchronized changes.
      argsForProcessChanges: {},
    }
  );

  await synchronizer.process();
  synchronizer.dispose();
  branchEditTxn.end("save");
  // Push the synchronized branch changes.
  const description = "updated branch with recent master changes";
  await branchDb.pushChanges({
    accessToken: myAccessToken,
    description,
  });
  // __PUBLISH_EXTRACT_END__
}

async function reverseSyncBranchToMaster(
  branchDb: BriefcaseDb,
  masterDb: BriefcaseDb,
  myAccessToken: AccessToken
) {
  // __PUBLISH_EXTRACT_START__ IModelBranchingOperations_reverseSync
  // The master and branch databases are already open (see the initialization example).
  const repositoryLinkId = branchDb.elements.queryElementIdByCode(
    RepositoryLink.createCode(
      masterDb,
      IModelDb.repositoryModelId,
      "example-code-value"
    )
  );
  let masterExternalSourceId;
  for await (const row of branchDb.createQueryReader(
    `SELECT ECInstanceId FROM ${ExternalSource.classFullName} WHERE Repository.Id=:id`,
    QueryBinder.from({ id: repositoryLinkId })
  )) {
    masterExternalSourceId = row.ECInstanceId;
  }
  const masterEditTxn = new EditTxn(masterDb, "synchronize branch changes");
  masterEditTxn.start();
  const branchEditTxn = new EditTxn(
    branchDb,
    "record synchronization provenance"
  );
  branchEditTxn.start();
  const reverseSynchronizer = new IModelTransformer(
    { source: branchDb, target: masterEditTxn },
    {
      // Identify the master's ExternalSource in the branch source for reverse synchronization.
      targetScopeElementId: masterExternalSourceId,
      // Pass an empty argsForProcessChanges object to process the next unsynchronized changes.
      argsForProcessChanges: {},
      // Record updated provenance in the branch source transaction.
      sourceEditTxn: branchEditTxn,
    }
  );

  await reverseSynchronizer.process();
  reverseSynchronizer.dispose();
  masterEditTxn.end("save");
  branchEditTxn.end("save");
  // Push the synchronized master changes and branch provenance.
  const description = "merged changes from branch into master";
  await masterDb.pushChanges({
    accessToken: myAccessToken,
    description,
  });
  await branchDb.pushChanges({
    accessToken: myAccessToken,
    description: `${description} provenance`,
  });
  // __PUBLISH_EXTRACT_END__
}

async function arbitraryEdit(
  db: BriefcaseDb,
  myAccessToken: AccessToken,
  description: string,
  editNumber: number
) {
  const editTxn = new EditTxn(db, description);
  editTxn.start();
  let editSucceeded = false;
  try {
    const spatialCategoryCode = SpatialCategory.createCode(
      db,
      IModel.dictionaryId,
      "SpatialCategory1"
    );
    const physicalModelCode = PhysicalPartition.createCode(
      db,
      IModel.rootSubjectId,
      "PhysicalModel1"
    );
    let spatialCategoryId =
      db.elements.queryElementIdByCode(spatialCategoryCode);
    let physicalModelId = db.elements.queryElementIdByCode(physicalModelCode);
    if (physicalModelId === undefined || spatialCategoryId === undefined) {
      spatialCategoryId = SpatialCategory.insert(
        editTxn,
        IModel.dictionaryId,
        "SpatialCategory1",
        new SubCategoryAppearance()
      );
      physicalModelId = PhysicalModel.insert(
        editTxn,
        IModel.rootSubjectId,
        "PhysicalModel1"
      );
    }
    const physicalObjectProps: PhysicalElementProps = {
      classFullName: PhysicalObject.classFullName,
      model: physicalModelId,
      category: spatialCategoryId,
      code: new Code({
        spec: IModelDb.rootSubjectId,
        scope: IModelDb.rootSubjectId,
        value: `${editNumber}`,
      }),
      userLabel: `${editNumber}`,
      geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
      placement: {
        origin: Point3d.create(editNumber, editNumber, 0),
        angles: YawPitchRollAngles.createDegrees(0, 0, 0),
      },
    };
    editTxn.insertElement(physicalObjectProps);
    editSucceeded = true;
  } finally {
    editTxn.end(editSucceeded ? "save" : "abandon");
  }
  await db.pushChanges({
    accessToken: myAccessToken,
    description,
  });
}

describe("IModelBranchingOperations", () => {
  const version0Path = path.join(
    TestUtils.KnownTestLocations.outputDir,
    "branching-ops.bim"
  );

  let restoreCheckpointDownload: (() => void) | undefined;

  beforeAll(() => {
    transformerTestHub.start(
      "IModelBranchingOperations",
      TestUtils.KnownTestLocations.outputDir
    );
    restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
    if (fs.existsSync(version0Path)) fs.unlinkSync(version0Path);
    SnapshotDb.createEmpty(version0Path, {
      rootSubject: { name: "branching-ops" },
    }).close();
  });

  afterAll(() => {
    restoreCheckpointDownload?.();
    transformerTestHub.stop();
  });

  it("run branching operations", async () => {
    const myAccessToken = await HubWrappers.getAccessToken(
      TestUtils.TestUserType.Regular
    );
    const myITwinId = transformerTestHub.iTwinId;
    const masterIModelId = await transformerTestHub.createNewIModel({
      accessToken: myAccessToken,
      iTwinId: myITwinId,
      iModelName: "my-branch-imodel",
      version0: version0Path,
      noLocks: true,
    });
    const { masterDb, branchDb } = await initializeBranch(
      myITwinId,
      masterIModelId,
      myAccessToken
    );
    // Use distinct numbers because the branch already contains the master's edit
    // after forward synchronization.
    await arbitraryEdit(masterDb, myAccessToken, "edit master", 0);
    await forwardSyncMasterToBranch(masterDb, branchDb, myAccessToken);
    await arbitraryEdit(branchDb, myAccessToken, "edit branch", 1);
    await reverseSyncBranchToMaster(branchDb, masterDb, myAccessToken);
    masterDb.close();
    branchDb.close();
  });
});
