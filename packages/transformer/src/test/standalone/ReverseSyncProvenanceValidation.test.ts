/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect } from "vitest";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import {
  AccessToken,
  Guid,
  GuidString,
  Id64String,
  Logger,
} from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer.js";
import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import * as TestUtils from "../TestUtils/index.js";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

/** Reverse synchronization must not trust branch provenance that names a master element the branch element was not synchronized with. */
describe("Reverse synchronization provenance validation", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let restoreCheckpointDownload: (() => void) | undefined;
  let mainDb: BriefcaseDb;
  let forkDb: BriefcaseDb | undefined;
  let modelId: Id64String;
  let categoryId: Id64String;

  async function closeAndDeleteBriefcase(db: BriefcaseDb) {
    const iModelId = db.iModelId;
    await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, db);
    await transformerTestHub.deleteIModel({ iTwinId, iModelId });
  }

  function insertPhysicalObject(
    db: BriefcaseDb,
    userLabel: string,
    federationGuid?: GuidString
  ): Id64String {
    return withEditTxn(db, `insert ${userLabel}`, (txn) =>
      txn.insertElement({
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        userLabel,
        federationGuid,
      } as PhysicalElementProps)
    );
  }

  function insertElementProvenance(
    db: BriefcaseDb,
    ownerId: Id64String,
    identifier: Id64String
  ) {
    withEditTxn(db, "insert element provenance", (txn) =>
      txn.insertAspect({
        classFullName: ExternalSourceAspect.classFullName,
        element: {
          id: ownerId,
          relClassName: ElementOwnsExternalSourceAspects.classFullName,
        },
        scope: { id: IModel.rootSubjectId },
        kind: ExternalSourceAspect.Kind.Element,
        identifier,
      } as ExternalSourceAspectProps)
    );
  }

  async function transform(
    source: BriefcaseDb,
    target: BriefcaseDb,
    options: IModelTransformOptions,
    description: string,
    reverse = false
  ) {
    const targetEditTxn = createStartedEditTxn(target);
    const sourceEditTxn = reverse ? createStartedEditTxn(source) : undefined;
    const transformer = new IModelTransformer(
      { source, target: targetEditTxn },
      { ...options, sourceEditTxn }
    );
    try {
      await transformer.process();
    } finally {
      transformer.dispose();
      targetEditTxn.end();
      sourceEditTxn?.end();
    }
    await target.pushChanges({ description });
    if (sourceEditTxn) await source.pushChanges({ description });
  }

  async function createFork(
    forceExternalSourceAspectProvenance = false
  ): Promise<BriefcaseDb> {
    mainDb.performCheckpoint();
    const forkIModelId = await transformerTestHub.createNewIModel({
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName("Fork"),
      version0: mainDb.pathName,
      noLocks: true,
    });
    const db = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: forkIModelId,
    });
    await transform(
      mainDb,
      db,
      {
        wasSourceIModelCopiedToTarget: true,
        forceExternalSourceAspectProvenance,
      },
      "fork init"
    );
    return db;
  }

  async function reverseSynchronize(
    forceExternalSourceAspectProvenance = false
  ) {
    await transform(
      forkDb!,
      mainDb,
      { argsForProcessChanges: {}, forceExternalSourceAspectProvenance },
      "reverse synchronization",
      true
    );
  }

  beforeAll(async () => {
    transformerTestHub.start(
      "ReverseSyncProvenanceValidation",
      KnownTestLocations.outputDir
    );
    iTwinId = transformerTestHub.iTwinId;
    accessToken = await HubWrappers.getAccessToken(
      TestUtils.TestUserType.Regular
    );
  });

  afterAll(() => transformerTestHub.stop());

  beforeEach(async () => {
    restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
    const iModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName("Main"),
      noLocks: true,
    });
    mainDb = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId,
    });
    ({ modelId, categoryId } = withEditTxn(mainDb, "insert model", (txn) => ({
      modelId: PhysicalModel.insert(txn, IModel.rootSubjectId, "PM1"),
      categoryId: SpatialCategory.insert(txn, IModel.dictionaryId, "C1", {}),
    })));
    forkDb = undefined;
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await closeAndDeleteBriefcase(mainDb);
    if (forkDb) await closeAndDeleteBriefcase(forkDb);
    restoreCheckpointDownload?.();
    restoreCheckpointDownload = undefined;
  });

  /** Forks main with a synchronized element, then inserts a victim element into main that the fork never received. */
  async function setUpForkAndVictim() {
    const syncedMainId = insertPhysicalObject(mainDb, "Synced");
    const syncedGuid = mainDb.elements.getElement(syncedMainId).federationGuid!;
    await mainDb.pushChanges({ description: "insert Synced" });
    forkDb = await createFork();
    const syncedForkId = forkDb.elements.getElement({
      federationGuid: syncedGuid,
    }).id;

    const victimId = insertPhysicalObject(mainDb, "Victim");
    const victimGuid = mainDb.elements.getElement(victimId).federationGuid!;
    await mainDb.pushChanges({ description: "insert Victim" });
    return { syncedMainId, syncedGuid, syncedForkId, victimId, victimGuid };
  }

  it("does not delete a master element named by forged provenance on a deleted branch element", async () => {
    const { syncedMainId, syncedForkId, victimId } = await setUpForkAndVictim();
    const warn = vi.spyOn(Logger, "logWarning");

    insertElementProvenance(forkDb!, syncedForkId, victimId);
    await forkDb!.pushChanges({ description: "forge provenance" });
    withEditTxn(forkDb!, "delete Synced", (txn) =>
      txn.deleteElement(syncedForkId)
    );
    await forkDb!.pushChanges({ description: "delete Synced" });

    await reverseSynchronize();

    expect(mainDb.elements.tryGetElement(victimId)?.userLabel).to.equal(
      "Victim"
    );
    expect(mainDb.elements.tryGetElement(syncedMainId)).to.be.undefined;
    expect(
      warn.mock.calls.some(([, message]) =>
        String(message).includes(`to target element ${victimId}`)
      )
    ).to.be.true;
  });

  it("does not update a master element named by forged provenance on an updated branch element", async () => {
    const { syncedMainId, syncedForkId, victimId, victimGuid } =
      await setUpForkAndVictim();

    insertElementProvenance(forkDb!, syncedForkId, victimId);
    withEditTxn(forkDb!, "update Synced", (txn) => {
      const props = forkDb!.elements.getElementProps(syncedForkId);
      props.userLabel = "Forged";
      txn.updateElement(props);
    });
    await forkDb!.pushChanges({ description: "forge provenance and update" });

    await reverseSynchronize();

    const victim = mainDb.elements.getElement(victimId);
    expect(victim.userLabel).to.equal("Victim");
    expect(victim.federationGuid).to.equal(victimGuid);
    expect(mainDb.elements.getElement(syncedMainId).userLabel).to.equal(
      "Forged"
    );
  });

  it("does not update a master element named by forged provenance on a branch element without a federation guid", async () => {
    const { victimId, victimGuid } = await setUpForkAndVictim();

    const attackerId = insertPhysicalObject(forkDb!, "Attacker", Guid.empty);
    insertElementProvenance(forkDb!, attackerId, victimId);
    await forkDb!.pushChanges({ description: "insert Attacker" });

    await reverseSynchronize();

    const victim = mainDb.elements.getElement(victimId);
    expect(victim.userLabel).to.equal("Victim");
    expect(victim.federationGuid).to.equal(victimGuid);
    const attackerLabels: string[] = [];
    for await (const row of mainDb.createQueryReader(
      "SELECT UserLabel FROM bis.PhysicalElement WHERE UserLabel='Attacker'"
    ))
      attackerLabels.push(row[0]);
    expect(attackerLabels).to.have.length(1);
  });

  it("deletes a master element without a federation guid through its provenance", async () => {
    const noGuidMainId = insertPhysicalObject(mainDb, "NoGuid", Guid.empty);
    await mainDb.pushChanges({ description: "insert NoGuid" });
    forkDb = await createFork();
    expect(forkDb.elements.tryGetElement(noGuidMainId)?.userLabel).to.equal(
      "NoGuid"
    );

    withEditTxn(forkDb, "delete NoGuid", (txn) =>
      txn.deleteElement(noGuidMainId)
    );
    await forkDb.pushChanges({ description: "delete NoGuid" });

    await reverseSynchronize();

    expect(mainDb.elements.tryGetElement(noGuidMainId)).to.be.undefined;
  });

  it("deletes a master model through external source aspect provenance", async () => {
    const otherModelId = withEditTxn(mainDb, "insert PM2", (txn) =>
      PhysicalModel.insert(txn, IModel.rootSubjectId, "PM2")
    );
    const otherElementId = insertPhysicalObject(mainDb, "Kept");
    await mainDb.pushChanges({ description: "insert PM2" });
    forkDb = await createFork(true);
    const warn = vi.spyOn(Logger, "logWarning");

    withEditTxn(forkDb, "delete PM2", (txn) => {
      txn.deleteModel(otherModelId);
      txn.deleteElement(otherModelId);
    });
    withEditTxn(forkDb, "update Kept", (txn) => {
      const props = forkDb!.elements.getElementProps(otherElementId);
      props.userLabel = "Updated";
      txn.updateElement(props);
    });
    await forkDb.pushChanges({ description: "delete PM2" });

    await reverseSynchronize(true);

    expect(mainDb.models.tryGetModelProps(otherModelId)).to.be.undefined;
    expect(mainDb.elements.tryGetElement(otherModelId)).to.be.undefined;
    expect(mainDb.elements.getElement(otherElementId).userLabel).to.equal(
      "Updated"
    );
    expect(
      warn.mock.calls.some(([, message]) =>
        String(message).includes("Ignoring ExternalSourceAspect provenance")
      )
    ).to.be.false;
  });
});
