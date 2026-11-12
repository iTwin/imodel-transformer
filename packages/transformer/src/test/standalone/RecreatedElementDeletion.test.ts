/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect } from "vitest";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { AccessToken, GuidString } from "@itwin/core-bentley";
import { Code, IModel, PhysicalElementProps } from "@itwin/core-common";
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
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations.js";
import { transformerTestHub } from "../TestUtils/TransformerTestHub.js";

/** iTwin/imodel-transformer#494: an element recreated with the same federation GUID must not be deleted from the target. */
describe("Element recreated with the same federation guid", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let restoreCheckpointDownload: (() => void) | undefined;
  let mainDb: BriefcaseDb;
  let otherDb: BriefcaseDb | undefined;

  async function openBriefcaseWithoutLocks(name: string) {
    const iModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(name),
      noLocks: true,
    });
    return HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId,
    });
  }

  async function closeAndDeleteBriefcase(db: BriefcaseDb) {
    const iModelId = db.iModelId;
    await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, db);
    await transformerTestHub.deleteIModel({ iTwinId, iModelId });
  }

  function insertPhysicalObject(db: BriefcaseDb, userLabel: string) {
    return withEditTxn(db, `insert ${userLabel}`, (txn) => {
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "C1",
        {}
      );
      const modelId = PhysicalModel.insert(txn, IModel.rootSubjectId, "PM1");
      const elementId = txn.insertElement({
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        userLabel,
      } as PhysicalElementProps);
      return { modelId, categoryId, elementId };
    });
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
    forceExternalSourceAspectProvenance: boolean
  ): Promise<BriefcaseDb> {
    mainDb.performCheckpoint();
    const forkIModelId = await transformerTestHub.createNewIModel({
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName("Fork"),
      version0: mainDb.pathName,
      noLocks: true,
    });
    const forkDb = await HubWrappers.downloadAndOpenBriefcase({
      accessToken,
      iTwinId,
      iModelId: forkIModelId,
    });
    await transform(
      mainDb,
      forkDb,
      {
        wasSourceIModelCopiedToTarget: true,
        forceExternalSourceAspectProvenance,
      },
      "fork init"
    );
    return forkDb;
  }

  beforeAll(async () => {
    transformerTestHub.start(
      "RecreatedElementDeletion",
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
    mainDb = await openBriefcaseWithoutLocks("Main");
    otherDb = undefined;
  });

  afterEach(async () => {
    await closeAndDeleteBriefcase(mainDb);
    if (otherDb) await closeAndDeleteBriefcase(otherDb);
    restoreCheckpointDownload?.();
    restoreCheckpointDownload = undefined;
  });

  it("keeps the target element when the source element is deleted and recreated in separate changesets", async () => {
    const { modelId, categoryId, elementId } = insertPhysicalObject(
      mainDb,
      "Element1"
    );
    const fedGuid = mainDb.elements.getElement(elementId).federationGuid!;
    await mainDb.pushChanges({ description: "insert Element1" });

    const targetDb = await openBriefcaseWithoutLocks("Target");
    otherDb = targetDb;
    await transform(mainDb, targetDb, {}, "process all");
    expect(targetDb.elements.tryGetElement({ federationGuid: fedGuid })).to.not
      .be.undefined;

    withEditTxn(mainDb, "delete Element1", (txn) =>
      txn.deleteElement(elementId)
    );
    await mainDb.pushChanges({ description: "delete Element1" });
    withEditTxn(mainDb, "recreate Element1", (txn) =>
      txn.insertElement({
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        userLabel: "Element1_recreated",
        federationGuid: fedGuid,
      } as PhysicalElementProps)
    );
    await mainDb.pushChanges({ description: "recreate Element1" });

    await transform(
      mainDb,
      targetDb,
      { argsForProcessChanges: {} },
      "process changes"
    );

    const targetElement = targetDb.elements.tryGetElement({
      federationGuid: fedGuid,
    });
    expect(targetElement?.userLabel).to.equal("Element1_recreated");
    expect(
      IModelTestUtils.count(targetDb, PhysicalObject.classFullName)
    ).to.equal(1);
  });

  for (const forceExternalSourceAspectProvenance of [false, true]) {
    const provenanceKind = forceExternalSourceAspectProvenance
      ? "external source aspect"
      : "federation guid";

    it(`keeps the master element when forward sync recreated it in the fork and the fork then edited it (${provenanceKind} provenance)`, async () => {
      const { elementId } = insertPhysicalObject(mainDb, "Element1");
      const fedGuid = mainDb.elements.getElement(elementId).federationGuid!;
      await mainDb.pushChanges({ description: "insert Element1" });
      const forkDb = await createFork(forceExternalSourceAspectProvenance);
      otherDb = forkDb;

      const forkElementId = forkDb.elements.getElement({
        federationGuid: fedGuid,
      }).id;
      withEditTxn(forkDb, "delete Element1 in fork", (txn) =>
        txn.deleteElement(forkElementId)
      );
      await forkDb.pushChanges({ description: "delete Element1 in fork" });

      withEditTxn(mainDb, "update Element1 in main", (txn) => {
        const props = mainDb.elements.getElementProps(elementId);
        props.userLabel = "Element1_mainUpdated";
        txn.updateElement(props);
      });
      await mainDb.pushChanges({ description: "update Element1 in main" });

      await transform(
        mainDb,
        forkDb,
        { argsForProcessChanges: {}, forceExternalSourceAspectProvenance },
        "forward synchronization"
      );
      const forkElement = forkDb.elements.getElement({
        federationGuid: fedGuid,
      });
      expect(forkElement.id).to.not.equal(forkElementId);
      expect(forkElement.userLabel).to.equal("Element1_mainUpdated");

      withEditTxn(forkDb, "update Element1 in fork", (txn) => {
        forkElement.setUserProperties("testNamespace", {
          testProperty: "forkUpdatedValue",
        });
        txn.updateElement(forkElement.toJSON());
      });
      await forkDb.pushChanges({ description: "update Element1 in fork" });

      await transform(
        forkDb,
        mainDb,
        { argsForProcessChanges: {}, forceExternalSourceAspectProvenance },
        "reverse synchronization",
        true
      );

      const mergedElement = mainDb.elements.tryGetElement({
        federationGuid: fedGuid,
      });
      expect(mergedElement?.id).to.equal(elementId);
      expect(mergedElement?.userLabel).to.equal("Element1_mainUpdated");
      expect(
        mergedElement?.getUserProperties("testNamespace").testProperty
      ).to.equal("forkUpdatedValue");
    });

    it(`keeps the master element when forward sync recreated it in the fork and the fork did not edit it (${provenanceKind} provenance)`, async () => {
      const { elementId } = insertPhysicalObject(mainDb, "Element1");
      const fedGuid = mainDb.elements.getElement(elementId).federationGuid!;
      await mainDb.pushChanges({ description: "insert Element1" });
      const forkDb = await createFork(forceExternalSourceAspectProvenance);
      otherDb = forkDb;

      const forkElementId = forkDb.elements.getElement({
        federationGuid: fedGuid,
      }).id;
      withEditTxn(forkDb, "delete Element1 in fork", (txn) =>
        txn.deleteElement(forkElementId)
      );
      await forkDb.pushChanges({ description: "delete Element1 in fork" });

      withEditTxn(mainDb, "update Element1 in main", (txn) => {
        const props = mainDb.elements.getElementProps(elementId);
        props.userLabel = "Element1_mainUpdated";
        txn.updateElement(props);
      });
      await mainDb.pushChanges({ description: "update Element1 in main" });

      await transform(
        mainDb,
        forkDb,
        { argsForProcessChanges: {}, forceExternalSourceAspectProvenance },
        "forward synchronization"
      );
      expect(forkDb.elements.tryGetElement({ federationGuid: fedGuid })).to.not
        .be.undefined;

      await transform(
        forkDb,
        mainDb,
        { argsForProcessChanges: {}, forceExternalSourceAspectProvenance },
        "reverse synchronization",
        true
      );

      const mergedElement = mainDb.elements.tryGetElement({
        federationGuid: fedGuid,
      });
      expect(mergedElement?.id).to.equal(elementId);
      expect(mergedElement?.userLabel).to.equal("Element1_mainUpdated");
    });

    it(`deletes the master element when the fork deleted it and it was not recreated (${provenanceKind} provenance)`, async () => {
      const { elementId } = insertPhysicalObject(mainDb, "Element1");
      const fedGuid = mainDb.elements.getElement(elementId).federationGuid!;
      await mainDb.pushChanges({ description: "insert Element1" });
      const forkDb = await createFork(forceExternalSourceAspectProvenance);
      otherDb = forkDb;

      const forkElementId = forkDb.elements.getElement({
        federationGuid: fedGuid,
      }).id;
      withEditTxn(forkDb, "delete Element1 in fork", (txn) =>
        txn.deleteElement(forkElementId)
      );
      await forkDb.pushChanges({ description: "delete Element1 in fork" });

      await transform(
        forkDb,
        mainDb,
        { argsForProcessChanges: {}, forceExternalSourceAspectProvenance },
        "reverse synchronization",
        true
      );

      expect(mainDb.elements.tryGetElement({ federationGuid: fedGuid })).to.be
        .undefined;
    });
  }
});
