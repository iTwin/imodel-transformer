/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ExternalSourceAspect, SnapshotDb } from "@itwin/core-backend";
import { ITwinError } from "@itwin/core-bentley";
import { IModel } from "@itwin/core-common";
import { assert, expect } from "chai";
import * as sinon from "sinon";
import { IModelCloneContext } from "../../IModelCloneContext";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";
import { ProvenanceManager } from "../../ProvenanceManager";
import { SyncTypeResolver } from "../../SyncTypeResolver";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

import "./TransformerTestStartup";

describe("ProvenanceManager errors", () => {
  function createDb(name: string): SnapshotDb {
    return SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "ProvenanceManager",
        `${name}.bim`
      ),
      { rootSubject: { name } }
    );
  }

  function assertTransformerError(
    error: unknown,
    key: IModelTransformerError,
    message: string
  ): void {
    expect(ITwinError.isError(error, IModelTransformerErrorScope, key)).to.be
      .true;
    expect(error).to.have.property("message", message);
  }

  it("identifies a provenance database with an unsupported BisCore schema", async () => {
    const sourceDb = createDb("UnsupportedBisCore-source");
    const targetDb = createDb("UnsupportedBisCore-target");
    const containsClassStub = sinon.stub(targetDb, "containsClass");
    containsClassStub
      .withArgs(ExternalSourceAspect.classFullName)
      .returns(false);

    try {
      await ProvenanceManager.forEachTrackedElement({
        provenanceSourceDb: sourceDb,
        provenanceDb: targetDb,
        targetScopeElementId: IModel.rootSubjectId,
        isReverseSynchronization: false,
        fn: () => {},
        skipPropagateChangesToRootElements: false,
      });
      assert.fail("Expected forEachTrackedElement() to throw");
    } catch (error) {
      assertTransformerError(
        error,
        IModelTransformerError.ProvenanceSchemaUnsupported,
        "The BisCore schema version of the target database is too old"
      );
    } finally {
      containsClassStub.restore();
      targetDb.close();
      sourceDb.close();
    }
  });

  it("identifies a relationship class missing from the target", async () => {
    const sourceDb = createDb("MissingRelationshipClass-source");
    const targetDb = createDb("MissingRelationshipClass-target");
    const editTxn = createStartedEditTxn(targetDb);
    const context = new IModelCloneContext(sourceDb, targetDb);
    const syncTypeResolver = new SyncTypeResolver(
      context,
      IModel.rootSubjectId
    );
    const manager = new ProvenanceManager(
      IModel.rootSubjectId,
      {},
      syncTypeResolver,
      editTxn
    );

    try {
      await manager["_getRelClassId"](
        targetDb,
        "MissingSchema:MissingRelationship"
      );
      assert.fail("Expected relationship class lookup to throw");
    } catch (error) {
      assertTransformerError(
        error,
        IModelTransformerError.RelationshipClassNotFound,
        "Could not find class MissingSchema:MissingRelationship in the db"
      );
    } finally {
      context[Symbol.dispose]();
      editTxn.end("abandon");
      targetDb.close();
      sourceDb.close();
    }
  });

  it("identifies missing relationship provenance", async () => {
    const sourceDb = createDb("MissingRelationshipProvenance-source");
    const targetDb = createDb("MissingRelationshipProvenance-target");

    try {
      await ProvenanceManager.initRelationshipProvenanceOptions(
        "0x123",
        "0x456",
        {
          sourceDb,
          targetDb,
          isReverseSynchronization: false,
          targetScopeElementId: IModel.rootSubjectId,
          forceOldRelationshipProvenanceMethod: false,
        }
      );
      assert.fail("Expected relationship provenance lookup to throw");
    } catch (error) {
      assertTransformerError(
        error,
        IModelTransformerError.RelationshipProvenanceNotFound,
        "relationship provenance query returned no rows"
      );
    } finally {
      targetDb.close();
      sourceDb.close();
    }
  });
});
