/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  StandaloneDb,
  Subject,
  SubjectOwnsSubjects,
} from "@itwin/core-backend";
import { Code, ExternalSourceAspectProps, IModel } from "@itwin/core-common";
import { Guid, Id64String } from "@itwin/core-bentley";
import { ProvenanceManager } from "../../ProvenanceManager";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

describe("ProvenanceManager tracked element mappings", () => {
  let sourceDb: StandaloneDb;
  let targetDb: StandaloneDb;
  let sourceTxn: EditTxn;
  let targetTxn: EditTxn;

  beforeEach(() => {
    sourceDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "ProvenanceManager",
        "source.bim"
      ),
      { rootSubject: { name: "source" }, enableTransactions: true }
    );
    targetDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "ProvenanceManager",
        "target.bim"
      ),
      { rootSubject: { name: "target" }, enableTransactions: true }
    );
    sourceTxn = new EditTxn(sourceDb, "tracked element mapping test");
    sourceTxn.start();
    targetTxn = new EditTxn(targetDb, "provenance query test");
    targetTxn.start();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    sourceTxn.end("abandon");
    targetTxn.end("abandon");
    sourceDb.close();
    targetDb.close();
  });

  function insertProvenance(
    ownerId: Id64String,
    scopeId: Id64String,
    identifier: string
  ): Id64String {
    const props: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: ownerId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: scopeId },
      kind: ExternalSourceAspect.Kind.Element,
      identifier,
    };
    return targetTxn.insertAspect(props);
  }

  async function loadMappings(
    mappings: Map<Id64String, Id64String>
  ): Promise<void> {
    await ProvenanceManager.forEachTrackedElement({
      provenanceSourceDb: sourceDb,
      provenanceDb: targetDb,
      targetScopeElementId: IModel.rootSubjectId,
      isReverseSynchronization: false,
      fn: (sourceId, targetId) => mappings.set(sourceId, targetId),
      skipPropagateChangesToRootElements: true,
    });
  }

  it("adds only current-scope provenance without removing context-only mappings", async () => {
    const expectedOwner = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "expected owner"
    );
    const otherOwner = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "other owner"
    );
    const otherScope = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "other scope"
    );
    const contextOnlySourceId = "0x123";
    const contextOnlyTargetId = "0x456";
    const scopedSourceId = "0x789";
    insertProvenance(expectedOwner, IModel.rootSubjectId, scopedSourceId);
    insertProvenance(otherOwner, otherScope, scopedSourceId);

    const mappings = new Map<Id64String, Id64String>([
      [contextOnlySourceId, contextOnlyTargetId],
    ]);
    await loadMappings(mappings);

    expect(mappings.get(contextOnlySourceId)).toBe(contextOnlyTargetId);
    expect(mappings.get(scopedSourceId)).toBe(expectedOwner);
    expect([...mappings.values()]).not.toContain(otherOwner);
    expect(mappings.has("0x999")).toBe(false);
  });

  it("maps matching federation GUIDs and lets scoped provenance override them", async () => {
    const federationGuid = Guid.createValue();
    const sourceElementId = sourceTxn.insertElement({
      classFullName: Subject.classFullName,
      code: Code.createEmpty(),
      federationGuid,
      model: IModel.repositoryModelId,
      parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
      userLabel: "source",
    });
    const guidOwner = targetTxn.insertElement({
      classFullName: Subject.classFullName,
      code: Code.createEmpty(),
      federationGuid,
      model: IModel.repositoryModelId,
      parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
      userLabel: "guid owner",
    });
    const provenanceOwner = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "provenance owner"
    );
    insertProvenance(provenanceOwner, IModel.rootSubjectId, sourceElementId);

    const mappings = new Map<Id64String, Id64String>();
    await loadMappings(mappings);

    expect(mappings.get(sourceElementId)).toBe(provenanceOwner);
    expect(mappings.get(sourceElementId)).not.toBe(guidOwner);
  });

  it("uses the earliest scoped provenance aspect for conflicting mappings", async () => {
    const earliestOwner = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "earliest owner"
    );
    const laterOwner = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "later owner"
    );
    insertProvenance(earliestOwner, IModel.rootSubjectId, "0x123");
    insertProvenance(laterOwner, IModel.rootSubjectId, "0x123");

    const mappings = new Map<Id64String, Id64String>();
    await loadMappings(mappings);

    expect(mappings.get("0x123")).toBe(earliestOwner);
  });

  it("propagates tracked-element query failures", async () => {
    const queryError = new Error("provenance query failed");
    vi.spyOn(targetDb, "createQueryReader").mockImplementation(() => {
      throw queryError;
    });

    await expect(loadMappings(new Map())).rejects.toBe(queryError);
  });
});
