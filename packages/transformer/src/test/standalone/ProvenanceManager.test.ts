/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  StandaloneDb,
  Subject,
  SubjectOwnsSubjects,
  withEditTxn,
} from "@itwin/core-backend";
import { Code, ExternalSourceAspectProps, IModel } from "@itwin/core-common";
import { Guid, Id64String } from "@itwin/core-bentley";
import { ProvenanceManager } from "../../ProvenanceManager";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

describe("ProvenanceManager tracked element mappings", () => {
  let sourceDb: StandaloneDb;
  let targetDb: StandaloneDb;

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
  });

  afterEach(() => {
    vi.restoreAllMocks();
    sourceDb.close();
    targetDb.close();
  });

  function createProvenanceProps(
    ownerId: Id64String,
    scopeId: Id64String,
    identifier: string
  ): ExternalSourceAspectProps {
    return {
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: ownerId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: scopeId },
      kind: ExternalSourceAspect.Kind.Element,
      identifier,
    };
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
    const contextOnlySourceId = "0x123";
    const contextOnlyTargetId = "0x456";
    const scopedSourceId = "0x789";
    const { expectedOwner, otherOwner } = withEditTxn(
      targetDb,
      "insert scoped provenance",
      (txn) => {
        const expected = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "expected owner"
        );
        const other = Subject.insert(txn, IModel.rootSubjectId, "other owner");
        const otherScope = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "other scope"
        );
        txn.insertAspect(
          createProvenanceProps(expected, IModel.rootSubjectId, scopedSourceId)
        );
        txn.insertAspect(
          createProvenanceProps(other, otherScope, scopedSourceId)
        );
        return { expectedOwner: expected, otherOwner: other };
      }
    );

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
    const sourceElementId = withEditTxn(
      sourceDb,
      "insert source subject",
      (txn) =>
        txn.insertElement({
          classFullName: Subject.classFullName,
          code: Code.createEmpty(),
          federationGuid,
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
          userLabel: "source",
        })
    );
    const { guidOwner, provenanceOwner } = withEditTxn(
      targetDb,
      "insert target subjects and provenance",
      (txn) => {
        const guid = txn.insertElement({
          classFullName: Subject.classFullName,
          code: Code.createEmpty(),
          federationGuid,
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
          userLabel: "guid owner",
        });
        const provenance = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "provenance owner"
        );
        txn.insertAspect(
          createProvenanceProps(
            provenance,
            IModel.rootSubjectId,
            sourceElementId
          )
        );
        return { guidOwner: guid, provenanceOwner: provenance };
      }
    );

    const mappings = new Map<Id64String, Id64String>();
    await loadMappings(mappings);

    expect(mappings.get(sourceElementId)).toBe(provenanceOwner);
    expect(mappings.get(sourceElementId)).not.toBe(guidOwner);
  });

  it("uses the earliest scoped provenance aspect for conflicting mappings", async () => {
    const earliestOwner = withEditTxn(
      targetDb,
      "insert conflicting provenance",
      (txn) => {
        const earliest = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "earliest owner"
        );
        const later = Subject.insert(txn, IModel.rootSubjectId, "later owner");
        txn.insertAspect(
          createProvenanceProps(earliest, IModel.rootSubjectId, "0x123")
        );
        txn.insertAspect(
          createProvenanceProps(later, IModel.rootSubjectId, "0x123")
        );
        return earliest;
      }
    );

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
