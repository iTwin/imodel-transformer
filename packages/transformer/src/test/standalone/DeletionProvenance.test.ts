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
  withEditTxn,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import {
  ChangesetFileProps,
  ExternalSourceAspectProps,
  IModel,
} from "@itwin/core-common";
import {
  ChangesetDeletionRecord,
  ChangesetDeletionRecordsByChangeset,
} from "../../ChangesetScanner";
import { IModelTransformer } from "../../IModelTransformer";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

describe("deletion provenance", () => {
  let sourceDb: StandaloneDb;
  let targetDb: StandaloneDb;
  let targetTxn: EditTxn;
  let transformer: IModelTransformer;

  beforeEach(() => {
    sourceDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "DeletionProvenance",
        "source.bim"
      ),
      { rootSubject: { name: "source" }, enableTransactions: true }
    );
    targetDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "DeletionProvenance",
        "target.bim"
      ),
      { rootSubject: { name: "target" }, enableTransactions: true }
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
    transformer?.dispose();
    targetTxn?.end("abandon");
    sourceDb.close();
    targetDb.close();
  });

  function createTransformer(): IModelTransformer {
    targetTxn = createStartedEditTxn(targetDb);
    transformer = new IModelTransformer({
      source: sourceDb,
      target: targetTxn,
    });
    return transformer;
  }

  function insertProvenance(
    txn: EditTxn,
    ownerId: Id64String,
    identifier: string
  ): Id64String {
    return txn.insertAspect({
      classFullName: ExternalSourceAspect.classFullName,
      element: new ElementOwnsExternalSourceAspects(ownerId),
      scope: { id: IModel.rootSubjectId },
      kind: ExternalSourceAspect.Kind.Element,
      identifier,
    } as ExternalSourceAspectProps);
  }

  it("does not process deleted aspects as elements and retains scoped owner metadata", async () => {
    const scopedAspectDeletion: ChangesetDeletionRecord = {
      ecInstanceId: "0x101",
      ecClassId: "0x201",
      classFullName: ExternalSourceAspect.classFullName,
      elementId: "0x301",
      scopeId: IModel.rootSubjectId,
      kind: ExternalSourceAspect.Kind.Element,
      identifier: "0x401",
    };
    const otherAspectDeletion: ChangesetDeletionRecord = {
      ecInstanceId: "0x102",
      ecClassId: "0x202",
      classFullName: "BisCore:ChannelRootAspect",
      elementId: "0x302",
    };
    const elementDeletion: ChangesetDeletionRecord = {
      ecInstanceId: "0x103",
      ecClassId: "0x203",
      classFullName: Subject.classFullName,
    };
    const deletionRecords: ChangesetDeletionRecordsByChangeset = [
      [scopedAspectDeletion, otherAspectDeletion, elementDeletion],
    ];

    const testTransformer = createTransformer();
    testTransformer["_csFileProps"] = [
      { pathname: "unused" } as ChangesetFileProps,
    ];
    testTransformer["_deletionRecordsByChangeset"] = deletionRecords;
    vi.spyOn(
      testTransformer["_provenanceManager"],
      "forEachTrackedElement"
    ).mockResolvedValue();
    const processDeletedOp = vi.fn(
      async (
        _change: ChangesetDeletionRecord,
        _scopeAspects: Map<string, ChangesetDeletionRecord>,
        _isRelationship: boolean,
        _elementInserts: Set<Id64String>,
        _modelInserts: Set<Id64String>
      ) => {}
    );
    Object.defineProperty(testTransformer, "processDeletedOp", {
      value: processDeletedOp,
    });

    await testTransformer["processChangesets"]();

    expect(processDeletedOp).toHaveBeenCalledTimes(1);
    expect(processDeletedOp.mock.calls[0][0]).toBe(elementDeletion);
    expect(processDeletedOp.mock.calls[0][1]).toEqual(
      new Map([[scopedAspectDeletion.elementId, scopedAspectDeletion]])
    );
    expect(processDeletedOp.mock.calls[0][2]).toBe(false);
  });

  it("selects the lowest ESA instance for ambiguous element provenance", async () => {
    const provenance = withEditTxn(
      targetDb,
      "insert ambiguous provenance",
      (txn) => {
        const lowerAspectOwner = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "lower aspect owner"
        );
        const higherAspectOwner = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "higher aspect owner"
        );
        const lowerAspectId = insertProvenance(
          txn,
          lowerAspectOwner,
          "ambiguous"
        );
        const higherAspectId = insertProvenance(
          txn,
          higherAspectOwner,
          "ambiguous"
        );
        return {
          lowerAspectOwner,
          higherAspectOwner,
          lowerAspectId,
          higherAspectId,
        };
      }
    );
    expect(BigInt(provenance.lowerAspectId)).toBeLessThan(
      BigInt(provenance.higherAspectId)
    );

    const result =
      await createTransformer()["_provenanceManager"].queryProvenanceForElement(
        "ambiguous"
      );

    expect(result).toBe(provenance.lowerAspectOwner);
    expect(result).not.toBe(provenance.higherAspectOwner);
  });
});
