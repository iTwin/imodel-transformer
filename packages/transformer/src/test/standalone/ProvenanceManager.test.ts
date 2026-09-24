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
} from "@itwin/core-backend";
import { ExternalSourceAspectProps, IModel } from "@itwin/core-common";
import { Id64, Id64String } from "@itwin/core-bentley";
import { ProvenanceManager } from "../../ProvenanceManager";
import { SyncTypeResolver } from "../../SyncTypeResolver";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

describe("ProvenanceManager element provenance queries", () => {
  let sourceDb: StandaloneDb;
  let targetDb: StandaloneDb;
  let targetTxn: EditTxn;
  let manager: ProvenanceManager;

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
    targetTxn = new EditTxn(targetDb, "provenance query test");
    targetTxn.start();
    manager = new ProvenanceManager(
      IModel.rootSubjectId,
      {},
      {
        sourceDb,
        targetDb,
        findTargetElementId: () => Id64.invalid,
      },
      new SyncTypeResolver(
        sourceDb,
        targetDb,
        IModel.rootSubjectId,
        false,
        false
      ),
      targetTxn
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
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

  it("deduplicates identifiers and bounds queries across batch boundaries", async () => {
    const querySpy = vi.spyOn(targetDb, "createQueryReader");

    await expect(manager.queryProvenanceForElements([])).resolves.toEqual(
      new Map()
    );
    expect(querySpy).not.toHaveBeenCalled();

    const firstBatch = Array.from({ length: 500 }, (_, index) => `${index}`);
    await manager.queryProvenanceForElements([
      ...firstBatch,
      firstBatch[0],
      firstBatch[499],
    ]);
    expect(querySpy).toHaveBeenCalledTimes(1);

    querySpy.mockClear();
    await manager.queryProvenanceForElements([...firstBatch, "overflow"]);
    expect(querySpy).toHaveBeenCalledTimes(2);
  });

  it("isolates scope, omits missing IDs, and accepts arbitrary string identifiers", async () => {
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
    const stringIdentifier = "connector-key' OR 1=1 --";
    insertProvenance(expectedOwner, IModel.rootSubjectId, "scoped");
    insertProvenance(otherOwner, otherScope, "scoped");
    insertProvenance(expectedOwner, IModel.rootSubjectId, stringIdentifier);

    const mappings = await manager.queryProvenanceForElements([
      "scoped",
      "missing",
      stringIdentifier,
    ]);

    expect(mappings).toEqual(
      new Map([
        ["scoped", expectedOwner],
        [stringIdentifier, expectedOwner],
      ])
    );
    expect(mappings.has("missing")).toBe(false);
  });

  it("returns the earliest ESA consistently for ambiguous mappings", async () => {
    const firstOwner = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "first owner"
    );
    const secondOwner = Subject.insert(
      targetTxn,
      IModel.rootSubjectId,
      "second owner"
    );
    insertProvenance(secondOwner, IModel.rootSubjectId, "ambiguous");
    insertProvenance(firstOwner, IModel.rootSubjectId, "ambiguous");

    const singularResult = await manager.queryProvenanceForElement("ambiguous");
    const bulkResult = await manager.queryProvenanceForElements(["ambiguous"]);

    expect(singularResult).toBe(secondOwner);
    expect(bulkResult.get("ambiguous")).toBe(singularResult);
  });

  it("propagates provenance query failures", async () => {
    const queryError = new Error("provenance query failed");
    vi.spyOn(targetDb, "createQueryReader").mockImplementation(() => {
      throw queryError;
    });

    await expect(
      manager.queryProvenanceForElements(["source-id"])
    ).rejects.toBe(queryError);
  });
});
