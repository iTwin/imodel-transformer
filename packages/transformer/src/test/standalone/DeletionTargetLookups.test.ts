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
  withEditTxn,
} from "@itwin/core-backend";
import { Code, ExternalSourceAspectProps, IModel } from "@itwin/core-common";
import { Guid, GuidString, Id64String } from "@itwin/core-bentley";
import {
  DeletionTargetResolver,
  queryFederationGuidsByElementId,
  queryScopeEsaIdentifiersByElementId,
} from "../../DeletionTargetLookups.js";
import type {
  DeletionBatch,
  ExternalSourceAspectDeletionRecord,
} from "../../ChangesetScanner.js";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils.js";

describe("DeletionTargetLookups", () => {
  let sourceDb: StandaloneDb;
  let targetDb: StandaloneDb;

  beforeEach(() => {
    sourceDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "DeletionTargetLookups",
        "source.bim"
      ),
      { rootSubject: { name: "source" }, enableTransactions: true }
    );
    targetDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "DeletionTargetLookups",
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

  function insertSubject(
    txn: EditTxn,
    label: string,
    federationGuid: GuidString = Guid.empty
  ): Id64String {
    return txn.insertElement({
      classFullName: Subject.classFullName,
      code: Code.createEmpty(),
      federationGuid,
      model: IModel.repositoryModelId,
      parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
      userLabel: label,
    });
  }

  function insertEsa(
    txn: EditTxn,
    ownerId: Id64String,
    scopeId: Id64String,
    identifier: string,
    kind: string = ExternalSourceAspect.Kind.Element
  ): void {
    const props: ExternalSourceAspectProps = {
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: ownerId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: scopeId },
      kind,
      identifier,
    };
    txn.insertAspect(props);
  }

  function deletedEsa(
    elementId: Id64String,
    identifier: string
  ): ExternalSourceAspectDeletionRecord {
    return {
      ecInstanceId: "0x1ffff",
      ecClassId: "0x1",
      elementId,
      scopeId: IModel.rootSubjectId,
      kind: ExternalSourceAspect.Kind.Element,
      identifier,
    };
  }

  function batch(args: Partial<DeletionBatch>): DeletionBatch {
    return {
      elements: [],
      models: [],
      relationships: [],
      externalSourceAspects: [],
      ...args,
    };
  }

  describe("set queries", () => {
    it("return empty maps without querying when given no valid IDs", async () => {
      const querySpy = vi.spyOn(sourceDb, "createQueryReader");
      expect(
        await queryFederationGuidsByElementId(sourceDb, ["0", "bad"])
      ).toEqual(new Map());
      expect(
        await queryScopeEsaIdentifiersByElementId(
          sourceDb,
          IModel.rootSubjectId,
          []
        )
      ).toEqual(new Map());
      expect(querySpy).not.toHaveBeenCalled();
    });
  });

  describe("queryFederationGuidsByElementId", () => {
    it("returns the federation GUIDs of existing elements that have one", async () => {
      const guid = Guid.createValue();
      const { withGuid, withoutGuid } = withEditTxn(
        sourceDb,
        "insert subjects",
        (txn) => ({
          withGuid: insertSubject(txn, "with guid", guid),
          withoutGuid: insertSubject(txn, "without guid"),
        })
      );

      const result = await queryFederationGuidsByElementId(sourceDb, [
        withGuid,
        withGuid,
        withoutGuid,
        "0xfffff",
        "0",
        "not an id",
      ]);

      expect(result).toEqual(new Map([[withGuid, guid]]));
    });
  });

  describe("queryScopeEsaIdentifiersByElementId", () => {
    it("reads the earliest element-kind aspect in the scope for each element", async () => {
      const { first, second, otherScopeOnly, relationshipKindOnly } =
        withEditTxn(sourceDb, "insert provenance", (txn) => {
          const otherScope = insertSubject(txn, "other scope");
          const elements = {
            first: insertSubject(txn, "first"),
            second: insertSubject(txn, "second"),
            otherScopeOnly: insertSubject(txn, "other scope only"),
            relationshipKindOnly: insertSubject(txn, "relationship kind"),
          };
          insertEsa(txn, elements.first, IModel.rootSubjectId, "0x101");
          insertEsa(txn, elements.first, IModel.rootSubjectId, "0x102");
          insertEsa(txn, elements.second, otherScope, "0x201");
          insertEsa(txn, elements.second, IModel.rootSubjectId, "0x202");
          insertEsa(txn, elements.otherScopeOnly, otherScope, "0x301");
          insertEsa(
            txn,
            elements.relationshipKindOnly,
            IModel.rootSubjectId,
            "0x401",
            ExternalSourceAspect.Kind.Relationship
          );
          return elements;
        });

      const result = await queryScopeEsaIdentifiersByElementId(
        sourceDb,
        IModel.rootSubjectId,
        [first, second, otherScopeOnly, relationshipKindOnly, "0xfffff"]
      );

      expect(result).toEqual(
        new Map([
          [first, "0x101"],
          [second, "0x202"],
        ])
      );
    });
  });

  describe("DeletionTargetResolver", () => {
    it("prefers a live aspect, then a deleted aspect, then the federation GUID", async () => {
      const liveEsaGuid = Guid.createValue();
      const deletedEsaGuid = Guid.createValue();
      const guidOnly = Guid.createValue();
      const emptyIdentifierGuid = Guid.createValue();
      const targets = withEditTxn(targetDb, "insert targets", (txn) => ({
        liveEsaGuid: insertSubject(txn, "live esa guid", liveEsaGuid),
        deletedEsaGuid: insertSubject(txn, "deleted esa guid", deletedEsaGuid),
        guidOnly: insertSubject(txn, "guid only", guidOnly),
        emptyIdentifierGuid: insertSubject(
          txn,
          "empty identifier guid",
          emptyIdentifierGuid
        ),
      }));
      const liveEsaElement = withEditTxn(sourceDb, "insert esa", (txn) => {
        const id = insertSubject(txn, "live esa", liveEsaGuid);
        insertEsa(txn, id, IModel.rootSubjectId, "0x9001");
        return id;
      });
      const deletedElementIds = {
        deletedEsa: "0x20000000001",
        guidOnly: "0x20000000002",
        emptyIdentifier: "0x20000000003",
      };
      const deletedScopeEsaByElementId = new Map([
        [liveEsaElement, deletedEsa(liveEsaElement, "0x9002")],
        [
          deletedElementIds.deletedEsa,
          deletedEsa(deletedElementIds.deletedEsa, "0x9003"),
        ],
        [
          deletedElementIds.emptyIdentifier,
          deletedEsa(deletedElementIds.emptyIdentifier, ""),
        ],
      ]);
      const elements = [
        {
          ecInstanceId: deletedElementIds.deletedEsa,
          federationGuid: deletedEsaGuid,
        },
        {
          ecInstanceId: deletedElementIds.guidOnly,
          federationGuid: guidOnly.toUpperCase(),
        },
        {
          ecInstanceId: deletedElementIds.emptyIdentifier,
          federationGuid: emptyIdentifierGuid,
        },
      ];
      const create = async (sourceHoldsProvenance: boolean) =>
        DeletionTargetResolver.create({
          sourceDb,
          targetDb,
          targetScopeElementId: IModel.rootSubjectId,
          sourceHoldsProvenance,
          batch: batch({
            elements: elements as DeletionBatch["elements"],
            models: [
              { ecInstanceId: liveEsaElement },
            ] as DeletionBatch["models"],
          }),
          deletedScopeEsaByElementId,
          deletedFederationGuidByElementId: new Map(
            elements.map((e) => [e.ecInstanceId, e.federationGuid])
          ),
        });

      const withProvenance = await create(true);
      expect(
        withProvenance.findTargetElementId(liveEsaElement, undefined)
      ).toBe("0x9001");
      expect(
        withProvenance.findTargetElementId(
          deletedElementIds.deletedEsa,
          deletedEsaGuid
        )
      ).toBe("0x9003");
      expect(
        withProvenance.findTargetElementId(
          deletedElementIds.guidOnly,
          guidOnly.toUpperCase()
        )
      ).toBe(targets.guidOnly);
      expect(
        withProvenance.findTargetElementId(
          deletedElementIds.emptyIdentifier,
          emptyIdentifierGuid
        )
      ).toBe(targets.emptyIdentifierGuid);

      const withoutProvenance = await create(false);
      expect(
        withoutProvenance.findTargetElementId(
          deletedElementIds.deletedEsa,
          deletedEsaGuid
        )
      ).toBe(targets.deletedEsaGuid);
      expect(
        withoutProvenance.findTargetElementId(
          deletedElementIds.guidOnly,
          guidOnly
        )
      ).toBe(targets.guidOnly);
      expect(
        withoutProvenance.findTargetElementId(liveEsaElement, undefined)
      ).toBeUndefined();
    });

    it("resolves relationship endpoints that survive or were deleted in the batch", async () => {
      const liveGuid = Guid.createValue();
      const deletedGuid = Guid.createValue();
      const targets = withEditTxn(targetDb, "insert targets", (txn) => ({
        live: insertSubject(txn, "live", liveGuid),
        deleted: insertSubject(txn, "deleted", deletedGuid),
      }));
      const liveEndpoint = withEditTxn(sourceDb, "insert endpoint", (txn) =>
        insertSubject(txn, "live endpoint", liveGuid)
      );
      const deletedEndpoint = "0x20000000001";
      const missingEndpoint = "0x20000000002";
      const querySpy = vi.spyOn(sourceDb, "createQueryReader");

      const resolver = await DeletionTargetResolver.create({
        sourceDb,
        targetDb,
        targetScopeElementId: IModel.rootSubjectId,
        sourceHoldsProvenance: false,
        batch: batch({
          relationships: [
            {
              sourceECInstanceId: liveEndpoint,
              targetECInstanceId: deletedEndpoint,
            },
            {
              sourceECInstanceId: liveEndpoint,
              targetECInstanceId: missingEndpoint,
            },
          ] as DeletionBatch["relationships"],
        }),
        deletedScopeEsaByElementId: new Map(),
        deletedFederationGuidByElementId: new Map([
          [deletedEndpoint, deletedGuid],
        ]),
      });

      expect(querySpy).toHaveBeenCalledTimes(1);
      expect(resolver.endpointFederationGuid(liveEndpoint)).toBe(liveGuid);
      expect(resolver.endpointFederationGuid(deletedEndpoint)).toBe(
        deletedGuid
      );
      expect(resolver.endpointFederationGuid(missingEndpoint)).toBeUndefined();
      expect(
        resolver.findTargetElementId(
          liveEndpoint,
          resolver.endpointFederationGuid(liveEndpoint)
        )
      ).toBe(targets.live);
      expect(
        resolver.findTargetElementId(
          deletedEndpoint,
          resolver.endpointFederationGuid(deletedEndpoint)
        )
      ).toBe(targets.deleted);
      expect(
        resolver.findTargetElementId(missingEndpoint, undefined)
      ).toBeUndefined();
    });
  });
});
