/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import { IModelTransformerError } from "../../IModelTransformerError";

describe("IModelTransformerError", () => {
  it("has stable wire values", () => {
    expect(IModelTransformerError).to.deep.equal({
      ChangedInstanceMetadataMissing: "changed-instance-metadata-missing",
      ChangesetIndexUnavailable: "changeset-index-unavailable",
      DanglingReference: "dangling-reference",
      DependencyMappingMissing: "dependency-mapping-missing",
      DependencyVersionMismatch: "dependency-version-mismatch",
      EditTxnNotActive: "edit-txn-not-active",
      ElementIdNotPreservable: "element-id-not-preservable",
      ElementIdRequired: "element-id-required",
      ExportChangesRequiresBriefcase: "export-changes-requires-briefcase",
      ExportHandlerNotRegistered: "export-handler-not-registered",
      GeographicCoordinateSystemUnavailable:
        "geographic-coordinate-system-unavailable",
      GeographicCoordinateSystemMismatch:
        "geographic-coordinate-system-mismatch",
      GeolocationUnavailable: "geolocation-unavailable",
      ImporterOptionMismatch: "importer-option-mismatch",
      InvalidCode: "invalid-code",
      InvalidEntityReference: "invalid-entity-reference",
      InvalidModelId: "invalid-model-id",
      InvalidSubCategory: "invalid-subcategory",
      NoChangesets: "no-changesets",
      ParentModelRequired: "parent-model-required",
      ProvenanceScopeConflict: "provenance-scope-conflict",
      ProvenanceSchemaUnsupported: "provenance-schema-unsupported",
      RelationshipClassNotFound: "relationship-class-not-found",
      RelationshipIdRequired: "relationship-id-required",
      RelationshipProvenanceNotFound: "relationship-provenance-not-found",
      RootSubjectNotProcessable: "root-subject-not-processable",
      SchemaLoadFailed: "schema-load-failed",
      SourceEditTxnRequired: "source-edit-txn-required",
      SynchronizationRangeInvalid: "synchronization-range-invalid",
      SynchronizationTypeNotDetermined: "synchronization-type-not-determined",
      SynchronizationVersionMissing: "synchronization-version-missing",
      TargetClassNotFound: "target-class-not-found",
    });
  });
});
