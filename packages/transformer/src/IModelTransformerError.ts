/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/** Scope shared by errors originating from `@itwin/imodel-transformer`.
 * @beta
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
export const IModelTransformerErrorScope = "@itwin/imodel-transformer";

/** Errors originating from `@itwin/imodel-transformer`.
 * @beta
 */
export enum IModelTransformerError {
  /** An ElementAspect change requires its owning element identifier. */
  AspectOwnerRequired = "aspect-owner-required",
  /** Required changed-instance metadata is missing. */
  ChangedInstanceMetadataMissing = "changed-instance-metadata-missing",
  /** A required changeset index is unavailable. */
  ChangesetIndexUnavailable = "changeset-index-unavailable",
  /** A source entity contains a dangling reference rejected by transformer policy. */
  DanglingReference = "dangling-reference",
  /** A required dependency has no target mapping. */
  DependencyMappingMissing = "dependency-mapping-missing",
  /** The installed iTwin.js dependency version is incompatible. */
  DependencyVersionMismatch = "dependency-version-mismatch",
  /** A required edit transaction is not active. */
  EditTxnNotActive = "edit-txn-not-active",
  /** An element identifier cannot be preserved in the target. */
  ElementIdNotPreservable = "element-id-not-preservable",
  /** A required element identifier was not provided. */
  ElementIdRequired = "element-id-required",
  /** Exporting changes requires a briefcase iModel. */
  ExportChangesRequiresBriefcase = "export-changes-requires-briefcase",
  /** No export handler has been registered. */
  ExportHandlerNotRegistered = "export-handler-not-registered",
  /** A required geographic coordinate system is unavailable. */
  GeographicCoordinateSystemUnavailable = "geographic-coordinate-system-unavailable",
  /** Source and target geographic coordinate systems are incompatible. */
  GeographicCoordinateSystemMismatch = "geographic-coordinate-system-mismatch",
  /** Required geolocation data or an invertible geolocation transform is unavailable. */
  GeolocationUnavailable = "geolocation-unavailable",
  /** A custom importer option conflicts with the transformer option. */
  ImporterOptionMismatch = "importer-option-mismatch",
  /** A code is invalid or incomplete. */
  InvalidCode = "invalid-code",
  /** An entity reference has an unsupported representation. */
  InvalidEntityReference = "invalid-entity-reference",
  /** A model identifier is invalid or missing. */
  InvalidModelId = "invalid-model-id",
  /** A subcategory is invalid or incomplete. */
  InvalidSubCategory = "invalid-subcategory",
  /** The source iModel has no changesets or custom changes. */
  NoChangesets = "no-changesets",
  /** A required parent model was not provided. */
  ParentModelRequired = "parent-model-required",
  /** A provenance scope conflicts with existing provenance. */
  ProvenanceScopeConflict = "provenance-scope-conflict",
  /** The BisCore schema does not support required provenance data. */
  ProvenanceSchemaUnsupported = "provenance-schema-unsupported",
  /** A required relationship class is absent. */
  RelationshipClassNotFound = "relationship-class-not-found",
  /** A required relationship identifier was not provided. */
  RelationshipIdRequired = "relationship-id-required",
  /** Expected relationship provenance is absent. */
  RelationshipProvenanceNotFound = "relationship-provenance-not-found",
  /** The root subject cannot be processed directly. */
  RootSubjectNotProcessable = "root-subject-not-processable",
  /** A required schema could not be loaded. */
  SchemaLoadFailed = "schema-load-failed",
  /** Reverse synchronization requires a source edit transaction. */
  SourceEditTxnRequired = "source-edit-txn-required",
  /** The requested synchronization range is invalid. */
  SynchronizationRangeInvalid = "synchronization-range-invalid",
  /** The synchronization direction cannot be determined. */
  SynchronizationTypeNotDetermined = "synchronization-type-not-determined",
  /** The previous synchronization version is missing. */
  SynchronizationVersionMissing = "synchronization-version-missing",
  /** A required class is absent from the target iModel. */
  TargetClassNotFound = "target-class-not-found",
}
