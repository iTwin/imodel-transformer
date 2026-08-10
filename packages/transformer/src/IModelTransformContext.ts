/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import type { Id64String } from "@itwin/core-bentley";
import type { EntityReference } from "@itwin/core-common";

/** The supported transformation context exposed by an [[IModelTransformer]].
 *
 * Use [[IModelTransformer.context]] to obtain this context. It records source-to-target mappings,
 * accepts explicit mapping and geometry-filtering rules, and resolves transformed entities.
 * Element cloning, database access, native resource management, and context persistence remain
 * implementation details of the transformer.
 * @beta
 */
export interface IModelTransformContext {
  /** Whether this context maps entities between two different iModels. */
  readonly isBetweenIModels: boolean;

  /** Find the target element corresponding to a source element.
   * @returns The target element ID, or [Id64.invalid]($bentley) if no mapping exists.
   */
  findTargetElementId(sourceElementId: Id64String): Id64String;

  /** Find the target element aspect corresponding to a source element aspect.
   * @returns The target aspect ID, or [Id64.invalid]($bentley) if no mapping exists.
   */
  findTargetAspectId(sourceAspectId: Id64String): Id64String;

  /** Find the target CodeSpec corresponding to a source CodeSpec.
   * @returns The target CodeSpec ID, or [Id64.invalid]($bentley) if no mapping exists.
   */
  findTargetCodeSpecId(sourceCodeSpecId: Id64String): Id64String;

  /** Find the target entity corresponding to a typed source entity reference.
   * @returns The typed target entity reference, containing [Id64.invalid]($bentley) if no mapping exists.
   */
  findTargetEntityId(sourceEntityId: EntityReference): Promise<EntityReference>;

  /** Add or replace a mapping from a source element to a target element. */
  remapElement(sourceElementId: Id64String, targetElementId: Id64String): void;

  /** Remove the mapping for a source element. */
  removeElement(sourceElementId: Id64String): void;

  /** Add or replace a mapping from a source element aspect to a target element aspect. */
  remapElementAspect(
    sourceAspectId: Id64String,
    targetAspectId: Id64String
  ): void;

  /** Remove the mapping for a source element aspect. */
  removeElementAspect(sourceAspectId: Id64String): void;

  /** Add or replace a mapping from a source element class to a target element class. */
  remapElementClass(
    sourceClassFullName: string,
    targetClassFullName: string
  ): void;

  /** Add or replace a mapping from a source CodeSpec to a target CodeSpec. */
  remapCodeSpec(sourceCodeSpecName: string, targetCodeSpecName: string): void;

  /** Exclude geometry belonging to a source SubCategory from cloned geometry streams.
   * Requests to filter a default SubCategory are ignored.
   */
  filterSubCategory(sourceSubCategoryId: Id64String): void;

  /** Whether any SubCategories are currently filtered. */
  readonly hasSubCategoryFilter: boolean;

  /** Determine whether a source SubCategory is currently filtered. */
  isSubCategoryFiltered(sourceSubCategoryId: Id64String): boolean;
}
