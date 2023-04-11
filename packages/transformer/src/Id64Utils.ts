/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { assert, Id64, Id64Array, Id64Set, Id64String } from "@itwin/core-bentley";
import { RelatedElement } from "@itwin/core-common";

/** specific to DisplayStyle json properties */
interface SubCategoryOverrideContainer {
  subcategoryOvr: { subCategory: Id64String }[];
}

/** specific to DisplayStyle json properties */
interface ModelOverrideContainer {
  modelOvr: { modelId: Id64String }[];
}

/**
 * transformer-specific containers of Id64s
 */
export type Id64UtilsArg =
  | Id64Set
  | Id64Array
  | Id64String
  | { id: Id64String }
  | SubCategoryOverrideContainer
  | ModelOverrideContainer
  | undefined;

/**
 * transformer-specific extra utilities for dealing with Id64, not contained in the [Id64]($bentley)
 * namespace. Some may be considered for promotion to [Id64]($bentley).
 */
export namespace Id64Utils {
  const unsupportedErrMessage = (idContainer: any) =>
    `Id64 container '${idContainer}' of class '${idContainer.constructor.name}' is unsupported.\n`
    + "Currently only Id64 strings, prop-like objects containing an 'id' property, or sets and arrays "
    + "of Id64 strings are supported.";

  // NOTE: I wonder if the v8 optimizer inlines these and removes the redundant condition checks
  export const isId64String = (arg: any): arg is Id64String => {
    const isString = typeof arg === "string";
    assert(() => !isString || Id64.isValidId64(arg));
    return isString;
  };
  export const isSubCategoryOverrideContainer = (arg: any): arg is SubCategoryOverrideContainer =>
    arg && typeof arg === "object" && "subcategoryOvr" in arg;
  export const isModelOverrideContainer = (arg: any): arg is ModelOverrideContainer =>
    arg && typeof arg === "object" && "modelOvr" in arg;
  export const isRelatedElem = (arg: any): arg is RelatedElement =>
    arg && typeof arg === "object" && "id" in arg;
  export const isId64Array = (arg: any): arg is Id64Array => Array.isArray(arg);
  export const isId64Set = (arg: any): arg is Id64Set => arg && typeof arg === "object" && "id" in arg;

  /**
   * Apply a function to each Id64 in a supported container [[Id64UtilsArg]] type of Id64s.
   * @internal
   */
  export function forEach<T extends Id64UtilsArg>(
    idContainer: T,
    func: (id: Id64String) => void
  ): T {
    // NOTE: order is important if adding a "CompressedId64Set" object to the list of supported Id64 args
    if (idContainer === undefined) {
      // nothing
    } else if (isId64String(idContainer)) {
      func(idContainer);
    } else if (isRelatedElem(idContainer)) {
      func(idContainer.id);
    } else if (isId64Array(idContainer) || isId64Set(idContainer)) {
        for (const id of idContainer)
          func(id);
    } else {
      throw Error(unsupportedErrMessage(idContainer));
    }

    return idContainer;
  }

  /**
   * Clone an arbitrary Id64UtilsArg
   * @internal
   */
  export function clone<T extends Id64UtilsArg>(
    idContainer: T,
  ): T {
    if (idContainer === undefined || isId64String("string")) {
      return idContainer;
    } else if (isRelatedElem(idContainer)) {
      return { ...idContainer } as any as T; // wtf typescript
    } else if (isId64Array(idContainer)) {
      return [ ...idContainer ] as any as T; // wtf typescript
    } else if (isId64Set(idContainer)) {
      return new Set(idContainer) as T;
    } else {
      throw Error(unsupportedErrMessage(idContainer));
    }
  }

  /**
   * Apply a function to each Id64 in a supported container type of Id64s.
   * Currently only supports raw Id64String or RelatedElement-like objects containing an `id` property that is an Id64String,
   * which matches the possible containers of references in [Element.requiredReferenceKeys]($backend).
   * @internal
   */
  export function map<R>(
    idContainer: Id64UtilsArg,
    func: (id: Id64String) => R
  ): R[] {
    const results: R[] = [];
    Id64Utils.forEach(idContainer, (id) => results.push(func(id)));
    return results;
  }
}

