/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import type { ITwinErrorId } from "@itwin/core-bentley";

/** Identifiers for errors originating from `@itwin/imodel-transformer`.
 * @public
 */
export namespace IModelTransformerError {
  /** The scope shared by all errors originating from this package. */
  export const scope = "@itwin/imodel-transformer";

  /** Stable keys identifying errors originating from this package. */
  export const key = {
    noChangesets: "no-changesets",
  } as const;

  /** A stable key identifying an error originating from this package. */
  export type Key = (typeof key)[keyof typeof key];

  /** The strongly typed identifier of an error originating from this package. */
  export interface Id extends ITwinErrorId {
    /** The scope shared by all errors originating from this package. */
    readonly scope: typeof scope;
    /** The key identifying a specific error within this package. */
    readonly key: Key;
  }

  /** Complete identifiers for errors originating from this package. */
  export const id = {
    noChangesets: { scope, key: key.noChangesets },
  } as const satisfies Record<keyof typeof key, Id>;
}
