/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import * as path from "path";

import { IModelTransformer, TransformerEvent } from "@itwin/transformer";

export type Hooks<ExtraArgs> = Record<string, (t: IModelTransformer, args?: ExtraArgs) => void>;

export async function hookIntoTransformerInstance<ExtraArgs>(
  t: IModelTransformer,
  hooks: Hooks<ExtraArgs>,
  args?: ExtraArgs,
): Promise<void> {
  hooks.processAll(t, args);
  hooks.processSchemas(t, args);
  hooks.processChanges(t, args);
}

export function hookIntoTransformer<ExtraArgs>(hooks: Hooks<ExtraArgs>) {
  const originalRegisterEvents = IModelTransformer.prototype._registerEvents;
  // we know this is called on construction, so we hook there
  IModelTransformer.prototype._registerEvents = function () {
    hookIntoTransformerInstance(this, hooks);
    return originalRegisterEvents.call(this);
  };
}

