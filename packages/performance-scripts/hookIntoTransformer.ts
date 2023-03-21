/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import * as path from "path";

import { IModelTransformer } from "@itwin/transformer";

export function hookIntoTransformer(hook: (t: IModelTransformer) => void) {
  const originalRegisterEvents = IModelTransformer.prototype._registerEvents;
  // we know this is called on construction, so we hook there
  IModelTransformer.prototype._registerEvents = function () {
    hook(this);
    return originalRegisterEvents.call(this);
  };
}

