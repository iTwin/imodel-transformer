/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ITwinError } from "@itwin/core-bentley";
import { IModelTransformer } from "../../IModelTransformer";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";

// __PUBLISH_EXTRACT_START__ ErrorHandling.handle-identified-error
async function processWithErrorHandling(
  transformer: IModelTransformer
): Promise<void> {
  try {
    await transformer.process();
  } catch (error) {
    if (
      ITwinError.isError(
        error,
        IModelTransformerErrorScope,
        IModelTransformerError.DanglingReference
      )
    ) {
      // Correct the source reference or choose a different policy before retrying.
      return;
    }

    throw error;
  }
}
// __PUBLISH_EXTRACT_END__

// This file is compiled to verify the extracted documentation example.
void processWithErrorHandling;
