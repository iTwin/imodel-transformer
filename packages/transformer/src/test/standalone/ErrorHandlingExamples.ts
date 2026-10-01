/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn } from "@itwin/core-backend";
import { Id64String, ITwinError } from "@itwin/core-bentley";
import { IModelImporter } from "../../IModelImporter";
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

// __PUBLISH_EXTRACT_START__ ErrorHandling.handle-bulk-delete-errors
/** `editTxn` must be the transaction that `importer` was constructed with. */
async function deleteTargetElements(
  editTxn: EditTxn,
  importer: IModelImporter,
  elementIds: ReadonlySet<Id64String>
): Promise<void> {
  try {
    await importer.deleteElements(elementIds);
  } catch (error) {
    if (
      ITwinError.isError(
        error,
        IModelTransformerErrorScope,
        IModelTransformerError.ElementBulkDeleteFailed
      )
    ) {
      // Deletions from earlier native calls are still pending. Discard them before retrying.
      editTxn.end("abandon");
    }
    throw error;
  }
}
// __PUBLISH_EXTRACT_END__

// This file is compiled to verify the extracted documentation examples.
void processWithErrorHandling;
void deleteTargetElements;
