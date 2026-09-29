/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn } from "@itwin/core-backend";
import { Id64String, ITwinError, Logger } from "@itwin/core-bentley";
import {
  ElementBulkDeleteBlockedError,
  IModelImporter,
} from "../../IModelImporter";
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
/** Returns false when references outside the requested trees block the deletion.
 * `editTxn` must be the transaction that `importer` was constructed with.
 */
async function deleteTargetElements(
  editTxn: EditTxn,
  importer: IModelImporter,
  elementIds: ReadonlySet<Id64String>
): Promise<boolean> {
  try {
    await importer.deleteElements(elementIds);
    return true;
  } catch (error) {
    if (
      ITwinError.isError<ElementBulkDeleteBlockedError>(
        error,
        IModelTransformerErrorScope,
        IModelTransformerError.ElementBulkDeleteBlocked
      )
    ) {
      // Nothing was deleted, so the transaction can continue.
      for (const [blockedId, referencingId] of error.blockedReferences)
        Logger.logWarning(
          "MyApp",
          `${blockedId} is still referenced by ${referencingId}`
        );
      return false;
    }
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
