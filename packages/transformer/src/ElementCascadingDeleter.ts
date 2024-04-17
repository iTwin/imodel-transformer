/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import {
  ElementTreeDeleter,
  ElementTreeWalkerScope,
  IModelDb,
} from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";

/** Deletes an element tree and code scope references starting with the specified top element. The top element is also deleted. Uses ElementCascadeDeleter.
 * @param iModel The iModel
 * @param topElement The parent of the sub-tree
 */
export function deleteElementTreeCascade(
  iModel: IModelDb,
  topElement: Id64String
): void {
  const del = new ElementCascadingDeleter(iModel);
  del.deleteNormalElements(topElement);
  del.deleteSpecialElements();
}

/** Deletes an entire element tree, including sub-models, child elements and code scope references.
 * Items are deleted in bottom-up order. Definitions and Subjects are deleted after normal elements.
 * Call deleteNormalElements on each tree. Then call deleteSpecialElements.
 */
export class ElementCascadingDeleter extends ElementTreeDeleter {
  protected shouldVisitCodeScopes(
    _elementId: Id64String,
    _scope: ElementTreeWalkerScope
  ) {
    return true;
  }

  /** The main tree-walking function */
  protected override processElementTree(
    element: Id64String,
    scope: ElementTreeWalkerScope
  ): void {
    if (this.shouldVisitCodeScopes(element, scope)) {
      this._processCodeScopes(element, scope);
    }
    super.processElementTree(element, scope);
  }
  /** Process code scope references */
  private _processCodeScopes(
    element: Id64String,
    scope: ElementTreeWalkerScope
  ) {
    const newScope = new ElementTreeWalkerScope(scope, element);
    this._iModel.withPreparedStatement(
      `
      SELECT ECInstanceId
      FROM bis.Element
      WHERE CodeScope.id=?
        AND Parent.id IS NULL
    `,
      (stmt) => {
        stmt.bindId(1, element);
        while (stmt.step() === DbResult.BE_SQLITE_ROW) {
          const elementId = stmt.getValue(0).getId();
          this.processElementTree(elementId, newScope);
        }
      }
    );
  }
}
