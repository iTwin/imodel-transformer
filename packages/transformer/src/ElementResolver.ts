/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { Id64, Id64String } from "@itwin/core-bentley";
import { Element, IModelDb } from "@itwin/core-backend";
import { Code, CodeProps, ElementProps, QueryBinder } from "@itwin/core-common";
import type { IModelCloneContext } from "./IModelCloneContext";

/** Result of resolving a target element ID for a source element. */
export interface ElementResolutionResult {
  targetElementId: Id64String;
  /** True if the code was cleared due to a class mismatch during code-based resolution. */
  codeCleared: boolean;
}

/**
 * Resolves target element IDs for source elements using a multi-strategy fallback chain:
 * 1. Existing remap (context lookup)
 * 2. FederationGuid match in target
 * 3. Code value match in target
 * 4. Not found (new element — caller handles insert)
 * @internal
 */
export class ElementResolver {
  private readonly _context: IModelCloneContext;

  public constructor(context: IModelCloneContext) {
    this._context = context;
  }

  /**
   * Resolve the target element ID for a source element using the fallback chain.
   * @param sourceElement Source element info (id and federationGuid).
   * @param targetElementProps The transformed target element props. The code value
   *   may be normalized (undefined → "") during resolution.
   * @returns Resolution result with the targetElementId and whether the code was cleared.
   */
  public async resolveTargetElementId(
    sourceElement: Pick<Element, "id" | "federationGuid">,
    targetElementProps: ElementProps
  ): Promise<ElementResolutionResult> {
    let targetElementId = this._context.findTargetElementId(sourceElement.id);
    let codeCleared = false;

    // Strategy 2: check by FederationGuid
    if (
      this._context.isBetweenIModels &&
      !Id64.isValid(targetElementId) &&
      sourceElement.federationGuid !== undefined
    ) {
      targetElementId =
        this._context.targetDb.elements.getIdFromFederationGuid(
          sourceElement.federationGuid
        ) ?? Id64.invalid;
      if (Id64.isValid(targetElementId))
        this._context.remapElement(sourceElement.id, targetElementId);
    }

    // Strategy 3: check by Code (only if CodeScope is valid — invalid means a missing reference)
    if (
      !Id64.isValidId64(targetElementId) &&
      Id64.isValidId64(targetElementProps.code.scope)
    ) {
      // respond the same way to undefined code value as the Code class, but don't use that class
      // because it trims whitespace from the value, and there are iModels with untrimmed whitespace
      targetElementProps.code.value = targetElementProps.code.value ?? "";
      const maybeTargetElementId = await this.queryElementIdByCode(
        this._context.targetDb,
        targetElementProps.code as Required<CodeProps>
      );
      if (undefined !== maybeTargetElementId) {
        const maybeTargetElem =
          this._context.targetDb.elements.getElement(maybeTargetElementId);
        if (
          maybeTargetElem.classFullName === targetElementProps.classFullName
        ) {
          // ensure code remapping doesn't change the target class
          targetElementId = maybeTargetElementId;
          this._context.remapElement(sourceElement.id, targetElementId);
        } else {
          targetElementProps.code = Code.createEmpty(); // clear out invalid code
          codeCleared = true;
        }
      }
    }

    return { targetElementId, codeCleared };
  }

  // In iTwin js 5.x Elements.queryElementIdByCode() uses Code class to query id:
  // https://github.com/iTwin/itwinjs-core/blob/master/core/backend/src/IModelDb.ts#L2779
  // Code class constructor trims white spaces from code value.
  // Custom implementation of queryElementIdByCode() was added to support querying elements with
  // code values that have trailing whitespaces.
  // It mimicks 4.x implementation: https://github.com/iTwin/itwinjs-core/blob/9c8b394ec3878a39764be81f928fd8b0b9115d31/core/backend/src/IModelDb.ts#L1882
  private async queryElementIdByCode(
    iModel: IModelDb,
    code: Required<CodeProps>
  ): Promise<Id64String | undefined> {
    if (Id64.isInvalid(code.spec)) throw new Error("Invalid CodeSpec");

    if (code.value === undefined) throw new Error("Invalid Code");

    const query =
      "SELECT ECInstanceId FROM BisCore:Element WHERE CodeSpec.Id=? AND CodeScope.Id=? AND CodeValue=?";
    const queryBinder = new QueryBinder()
      .bindId(1, code.spec)
      .bindId(2, Id64.fromString(code.scope))
      .bindString(3, code.value);
    const queryReader = iModel.createQueryReader(query, queryBinder, {
      usePrimaryConn: true,
    });
    return (await queryReader.step()) ? queryReader.current[0] : undefined;
  }
}
