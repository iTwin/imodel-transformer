/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @internal */

import { Id64, Id64String } from "@itwin/core-bentley";
import {
  Category,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  SubCategory,
} from "@itwin/core-backend";
import {
  Code,
  CodeProps,
  CodeScopeSpec,
  ElementProps,
  IModel,
} from "@itwin/core-common";
import type { IModelExporter } from "./IModelExporter";
import type { IModelCloneContext } from "./IModelCloneContext";
import type {
  ElementReservationElementPlan,
  ElementReservationOperation,
} from "./ElementReservationOperation";
import {
  ElementReservationIdentity,
  isSameReservationCode,
  throwReservationUnsupported,
} from "./ElementReservationState";

/** The transformer internals that planning reads. Planning never writes target elements. @internal */
export interface ElementReservationPlannerHost {
  readonly sourceDb: IModelDb;
  readonly targetDb: IModelDb;
  readonly exporter: IModelExporter;
  readonly cloneContext: IModelCloneContext;
  readonly wasSourceIModelCopiedToTarget: boolean;
  readonly preserveElementIdsForFiltering: boolean;
  /** Target elements matched by code; planning adds to it and must undo its additions on failure. */
  readonly targetElementIdsRemappedByCode: Set<Id64String>;
  hasElementChanged(sourceElement: Element): boolean;
  transformElement(sourceElement: Element): Promise<ElementProps>;
  findExistingTargetElement(
    sourceElement: Element,
    targetProps: Pick<ElementProps, "classFullName" | "code">
  ): Promise<Id64String>;
}

function identityOf(
  props: ElementReservationIdentity
): ElementReservationIdentity {
  return {
    classFullName: props.classFullName,
    federationGuid: props.federationGuid,
    code: { ...props.code },
  };
}

/** Classifies source elements into reservation plans for one operation, and undoes its matches if the operation fails. @internal */
export class ElementReservationPlanner {
  private readonly _addedMappings: Array<[Id64String, Id64String]> = [];
  private readonly _addedCodeRemaps = new Set<Id64String>();

  public constructor(
    private readonly _host: ElementReservationPlannerHost,
    private readonly _operation: ElementReservationOperation
  ) {}

  /** Remove the target matches this planner added, unless something else replaced them. */
  public discard(): void {
    const context = this._host.cloneContext;
    for (const [sourceId, targetId] of this._addedMappings) {
      if (context.findTargetElementId(sourceId) === targetId)
        context.removeElement(sourceId);
    }
    for (const targetId of this._addedCodeRemaps)
      this._host.targetElementIdsRemappedByCode.delete(targetId);
  }

  /** Classify one accepted source element without importing it. */
  public async collectElement(
    sourceElement: Element
  ): Promise<ElementReservationElementPlan | undefined> {
    const host = this._host;
    if (!host.hasElementChanged(sourceElement)) return undefined;

    const autoInsertedForCategory =
      this.getAutoInsertingCategory(sourceElement);
    let targetElementId: Id64String;
    let prepared: ElementReservationElementPlan["prepared"];
    let identity: ElementReservationIdentity;

    if (host.wasSourceIModelCopiedToTarget) {
      targetElementId = sourceElement.id;
      identity = identityOf(
        host.targetDb.elements.getElementProps(targetElementId)
      );
    } else if (!host.cloneContext.isBetweenIModels) {
      // Existing in-place matches need no shared allocation or identity-only clone.
      const targetProps = await host.transformElement(sourceElement);
      targetElementId = host.cloneContext.findTargetElementId(sourceElement.id);
      if (!Id64.isValid(targetElementId))
        targetElementId = await host.findExistingTargetElement(
          sourceElement,
          targetProps
        );
      identity = identityOf(targetProps);
      if (
        !Id64.isValidId64(targetElementId) &&
        identity.federationGuid !== undefined
      )
        throwReservationUnsupported(
          sourceElement.id,
          "in-place cloning requires a new reserved identity"
        );
    } else {
      const sourceCode = host.cloneContext.readSourceCode(sourceElement.id);
      // Resolving the scope first keeps scope-before-element ordering.
      const targetCodeScope = await this.resolveCodeScope(
        sourceElement,
        sourceCode
      );
      const mappedId = host.cloneContext.findTargetElementId(sourceElement.id);
      if (Id64.isValidId64(mappedId) && autoInsertedForCategory === undefined) {
        // Updates reserve nothing, so they replay the ordinary transform and
        // this identity is informational only.
        targetElementId = mappedId;
        identity = {
          classFullName: sourceElement.classFullName,
          federationGuid: sourceElement.federationGuid,
          code: {
            spec: host.cloneContext.findTargetCodeSpecId(sourceCode.spec),
            scope: targetCodeScope,
            value: sourceCode.value,
          },
        };
      } else {
        prepared = await host.cloneContext.prepareElementIdentity(
          sourceElement,
          sourceCode,
          targetCodeScope,
          async () => host.transformElement(sourceElement)
        );
        identity = prepared.identity;
        targetElementId =
          autoInsertedForCategory !== undefined
            ? Id64.invalid
            : await this.matchTargetElement(sourceElement, prepared);
      }
    }

    if (
      !Id64.isValid(targetElementId) &&
      identity.federationGuid !== undefined &&
      host.preserveElementIdsForFiltering
    ) {
      throwReservationUnsupported(
        sourceElement.id,
        "preserveElementIdsForFiltering requires a real target ID before insertion"
      );
    }

    return {
      sourceId: sourceElement.id,
      targetElementId,
      autoInsertedForCategory,
      insertId:
        host.preserveElementIdsForFiltering &&
        identity.federationGuid === undefined
          ? sourceElement.id
          : undefined,
      prepared,
      identity,
    };
  }

  /** A default SubCategory of a planned new Category is created by the Category insert, not by us. */
  private getAutoInsertingCategory(
    sourceElement: Element
  ): Id64String | undefined {
    if (
      !(sourceElement instanceof SubCategory) ||
      !sourceElement.isDefaultSubCategory
    )
      return undefined;
    const categoryPlan = this._operation.getElementPlan(
      sourceElement.getCategoryId()
    );
    if (
      categoryPlan === undefined ||
      Id64.isValidId64(categoryPlan.targetElementId)
    )
      return undefined;
    if (
      !(
        this._host.targetDb.getJsClass(categoryPlan.identity.classFullName)
          .prototype instanceof Category
      )
    )
      throwReservationUnsupported(
        sourceElement.id,
        "the transformed parent does not create a default SubCategory"
      );
    return categoryPlan.sourceId;
  }

  /** Find an existing target for a prepared identity, keeping the matched code as the identity. */
  private async matchTargetElement(
    sourceElement: Element,
    prepared: NonNullable<ElementReservationElementPlan["prepared"]>
  ): Promise<Id64String> {
    const context = this._host.cloneContext;
    const oldTargetId = context.findTargetElementId(sourceElement.id);
    if (Id64.isValid(oldTargetId)) return oldTargetId;

    // findExistingTargetElement may rewrite the code to the matched target's code.
    const targetProps = identityOf(prepared.identity);
    const targetElementId = await this._host.findExistingTargetElement(
      sourceElement,
      targetProps
    );
    if (!isSameReservationCode(targetProps.code, prepared.identity.code))
      prepared.codeBeforeMatch = { ...prepared.identity.code };
    prepared.identity.code = { ...targetProps.code };
    if (Id64.isValidId64(targetElementId)) {
      this._addedMappings.push([sourceElement.id, targetElementId]);
      if (this._host.targetElementIdsRemappedByCode.has(targetElementId))
        this._addedCodeRemaps.add(targetElementId);
    }
    return targetElementId;
  }

  /** Resolve the target code scope to a real ID or the GUID of a scope prepared in this operation. */
  private async resolveCodeScope(
    sourceElement: Element,
    sourceCode: CodeProps
  ): Promise<string> {
    const { sourceDb, exporter } = this._host;
    const operation = this._operation;
    if (Code.isEmpty(sourceCode) && sourceCode.spec === Code.createEmpty().spec)
      return IModel.rootSubjectId;

    if (sourceCode.scope === undefined || Id64.isInvalid(sourceCode.scope)) {
      throwReservationUnsupported(
        sourceElement.id,
        "a non-empty code has no usable scope"
      );
    }
    if (sourceCode.scope === IModel.rootSubjectId) {
      const scopeType = sourceDb.codeSpecs.getById(sourceCode.spec).scopeType;
      // The repository model is always 0x1, even when the root Subject is remapped.
      if (
        scopeType === CodeScopeSpec.Type.Repository ||
        scopeType === CodeScopeSpec.Type.Model
      )
        return IModel.rootSubjectId;
      return (
        operation.getLogicalScope(sourceCode.scope) ?? IModel.rootSubjectId
      );
    }

    let targetScope = operation.getLogicalScope(sourceCode.scope);
    if (targetScope !== undefined) return targetScope;

    if (sourceDb.models.tryGetModelProps(sourceCode.scope)) {
      await exporter.exportModel(sourceCode.scope);
    } else {
      await exporter.exportElement(sourceCode.scope);
    }
    targetScope = operation.getLogicalScope(sourceCode.scope);
    if (targetScope === undefined) {
      throwReservationUnsupported(
        sourceElement.id,
        `code scope ${sourceCode.scope} is not an existing or prepared target identity`
      );
    }
    return targetScope;
  }
}
