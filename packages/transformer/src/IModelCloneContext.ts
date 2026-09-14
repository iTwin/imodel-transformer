/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import { strict as assert } from "node:assert";
import { Id64, Id64String, ITwinError, Logger } from "@itwin/core-bentley";
import {
  Code,
  CodeProps,
  CodeScopeSpec,
  ConcreteEntityTypes,
  ECJsNames,
  ElementAspectProps,
  ElementProps,
  EntityReference,
  IModel,
  QueryBinder,
  RelatedElement,
  RelatedElementProps,
} from "@itwin/core-common";
import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  EntityReferences,
  IModelElementCloneContext,
  IModelJsNative,
} from "@itwin/core-backend";
import { EntityClass } from "@itwin/ecschema-metadata";
import { ECReferenceTypesCache } from "./ECReferenceTypesCache";
import { EntityExistenceCache } from "./EntityExistenceCache";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { BigMap } from "./BigMap";
import type { IModelTransformContext } from "./IModelTransformContext";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError";

/** Target identity only; model IDs and geometry are resolved during ordinary cloning. @internal */
export type ElementReservationIdentity = Pick<
  ElementProps,
  "classFullName" | "federationGuid" | "code"
>;

/** Compact preparation state; codeBeforeHooks is pinned separately to avoid applying customization twice. @internal */
export interface PreparedElementIdentity {
  sourceId: Id64String;
  identity: ElementReservationIdentity;
  codeBeforeHooks: CodeProps;
  /** Preserve hook output separately when target matching rewrites its code. */
  codeBeforeMatch?: CodeProps;
}

const loggerCategory: string = TransformerLoggerCategory.IModelCloneContext;

/** The context for transforming a *source* Element to a *target* Element and remapping internal identifiers to the target iModel.
 * @internal
 */
export class IModelCloneContext
  extends IModelElementCloneContext
  implements IModelTransformContext
{
  private _refTypesCache = new ECReferenceTypesCache();
  private _aspectRemapTable = new BigMap<Id64String>();
  private readonly _elementClassRemaps = new Map<string, string>();
  private _preparedCode: { sourceId: Id64String; code: CodeProps } | undefined;
  private _identityPreparation:
    | { sourceId: Id64String; identityOnly: boolean }
    | undefined;

  public override remapElementClass(
    sourceClassFullName: string,
    targetClassFullName: string
  ): void {
    super.remapElementClass(sourceClassFullName, targetClassFullName);
    this._elementClassRemaps.set(
      sourceClassFullName.replace(".", ":").toLowerCase(),
      targetClassFullName
    );
  }

  /** Whether the current transform has only an identity, not fully cloned target props. @internal */
  public isPreparingIdentityOnly(sourceId: Id64String): boolean {
    return (
      this._identityPreparation?.sourceId === sourceId &&
      this._identityPreparation.identityOnly
    );
  }

  /** Pin standard code mapping before customization, for preparation or replay. @internal */
  private async withPreparedCode<T>(
    sourceId: Id64String,
    code: CodeProps,
    action: () => Promise<T>
  ): Promise<T> {
    const previous = this._preparedCode;
    this._preparedCode = { sourceId, code: { ...code } };
    try {
      return await action();
    } finally {
      this._preparedCode = previous;
    }
  }

  /** Prepare the target identity through the existing transform callbacks, without target writes. @internal */
  public async prepareElementIdentity(
    sourceElement: Element,
    targetCodeScope: string,
    transform: () => Promise<ElementProps>
  ): Promise<PreparedElementIdentity> {
    if (!this.isBetweenIModels)
      this.unsupportedIdentity(sourceElement, "in-place cloning");
    // Element.code constructs a Code, which trims whitespace. Native cloning
    // reads the stored value, so preparation must use raw props for parity.
    const sourceCode = this.sourceDb.elements.getElementProps(
      sourceElement.id
    ).code;
    const emptyCode = Code.createEmpty().toJSON();
    // The intrinsic null code spec is not necessarily in the native remap table.
    // Do not erase an empty-valued domain code, which can still be customized.
    const code =
      Code.isEmpty(sourceCode) && sourceCode.spec === emptyCode.spec
        ? emptyCode
        : {
            spec: this.findTargetCodeSpecId(sourceCode.spec),
            scope: targetCodeScope,
            value: sourceCode.value,
          };
    if (!code.scope || code.scope === Id64.invalid)
      this.unsupportedIdentity(sourceElement, "an unresolved code scope");
    return this.withPreparedCode(sourceElement.id, code, async () => {
      const previous = this._identityPreparation;
      this._identityPreparation = {
        sourceId: sourceElement.id,
        identityOnly: false,
      };
      try {
        const props = await transform();
        assert(this._preparedCode?.sourceId === sourceElement.id);
        return {
          sourceId: sourceElement.id,
          identity: {
            classFullName: props.classFullName,
            federationGuid: props.federationGuid,
            code: { ...props.code },
          },
          codeBeforeHooks: { ...this._preparedCode.code },
        };
      } finally {
        this._identityPreparation = previous;
      }
    });
  }

  /** Re-run ordinary transformation with the same code input and check the reserved identity. @internal */
  public async transformPreparedElement(
    prepared: PreparedElementIdentity,
    transform: () => Promise<ElementProps>
  ): Promise<ElementProps> {
    return this.withPreparedCode(
      prepared.sourceId,
      prepared.codeBeforeHooks,
      async () => {
        const props = await transform();
        const expected = prepared.identity;
        const expectedCode = prepared.codeBeforeMatch ?? expected.code;
        if (
          props.classFullName !== expected.classFullName ||
          props.federationGuid !== expected.federationGuid ||
          props.code.spec !== expectedCode.spec ||
          props.code.scope !== expectedCode.scope ||
          (props.code.value ?? "") !== (expectedCode.value ?? "")
        ) {
          ITwinError.throwError({
            iTwinErrorId: {
              scope: IModelTransformerErrorScope,
              key: IModelTransformerError.ElementReservationPlanChanged,
            },
            message: `Reservation identity changed during execution for ${prepared.sourceId}. Transform callbacks must preserve the prepared GUID, class, and code.`,
          });
        }
        if (prepared.codeBeforeMatch !== undefined)
          props.code = { ...expected.code };
        return props;
      }
    );
  }

  private unsupportedIdentity(sourceElement: Element, reason: string): never {
    ITwinError.throwError({
      iTwinErrorId: {
        scope: IModelTransformerErrorScope,
        key: IModelTransformerError.ElementReservationUnsupported,
      },
      message: `Cannot prepare reservation identity for ${sourceElement.id}: ${reason}. Preparation cannot insert target dependencies first.`,
    });
  }

  private identityProps(sourceElement: Element): ElementProps {
    assert(
      this._preparedCode?.sourceId === sourceElement.id,
      "Identity preparation requires pinned code."
    );
    const identity: ElementReservationIdentity = {
      classFullName: this.targetDb.getJsClass<typeof Element>(
        this._elementClassRemaps.get(
          sourceElement.classFullName.toLowerCase()
        ) ?? sourceElement.classFullName
      ).classFullName,
      federationGuid: sourceElement.federationGuid,
      code: { ...this._preparedCode.code },
    };
    // This is not a substitute for native cloning. A hook may use source data or
    // identity fields, but must not mistake missing target props for undefined.
    return new Proxy(identity as ElementProps, {
      get: (target, property, receiver) => {
        if (property === "then") return undefined; // Promise resolution inspects thenables.
        if (!Object.prototype.hasOwnProperty.call(target, property))
          this.unsupportedIdentity(
            sourceElement,
            `transform hook reads target property ${String(property)} before its model exists`
          );
        return Reflect.get(target, property, receiver);
      },
      has: (target, property) => {
        if (!Object.prototype.hasOwnProperty.call(target, property))
          this.unsupportedIdentity(
            sourceElement,
            `transform hook inspects target property ${String(property)} before its model exists`
          );
        return true;
      },
      ownKeys: () =>
        this.unsupportedIdentity(
          sourceElement,
          "transform hook enumerates incomplete target props"
        ),
      getOwnPropertyDescriptor: (target, property) => {
        if (!Object.prototype.hasOwnProperty.call(target, property))
          this.unsupportedIdentity(
            sourceElement,
            `transform hook inspects target property ${String(property)} before its model exists`
          );
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
  }

  /** A positive-only cache of entity existence checks, scoped to the current transformation run.
   * @internal
   */
  public readonly existenceCache = new EntityExistenceCache();

  /** perform necessary initialization to use a clone context, namely caching the reference types in the source's schemas */
  public override async initialize() {
    await this._refTypesCache.initAllSchemasInIModel(this.sourceDb);
  }

  /** Clone the specified source Element into ElementProps for the target iModel.
   * @internal
   */
  public override async cloneElement(
    sourceElement: Element,
    cloneOptions?: IModelJsNative.CloneElementOptions
  ): Promise<ElementProps> {
    const preparation =
      this._identityPreparation?.sourceId === sourceElement.id
        ? this._identityPreparation
        : undefined;
    if (preparation) {
      const scopedCode = this._preparedCode;
      assert(scopedCode?.sourceId === sourceElement.id);
      if (!Id64.isValidId64(scopedCode.code.spec)) {
        // Native cloning imports CodeSpecs lazily. Incremental exports need not
        // include an unchanged CodeSpec, so mirror that setup before identity-only cloning.
        const sourceSpec = this.sourceDb.elements.getElementProps(
          sourceElement.id
        ).code.spec;
        this.importCodeSpec(sourceSpec);
        scopedCode.code.spec = this.findTargetCodeSpecId(sourceSpec);
      }
      const targetModelId =
        sourceElement.model === IModel.repositoryModelId
          ? IModel.repositoryModelId
          : this.findTargetElementId(sourceElement.model);
      preparation.identityOnly =
        !Id64.isValidId64(targetModelId) ||
        !this.targetDb.models.tryGetModelProps(targetModelId);
    }
    const targetElementProps: ElementProps = preparation?.identityOnly
      ? this.identityProps(sourceElement)
      : this["_nativeContext"].cloneElement(sourceElement.id, cloneOptions);
    // Native same-iModel cloning clears codes for inserts, but identity mappings are updates.
    if (
      !this.isBetweenIModels &&
      this.findTargetElementId(sourceElement.id) === sourceElement.id
    ) {
      targetElementProps.code.value = sourceElement.code.value;
      if (!Id64.isValidId64(targetElementProps.code.spec)) {
        const mappedCodeSpecId = this.findTargetCodeSpecId(
          sourceElement.code.spec
        );
        targetElementProps.code.spec = Id64.isValidId64(mappedCodeSpecId)
          ? mappedCodeSpecId
          : sourceElement.code.spec;
      }
      if (!Id64.isValidId64(targetElementProps.code.scope)) {
        const mappedCodeScopeId = this.findTargetElementId(
          sourceElement.code.scope
        );
        targetElementProps.code.scope = Id64.isValidId64(mappedCodeScopeId)
          ? mappedCodeScopeId
          : sourceElement.code.scope;
      }
    }
    // Ensure that all NavigationProperties in targetElementProps have a defined value so "clearing" changes will be part of the JSON used for update
    const sourceElementClass =
      sourceElement.iModel.schemaContext.getSchemaItemSync(
        sourceElement.schemaItemKey,
        EntityClass
      );
    assert(
      sourceElementClass !== undefined,
      `Cannot get metadata for ${sourceElement.classFullName}.`
    );
    for (const property of preparation?.identityOnly
      ? []
      : sourceElementClass.getPropertiesSync()) {
      // Exclude custom properties because C++ has already handled them.
      if (property.customAttributes?.has("BisCore.CustomHandledProperty"))
        continue;

      const name = property.name;
      if (
        property.isNavigation() &&
        undefined === (sourceElement as any)[ECJsNames.toJsName(name)]
      ) {
        (targetElementProps as any)[ECJsNames.toJsName(name)] =
          RelatedElement.none;
      }
    }
    const preparedCode =
      this._preparedCode?.sourceId === sourceElement.id
        ? this._preparedCode.code
        : undefined;
    if (preparedCode !== undefined) {
      // Pending scopes have GUID identities, not native target mappings. Keep
      // this representation before customization runs, including during replay.
      targetElementProps.code = { ...preparedCode };
    }
    if (this.isBetweenIModels) {
      // The native C++ cloneElement strips off federationGuid, want to put it back if transformation is between iModels
      targetElementProps.federationGuid = sourceElement.federationGuid;
      const targetElementCodeScopeType = this.targetDb.codeSpecs.getById(
        targetElementProps.code.spec
      ).scopeType;
      if (
        CodeScopeSpec.Type.Repository === targetElementCodeScopeType &&
        targetElementProps.code.scope !== IModel.rootSubjectId
      ) {
        Logger.logWarning(
          loggerCategory,
          `Incorrect CodeScope '${targetElementCodeScopeType}' is set for target element ${preparation?.identityOnly ? sourceElement.id : targetElementProps.id}`
        );
      }
    }
    // unlike other references, code cannot be null. If it is null, use an empty code instead
    // this will be updated back later as the transformer resolves references
    if (
      targetElementProps.code.scope === Id64.invalid ||
      targetElementProps.code.spec === Id64.invalid
    ) {
      targetElementProps.code = Code.createEmpty();
    }
    const jsClass = this.sourceDb.getJsClass<typeof Element>(
      sourceElement.classFullName
    );
    await jsClass["onCloned"](this, sourceElement.toJSON(), targetElementProps);
    return targetElementProps;
  }

  /** Add a rule that remaps the specified source ElementAspect to the specified target ElementAspect. */
  public remapElementAspect(
    aspectSourceId: Id64String,
    aspectTargetId: Id64String
  ): void {
    this._aspectRemapTable.set(aspectSourceId, aspectTargetId);
  }

  /** Remove a rule that remaps the specified source ElementAspect */
  public removeElementAspect(aspectSourceId: Id64String): void {
    this._aspectRemapTable.delete(aspectSourceId);
  }

  /** Look up a target AspectId from the source AspectId.
   * @returns the target AspectId or [Id64.invalid]($bentley) if a mapping not found.
   */
  public findTargetAspectId(sourceAspectId: Id64String): Id64String {
    return this._aspectRemapTable.get(sourceAspectId) ?? Id64.invalid;
  }

  /** Look up a target [EntityReference]($bentley) from a source [EntityReference]($bentley)
   * @returns the target CodeSpecId or a [EntityReference]($bentley) containing [Id64.invalid]($bentley) if a mapping is not found.
   */
  public async findTargetEntityId(
    sourceEntityId: EntityReference
  ): Promise<EntityReference> {
    const [type, rawId] = EntityReferences.split(sourceEntityId);
    if (Id64.isValid(rawId)) {
      switch (type) {
        case ConcreteEntityTypes.Model: {
          let targetId;
          /**
           * The repository model is a singleton container which exists in all iModels which has a modelId of 0x1.
           * It is possible to remap the root subject (id 0x1) to some other non root subject, but this is not possible for the repository model.
           * The RepositoryModel only contains bis:Subject elements and bis:InformationpartitionElement instances (https://www.itwinjs.org/bis/domains/biscore.ecschema/#repositorymodel)
           * The system handler for both bis:InformationPartitionElement and bis:Subject will only permit instances to be inserted into the RepositoryModel.
           * https://www.itwinjs.org/bis/domains/biscore.ecschema/#informationpartitionelement
           * https://www.itwinjs.org/bis/domains/biscore.ecschema/#subject
           * Since there is a chance that the root subject is remapped to some other subject which has the same id as the repositoryModel, we need to ignore the remapping for the repository model.
           */
          if (rawId === IModel.repositoryModelId) {
            targetId = `m${IModel.repositoryModelId}` as const;
          } else {
            targetId = `m${this.findTargetElementId(rawId)}` as const;
          }
          // Check if the model exists, `findTargetElementId` may have worked because the element exists when the model doesn't.
          // That can occur in the transformer since a submodeled element is imported before its submodel.
          if (await this.existenceCache.exists(this.targetDb, targetId))
            return targetId;
          break;
        }
        case ConcreteEntityTypes.Element:
          return `e${this.findTargetElementId(rawId)}`;
        case ConcreteEntityTypes.ElementAspect:
          return `a${this.findTargetAspectId(rawId)}`;
        case ConcreteEntityTypes.Relationship: {
          const makeGetConcreteEntityTypeSql = (property: string) => `
            CASE
              WHEN [${property}] IS (BisCore.ElementUniqueAspect) OR [${property}] IS (BisCore.ElementMultiAspect)
                THEN 'a'
              WHEN [${property}] IS (BisCore.Element)
                THEN 'e'
              WHEN [${property}] IS (BisCore.Model)
                THEN 'm'
              WHEN [${property}] IS (BisCore.CodeSpec)
                THEN 'c'
              WHEN [${property}] IS (BisCore.ElementRefersToElements) -- TODO: ElementDrivesElement still not handled by the transformer
                THEN 'r'
              ELSE 'error'
            END
          `;
          const sql = `
            SELECT
              SourceECInstanceId,
              TargetECInstanceId,
              (${makeGetConcreteEntityTypeSql(
                "SourceECClassId"
              )}) AS SourceType,
              (${makeGetConcreteEntityTypeSql("TargetECClassId")}) AS TargetType
            FROM BisCore:ElementRefersToElements
            WHERE ECInstanceId=?
            `;
          const params = new QueryBinder().bindId(1, rawId);
          const reader = this.sourceDb.createQueryReader(sql, params, {
            usePrimaryConn: true,
          });
          const relInSource = await (async () => {
            if (await reader.step()) {
              const sourceId = reader.current[0];
              const targetId = reader.current[1];
              const sourceType = reader.current[2] as
                | ConcreteEntityTypes
                | "error";
              const targetType = reader.current[3] as
                | ConcreteEntityTypes
                | "error";
              if (sourceType === "error" || targetType === "error")
                throw Error("relationship end had unknown root class");
              return {
                sourceId: `${sourceType}${sourceId}`,
                targetId: `${targetType}${targetId}`,
              } as const;
            }
            return undefined;
          })();
          if (relInSource === undefined) break;
          // just in case prevent recursion
          if (
            relInSource.sourceId === sourceEntityId ||
            relInSource.targetId === sourceEntityId
          )
            throw Error(
              "link table relationship end was resolved to itself. This should be impossible"
            );
          const relInTarget = {
            sourceId: await this.findTargetEntityId(relInSource.sourceId),
            targetId: await this.findTargetEntityId(relInSource.targetId),
          };
          // return a null
          if (
            !EntityReferences.isValid(relInTarget.sourceId) ||
            !EntityReferences.isValid(relInTarget.targetId)
          )
            break;
          const relInTargetSql = `
            SELECT ECInstanceId
            FROM BisCore:ElementRefersToElements
            WHERE SourceECInstanceId=?
              AND TargetECInstanceId=?
            `;
          const relInTargetParams = new QueryBinder();
          relInTargetParams.bindId(
            1,
            EntityReferences.toId64(relInTarget.sourceId)
          );
          relInTargetParams.bindId(
            2,
            EntityReferences.toId64(relInTarget.targetId)
          );
          const relInTargetResult = await this.targetDb
            .createQueryReader(relInTargetSql, relInTargetParams, {
              usePrimaryConn: true,
            })
            .next();
          const relInTargetId = relInTargetResult.done
            ? Id64.invalid
            : relInTargetResult.value.id;
          return `r${relInTargetId}`;
        }
      }
    }
    return `${type}${Id64.invalid}`;
  }

  /** Clone the specified source Element into ElementProps for the target iModel.
   * @internal
   */
  public async cloneElementAspect(
    sourceElementAspect: ElementAspect
  ): Promise<ElementAspectProps> {
    const targetElementAspectProps: ElementAspectProps =
      sourceElementAspect.toJSON();
    targetElementAspectProps.id = undefined;
    const targetEntityIds: Promise<void>[] = [];
    const sourceElementAspectClass =
      sourceElementAspect.iModel.schemaContext.getSchemaItemSync(
        sourceElementAspect.schemaItemKey,
        EntityClass
      );
    assert(
      sourceElementAspectClass !== undefined,
      `Cannot get metadata for ${sourceElementAspect.classFullName}.`
    );
    for (const property of sourceElementAspectClass.getPropertiesSync()) {
      const ecName = property.name;
      const name = ECJsNames.toJsName(ecName);
      if (property.isNavigation()) {
        const sourceNavProp: RelatedElementProps | undefined =
          sourceElementAspect.asAny[name];
        if (sourceNavProp?.id) {
          const navPropRefType = this._refTypesCache.getNavPropRefType(
            sourceElementAspect.schemaName,
            sourceElementAspect.className,
            name
          );
          assert(
            navPropRefType !== undefined,
            `nav prop ref type for '${name}' was not in the cache, this is a bug.`
          );
          targetEntityIds.push(
            this.findTargetEntityId(
              EntityReferences.fromEntityType(sourceNavProp.id, navPropRefType)
            ).then((targetEntityReference) => {
              const targetEntityId = EntityReferences.toId64(
                targetEntityReference
              );
              // spread the property in case toJSON did not deep-clone
              (targetElementAspectProps as any)[name] = {
                ...(targetElementAspectProps as any)[name],
                id: targetEntityId,
              };
            })
          );
        }
      } else if (property.isPrimitive() && "Id" === property.extendedTypeName) {
        (targetElementAspectProps as any)[name] = this.findTargetElementId(
          sourceElementAspect.asAny[name]
        );
      }
    }
    await Promise.all(targetEntityIds);
    return targetElementAspectProps;
  }
}
