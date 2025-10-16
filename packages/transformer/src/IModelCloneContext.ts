/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import * as assert from "assert";
import { DbResult, Id64, Id64String, Logger } from "@itwin/core-bentley";
import {
  Code,
  CodeScopeSpec,
  ConcreteEntityTypes,
  ElementAspectProps,
  ElementProps,
  EntityReference,
  IModel,
  IModelError,
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
import { ECReferenceTypesCache } from "./ECReferenceTypesCache";
import { EntityUnifier } from "./EntityUnifier";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { PrimitiveType, type Property } from "@itwin/ecschema-metadata";

const loggerCategory: string = TransformerLoggerCategory.IModelCloneContext;

/** The context for transforming a *source* Element to a *target* Element and remapping internal identifiers to the target iModel.
 * @beta
 */
export class IModelCloneContext extends IModelElementCloneContext {
  private _refTypesCache = new ECReferenceTypesCache();
  private _aspectRemapTable = new Map<Id64String, Id64String>();

  /** perform necessary initialization to use a clone context, namely caching the reference types in the source's schemas */
  public override async initialize() {
    await this._refTypesCache.initAllSchemasInIModel(this.sourceDb);
  }

  /** Clone the specified source Element into ElementProps for the target iModel.
   * @internal
   */
  public override cloneElement(
    sourceElement: Element,
    cloneOptions?: IModelJsNative.CloneElementOptions
  ): ElementProps {
    const targetElementProps: ElementProps = this[
      "_nativeContext"
    ].cloneElement(sourceElement.id, cloneOptions);
    // Ensure that all NavigationProperties in targetElementProps have a defined value so "clearing" changes will be part of the JSON used for update
    sourceElement.forEach((propertyName: string, property: Property) => {
      if (
        property.isNavigation() &&
        undefined === (sourceElement as any)[propertyName]
      ) {
        (targetElementProps as any)[propertyName] = RelatedElement.none;
      }
    }, false); // exclude custom because C++ has already handled them
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
          `Incorrect CodeScope '${targetElementCodeScopeType}' is set for target element ${targetElementProps.id}`
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
    jsClass["onCloned"](this, sourceElement.toJSON(), targetElementProps);
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
  public findTargetEntityId(sourceEntityId: EntityReference): EntityReference {
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
          if (
            EntityUnifier.exists(this.targetDb, { entityReference: targetId })
          )
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
          // eslint-disable-next-line @itwin/no-internal, @typescript-eslint/no-deprecated
          const relInSource = this.sourceDb.withPreparedStatement(
            `
            SELECT
              SourceECInstanceId,
              TargetECInstanceId,
              (${makeGetConcreteEntityTypeSql(
                "SourceECClassId"
              )}) AS SourceType,
              (${makeGetConcreteEntityTypeSql("TargetECClassId")}) AS TargetType
            FROM BisCore:ElementRefersToElements
            WHERE ECInstanceId=?
            `,
            (stmt) => {
              stmt.bindId(1, rawId);
              let status: DbResult;
              while ((status = stmt.step()) === DbResult.BE_SQLITE_ROW) {
                const sourceId = stmt.getValue(0).getId();
                const targetId = stmt.getValue(1).getId();
                const sourceType = stmt.getValue(2).getString() as
                  | ConcreteEntityTypes
                  | "error";
                const targetType = stmt.getValue(3).getString() as
                  | ConcreteEntityTypes
                  | "error";
                if (sourceType === "error" || targetType === "error")
                  throw Error("relationship end had unknown root class");
                return {
                  sourceId: `${sourceType}${sourceId}`,
                  targetId: `${targetType}${targetId}`,
                } as const;
              }
              if (status !== DbResult.BE_SQLITE_DONE)
                throw new IModelError(status, "unexpected query failure");
              return undefined;
            }
          );
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
            sourceId: this.findTargetEntityId(relInSource.sourceId),
            targetId: this.findTargetEntityId(relInSource.targetId),
          };
          // return a null
          if (
            !EntityReferences.isValid(relInTarget.sourceId) ||
            !EntityReferences.isValid(relInTarget.targetId)
          )
            break;
          // eslint-disable-next-line @itwin/no-internal, @typescript-eslint/no-deprecated
          const relInTargetId = this.targetDb.withPreparedStatement(
            `
            SELECT ECInstanceId
            FROM BisCore:ElementRefersToElements
            WHERE SourceECInstanceId=?
              AND TargetECInstanceId=?
            `,
            (stmt) => {
              stmt.bindId(1, EntityReferences.toId64(relInTarget.sourceId));
              stmt.bindId(2, EntityReferences.toId64(relInTarget.targetId));
              const status: DbResult = stmt.step();
              if (status === DbResult.BE_SQLITE_ROW)
                return stmt.getValue(0).getId();
              if (status !== DbResult.BE_SQLITE_DONE)
                throw new IModelError(status, "unexpected query failure");
              return Id64.invalid;
            }
          );
          return `r${relInTargetId}`;
        }
      }
    }
    return `${type}${Id64.invalid}`;
  }

  /** Clone the specified source Element into ElementProps for the target iModel.
   * @internal
   */
  public cloneElementAspect(
    sourceElementAspect: ElementAspect
  ): ElementAspectProps {
    const targetElementAspectProps: ElementAspectProps =
      sourceElementAspect.toJSON();
    targetElementAspectProps.id = undefined;
    sourceElementAspect.forEach((propertyName, property) => {
      if (property.isNavigation()) {
        const sourceNavProp: RelatedElementProps | undefined =
          sourceElementAspect.asAny[propertyName];
        if (sourceNavProp?.id) {
          const navPropRefType = this._refTypesCache.getNavPropRefType(
            sourceElementAspect.schemaName,
            sourceElementAspect.className,
            propertyName
          );
          assert(
            navPropRefType !== undefined,
            `nav prop ref type for '${propertyName}' was not in the cache, this is a bug.`
          );
          const targetEntityReference = this.findTargetEntityId(
            EntityReferences.fromEntityType(sourceNavProp.id, navPropRefType)
          );
          const targetEntityId = EntityReferences.toId64(targetEntityReference);
          // spread the property in case toJSON did not deep-clone
          (targetElementAspectProps as any)[propertyName] = {
            ...(targetElementAspectProps as any)[propertyName],
            id: targetEntityId,
          };
        }
      } else if (
        property.isPrimitive() &&
        property.primitiveType === PrimitiveType.Long &&
        property.extendedTypeName === "Id"
      ) {
        (targetElementAspectProps as any)[propertyName] =
          this.findTargetElementId(sourceElementAspect.asAny[propertyName]);
      }
    });
    return targetElementAspectProps;
  }
}
