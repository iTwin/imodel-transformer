/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import * as assert from "assert";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import {
  Code, CodeScopeSpec, ConcreteEntityTypes, ElementAspectProps, ElementProps, EntityProps, EntityReference, IModelError,
  PrimitiveTypeCode, PropertyMetaData, RelatedElement, RelatedElementProps,
} from "@itwin/core-common";
import {
  ClassRegistry,
  Element, ElementAspect, Entity, EntityReferences, GeometricElement, GeometricElement3d, GeometryPart, IModelDb, IModelElementCloneContext, IModelJsNative, SQLiteDb,
} from "@itwin/core-backend";
import { ECReferenceTypesCache } from "./ECReferenceTypesCache";
import { EntityUnifier } from "./EntityUnifier";

/** The context for transforming a *source* Element to a *target* Element and remapping internal identifiers to the target iModel.
 * @beta
 * FIXME: while this inherits from IModelElementCloneContext, it ignores the remapping tables there and keeps them in JS (for now)
 */
export class IModelCloneContext extends IModelElementCloneContext {
  private _refTypesCache = new ECReferenceTypesCache();

  /** perform necessary initialization to use a clone context, namely caching the reference types in the source's schemas */
  public override async initialize() {
    await this._refTypesCache.initAllSchemasInIModel(this.sourceDb);
  }

  /**
   * Returns `true` if this context is for transforming between 2 iModels and `false` if it for transforming within the same iModel.
   * @deprecated, use [[targetIsSource]]
   */
  public override get isBetweenIModels(): boolean { return this.targetIsSource; }

  /** Returns `true` if this context is for transforming between 2 iModels and `false` if it for transforming within the same iModel. */
  public get targetIsSource(): boolean { return this.sourceDb !== this.targetDb; }

  private _aspectRemapTable = new Map<Id64String, Id64String>();
  private _elementRemapTable = new Map<Id64String, Id64String>();
  private _codeSpecRemapTable = new Map<Id64String, Id64String>();

  private _elementClassRemapTable = new Map<typeof Entity, typeof Entity>();

  /** Add a rule that remaps the specified source [CodeSpec]($common) to the specified target [CodeSpec]($common).
   * @param sourceCodeSpecName The name of the CodeSpec from the source iModel.
   * @param targetCodeSpecName The name of the CodeSpec from the target iModel.
   * @throws [[IModelError]] if either CodeSpec could not be found.
   */
  public override remapCodeSpec(sourceCodeSpecName: string, targetCodeSpecName: string): void {
    const sourceCodeSpec = this.sourceDb.codeSpecs.getByName(sourceCodeSpecName);
    const targetCodeSpec = this.targetDb.codeSpecs.getByName(targetCodeSpecName);
    this._codeSpecRemapTable.set(sourceCodeSpec.id, targetCodeSpec.id);
  }

  /** Add a rule that remaps the specified source class to the specified target class. */
  public override remapElementClass(sourceClassFullName: string, targetClassFullName: string): void {
    // NOTE: should probably also map class ids
    const sourceClass = ClassRegistry.getClass(sourceClassFullName, this.sourceDb);
    const targetClass = ClassRegistry.getClass(targetClassFullName, this.targetDb);
    this._elementClassRemapTable.set(sourceClass, targetClass);
  }

  /** Add a rule that remaps the specified source Element to the specified target Element. */
  public override remapElement(sourceId: Id64String, targetId: Id64String): void {
    this._elementRemapTable.set(sourceId, targetId);
  }

  /** Remove a rule that remaps the specified source Element. */
  public override removeElement(sourceId: Id64String): void {
    this._elementRemapTable.delete(sourceId);
  }

  /** Look up a target CodeSpecId from the source CodeSpecId.
   * @returns the target CodeSpecId or [Id64.invalid]($bentley) if a mapping not found.
   */
  public override findTargetCodeSpecId(sourceId: Id64String): Id64String {
    if (Id64.invalid === sourceId) {
      return Id64.invalid;
    }
    return this._codeSpecRemapTable.get(sourceId) ?? Id64.invalid;
  }

  /** Look up a target ElementId from the source ElementId.
   * @returns the target ElementId or [Id64.invalid]($bentley) if a mapping not found.
   */
  public override findTargetElementId(sourceElementId: Id64String): Id64String {
    if (Id64.invalid === sourceElementId) {
      return Id64.invalid;
    }
    return this._elementRemapTable.get(sourceElementId) ?? Id64.invalid;
  }

  /** Add a rule that remaps the specified source ElementAspect to the specified target ElementAspect. */
  public remapElementAspect(aspectSourceId: Id64String, aspectTargetId: Id64String): void {
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
          const targetId = `m${this.findTargetElementId(rawId)}` as const;
          // Check if the model exists, `findTargetElementId` may have worked because the element exists when the model doesn't.
          // That can occur in the transformer since a submodeled element is imported before its submodel.
          if (EntityUnifier.exists(this.targetDb, { entityReference: targetId }))
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
          const relInSource = this.sourceDb.withPreparedStatement(
            `
            SELECT
              SourceECInstanceId,
              TargetECInstanceId,
              (${makeGetConcreteEntityTypeSql("SourceECClassId")}) AS SourceType,
              (${makeGetConcreteEntityTypeSql("TargetECClassId")}) AS TargetType
            FROM BisCore:ElementRefersToElements
            WHERE ECInstanceId=?
            `, (stmt) => {
              stmt.bindId(1, rawId);
              let status: DbResult;
              while ((status = stmt.step()) === DbResult.BE_SQLITE_ROW) {
                const sourceId = stmt.getValue(0).getId();
                const targetId = stmt.getValue(1).getId();
                const sourceType = stmt.getValue(2).getString() as ConcreteEntityTypes | "error";
                const targetType = stmt.getValue(3).getString() as ConcreteEntityTypes | "error";
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
            });
          if (relInSource === undefined)
            break;
          // just in case prevent recursion
          if (relInSource.sourceId === sourceEntityId || relInSource.targetId === sourceEntityId)
            throw Error("link table relationship end was resolved to itself. This should be impossible");
          const relInTarget = {
            sourceId: this.findTargetEntityId(relInSource.sourceId),
            targetId: this.findTargetEntityId(relInSource.targetId),
          };
          // return a null
          if (Id64.isInvalid(relInTarget.sourceId) || Id64.isInvalid(relInTarget.targetId))
            break;
          const relInTargetId = this.sourceDb.withPreparedStatement(
            `
            SELECT ECInstanceId
            FROM BisCore:ElementRefersToElements
            WHERE SourceECInstanceId=?
              AND TargetECInstanceId=?
            `, (stmt) => {
              stmt.bindId(1, EntityReferences.toId64(relInTarget.sourceId));
              stmt.bindId(2, EntityReferences.toId64(relInTarget.targetId));
              let status: DbResult;
              if ((status = stmt.step()) === DbResult.BE_SQLITE_ROW)
                return stmt.getValue(0).getId();
              if (status !== DbResult.BE_SQLITE_DONE)
                throw new IModelError(status, "unexpected query failure");
              return Id64.invalid;
            });
          return `r${relInTargetId}`;
        }
      }
    }
    return `${type}${Id64.invalid}`;
  }

  /** Clone the specified source Element into ElementProps for the target iModel.
   * @internal
   */
  public cloneElementAspect(sourceElementAspect: ElementAspect): ElementAspectProps {
    return this._cloneEntity(sourceElementAspect) as ElementAspectProps;
  }

  private _cloneEntity(sourceEntity: Entity): EntityProps {
    const targetEntityProps: EntityProps = sourceEntity.toJSON();
    targetEntityProps.id = undefined;

    if (this.targetIsSource)
      return targetEntityProps;

    sourceEntity.forEachProperty((propertyName, propertyMetaData) => {
      if (propertyMetaData.isNavigation) {
        const sourceNavProp: RelatedElementProps | undefined = (sourceEntity as any)[propertyName];
        if (sourceNavProp?.id) {
          const navPropRefType = this._refTypesCache.getNavPropRefType(
            sourceEntity.schemaName,
            sourceEntity.className,
            propertyName
          );
          assert(navPropRefType !== undefined,`nav prop ref type for '${propertyName}' was not in the cache, this is a bug.`);
          const targetEntityReference = this.findTargetEntityId(EntityReferences.fromEntityType(sourceNavProp.id, navPropRefType));
          const targetEntityId = EntityReferences.toId64(targetEntityReference);
          // spread the property in case toJSON did not deep-clone
          (targetEntityProps as any)[propertyName] = { ...(targetEntityProps as any)[propertyName], id: targetEntityId };
        }
      } else if ((PrimitiveTypeCode.Long === propertyMetaData.primitiveType) && ("Id" === propertyMetaData.extendedType)) {
        (targetEntityProps as any)[propertyName] = this.findTargetElementId((sourceEntity as any)[propertyName]);
      }
    });

    return targetEntityProps;
  }

  /** Clone the specified source Element into ElementProps for the target iModel.
   * @internal
   */
  public override cloneElement(sourceElement: Element, cloneOptions?: IModelJsNative.CloneElementOptions): ElementProps {
    /*
    // FIXME: remove
    const targetModelId = sourceElement.model === IModelDb.repositoryModelId
      ? IModelDb.repositoryModelId
      : this.findTargetElementId(sourceElement.id);
    */

    // Clone
    const targetElementProps = this._cloneEntity(sourceElement) as ElementProps;
    // send geometry (if binaryGeometry, try querying it via raw SQLite as an array buffer)
    if (cloneOptions?.binaryGeometry && (sourceElement instanceof GeometricElement3d || sourceElement instanceof GeometryPart)) {
      // TODO: handle 2d
      // NOTE: how do I remap the material Ids in here?
      this.sourceDb.withPreparedSqliteStatement("SELECT GeometryStream FROM bis_GeometricElement3d WHERE ECInstanceId=?", (stmt) => {
        stmt.bindId(1, sourceElement.id);
        assert(stmt.step() === DbResult.BE_SQLITE_ROW);
        const geomBinary = stmt.getValue(0).getBlob();
        assert(stmt.step() === DbResult.BE_SQLITE_DONE);
        (targetElementProps as any)["geomBinary"] = geomBinary;
      });
    }

    if (!cloneOptions?.binaryGeometry)
      throw Error("not yet supported, will require the native context to be modified");

    // // FIXME: do we still need this?>
    // Ensure that all NavigationProperties in targetElementProps have a defined value
    // so "clearing" changes will be part of the JSON used for update
    sourceElement.forEachProperty((propertyName: string, meta: PropertyMetaData) => {
      if ((meta.isNavigation) && (undefined === (sourceElement as any)[propertyName])) {
        (targetElementProps as any)[propertyName] = RelatedElement.none;
      }
    }, false); // exclude custom because C++ has already handled them (THIS IS NOW FALSE)

    if (this.targetIsSource) {
      // The native C++ cloneElement strips off federationGuid, want to put it back if transformation is into itself
      targetElementProps.federationGuid = sourceElement.federationGuid;
      if (CodeScopeSpec.Type.Repository === this.targetDb.codeSpecs.getById(targetElementProps.code.spec).scopeType) {
        targetElementProps.code.scope = IModelDb.rootSubjectId;
      }
    }
    // unlike other references, code cannot be null. If it is null, use an empty code instead
    if (targetElementProps.code.scope === Id64.invalid || targetElementProps.code.spec === Id64.invalid) {
      targetElementProps.code = Code.createEmpty();
    }
    const jsClass = this.sourceDb.getJsClass<typeof Element>(sourceElement.classFullName);
    // eslint-disable-next-line @typescript-eslint/dot-notation
    jsClass["onCloned"](this, sourceElement.toJSON(), targetElementProps);
    return targetElementProps;
  }

  /** Import a single CodeSpec from the source iModel into the target iModel.
   * @internal
   */
  public override importCodeSpec(sourceCodeSpecId: Id64String): void {
    if (this._codeSpecRemapTable.has(sourceCodeSpecId))
      return;
    if (this.targetIsSource)
      return;
    const sourceCodeSpec = this.sourceDb.codeSpecs.getById(sourceCodeSpecId);
    delete (sourceCodeSpec as any).id;
    // TODO: test code spec name collision fails
    this.targetDb.codeSpecs.insert(sourceCodeSpec);
  }


  private static aspectRemapTableName = "AspectIdRemaps";

  public override saveStateToDb(db: SQLiteDb): void {
    super.saveStateToDb(db);
    if (DbResult.BE_SQLITE_DONE !== db.executeSQL(
      `CREATE TABLE ${IModelCloneContext.aspectRemapTableName} (Source INTEGER, Target INTEGER)`
    ))
      throw Error("Failed to create the aspect remap table in the state database");
    db.saveChanges();
    db.withPreparedSqliteStatement(
      `INSERT INTO ${IModelCloneContext.aspectRemapTableName} (Source, Target) VALUES (?, ?)`,
      (stmt) => {
        for (const [source, target] of this._aspectRemapTable) {
          stmt.reset();
          stmt.bindId(1, source);
          stmt.bindId(2, target);
          if (DbResult.BE_SQLITE_DONE !== stmt.step())
            throw Error("Failed to insert aspect remapping into the state database");
        }
      });
  }

  public override loadStateFromDb(db: SQLiteDb): void {
    super.loadStateFromDb(db);
    // FIXME: test this
    db.withSqliteStatement(`SELECT Source, Target FROM ${IModelCloneContext.aspectRemapTableName}`, (stmt) => {
      let status = DbResult.BE_SQLITE_ERROR;
      while ((status = stmt.step()) === DbResult.BE_SQLITE_ROW) {
        const source = stmt.getValue(0).getId();
        const target = stmt.getValue(1).getId();
        this._aspectRemapTable.set(source, target);
      }
      assert(status === DbResult.BE_SQLITE_DONE);
    });
  }
}
