/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import {
  BriefcaseDb,
  BriefcaseManager,
  ChangedECInstance,
  ChangesetECAdaptor,
  ElementMultiAspect,
  ElementRefersToElements,
  ElementUniqueAspect,
  IModelDb,
  IModelJsNative,
  PartialECChangeUnifier,
  SqliteChangeOp,
  SqliteChangesetReader,
} from "@itwin/core-backend";
import {
  Id64,
  Id64Arg,
  Id64Set,
  Id64String,
  IModelStatus,
  ITwinError,
} from "@itwin/core-bentley";
import { IModelError, QueryBinder } from "@itwin/core-common";
import type { ExportChangesOptions } from "./IModelExporter";

/**
 * Arguments for [[ChangedInstanceIds.initialize]]
 * @public
 */
export type ChangedInstanceIdsInitOptions = ExportChangesOptions & {
  iModel: BriefcaseDb;
};

/** Class for holding change information.
 * @public
 */
export class ChangedInstanceOps {
  public insertIds = new Set<Id64String>();
  public updateIds = new Set<Id64String>();
  public deleteIds = new Set<Id64String>();

  /** Initializes the object from IModelJsNative.ChangedInstanceOpsProps. */
  public addFromJson(
    val: IModelJsNative.ChangedInstanceOpsProps | undefined
  ): void {
    if (undefined !== val) {
      if (undefined !== val.insert && Array.isArray(val.insert))
        val.insert.forEach((id: Id64String) => this.insertIds.add(id));

      if (undefined !== val.update && Array.isArray(val.update))
        val.update.forEach((id: Id64String) => this.updateIds.add(id));

      if (undefined !== val.delete && Array.isArray(val.delete))
        val.delete.forEach((id: Id64String) => this.deleteIds.add(id));
    }
  }

  /**
   * Checks if empty.
   * @returns true if there no ids in the ChangedInstanceOps object.
   */
  public get isEmpty(): boolean {
    return (
      0 === this.insertIds.size &&
      0 === this.updateIds.size &&
      0 === this.deleteIds.size
    );
  }
}

/**
 * Class for discovering modified elements between 2 versions of an iModel.
 * @public
 */
export class ChangedInstanceIds {
  public codeSpec = new ChangedInstanceOps();
  public model = new ChangedInstanceOps();
  public element = new ChangedInstanceOps();
  public aspect = new ChangedInstanceOps();
  public relationship = new ChangedInstanceOps();
  public font = new ChangedInstanceOps();
  private _codeSpecSubclassIds?: Set<string>;
  private _modelSubclassIds?: Set<string>;
  private _elementSubclassIds?: Set<string>;
  private _aspectSubclassIds?: Set<string>;
  private _relationshipSubclassIds?: Set<string>;
  private _relationshipSubclassIdsToSkip?: Set<string>;
  private readonly _aspectOwnerElementIds = new Set<Id64String>();

  /** Element IDs that own the aspects represented by `aspect` changes.
   * @internal
   */
  public get aspectOwnerElementIds(): ReadonlySet<Id64String> {
    return this._aspectOwnerElementIds;
  }

  private _db: IModelDb;
  public constructor(db: IModelDb) {
    this._db = db;
  }

  private async setupECClassIds(): Promise<void> {
    this._codeSpecSubclassIds = new Set<string>();
    this._modelSubclassIds = new Set<string>();
    this._elementSubclassIds = new Set<string>();
    this._aspectSubclassIds = new Set<string>();
    this._relationshipSubclassIds = new Set<string>();
    this._relationshipSubclassIdsToSkip = new Set<string>();

    const addECClassIdsToSet = async (
      setToModify: Set<string>,
      baseClass: string
    ) => {
      for await (const row of this._db.createQueryReader(
        `SELECT ECInstanceId FROM ECDbMeta.ECClassDef where ECInstanceId IS (${baseClass})`,
        undefined,
        { usePrimaryConn: true }
      )) {
        setToModify.add(row.ECInstanceId);
      }
    };
    const promises = [
      addECClassIdsToSet(this._codeSpecSubclassIds, "BisCore.CodeSpec"),
      addECClassIdsToSet(this._modelSubclassIds, "BisCore.Model"),
      addECClassIdsToSet(this._elementSubclassIds, "BisCore.Element"),
      addECClassIdsToSet(
        this._aspectSubclassIds,
        "BisCore.ElementUniqueAspect"
      ),
      addECClassIdsToSet(this._aspectSubclassIds, "BisCore.ElementMultiAspect"),
      addECClassIdsToSet(
        this._relationshipSubclassIds,
        "BisCore.ElementRefersToElements"
      ),
      addECClassIdsToSet(
        this._relationshipSubclassIdsToSkip,
        "BisCore.ElementDrivesElement"
      ),
    ];
    await Promise.all(promises);
  }

  private get _ecClassIdsInitialized() {
    return (
      this._codeSpecSubclassIds &&
      this._modelSubclassIds &&
      this._elementSubclassIds &&
      this._aspectSubclassIds &&
      this._relationshipSubclassIds &&
      this._relationshipSubclassIdsToSkip
    );
  }

  private isRelationship(ecClassId: string) {
    return this._relationshipSubclassIds?.has(ecClassId);
  }

  private isCodeSpec(ecClassId: string) {
    return this._codeSpecSubclassIds?.has(ecClassId);
  }

  private isAspect(ecClassId: string) {
    return this._aspectSubclassIds?.has(ecClassId);
  }

  private isModel(ecClassId: string) {
    return this._modelSubclassIds?.has(ecClassId);
  }

  private isElement(ecClassId: string) {
    return this._elementSubclassIds?.has(ecClassId);
  }

  /** Checks if there are any changes.
   * @returns true if there are any changes in the ChangedInstanceIds object.
   */
  public get hasChanges(): boolean {
    return (
      !this.codeSpec.isEmpty ||
      !this.model.isEmpty ||
      !this.element.isEmpty ||
      !this.aspect.isEmpty ||
      !this.relationship.isEmpty ||
      !this.font.isEmpty
    );
  }

  /**
   * Adds the provided [[ChangedECInstance]] to the appropriate set of changes by class type (codeSpec, model, element, aspect, or relationship) maintained by this instance of ChangedInstanceIds.
   * If the same ECInstanceId is seen multiple times, the changedInstanceIds will be modified accordingly, i.e. if an id 'x' was updated but now we see 'x' was deleted, we will remove 'x'
   * from the set of updatedIds and add it to the set of deletedIds for the appropriate class type.
   * @param change ChangedECInstance which has the ECInstanceId, changeType (insert, update, delete) and ECClassId of the changed entity
   */
  // eslint-disable-next-line @typescript-eslint/no-deprecated
  public async addChange(change: ChangedECInstance): Promise<void> {
    if (!this._ecClassIdsInitialized) await this.setupECClassIds();
    const ecClassId = change.ECClassId ?? change.$meta?.fallbackClassId;
    if (ecClassId === undefined)
      throw new Error(
        `ECClassId was not found for id: ${change.ECInstanceId}! Table is : ${change?.$meta?.tables}`
      );
    const changeType: SqliteChangeOp | undefined = change.$meta?.op;
    if (changeType === undefined)
      throw new Error(
        `ChangeType was undefined for id: ${change.ECInstanceId}.`
      );
    if (this._relationshipSubclassIdsToSkip?.has(ecClassId)) return;

    if (this.isRelationship(ecClassId))
      this.handleChange(this.relationship, changeType, change.ECInstanceId);
    else if (this.isCodeSpec(ecClassId))
      this.handleChange(this.codeSpec, changeType, change.ECInstanceId);
    else if (this.isAspect(ecClassId)) {
      const ownerElementId =
        change.Element?.Id ??
        this.tryGetAspectOwnerElementId(change.ECInstanceId);
      if (ownerElementId !== undefined) {
        this._aspectOwnerElementIds.add(ownerElementId);
      }
      this.handleChange(this.aspect, changeType, change.ECInstanceId);
    } else if (this.isModel(ecClassId))
      this.handleChange(this.model, changeType, change.ECInstanceId);
    else if (this.isElement(ecClassId))
      this.handleChange(this.element, changeType, change.ECInstanceId);
  }

  private tryGetAspectOwnerElementId(
    aspectId: Id64String
  ): Id64String | undefined {
    try {
      return this._db.elements.getAspect(aspectId).element.id;
    } catch (error) {
      if (
        error instanceof IModelError &&
        error.errorNumber === IModelStatus.NotFound
      ) {
        return undefined;
      }
      throw error;
    }
  }

  /**
   * This method should only be called inside [[IModelTransformer.addCustomChanges]].
   * It adds the provided change to the element changes maintained by this instance of ChangedInstanceIds.
   * If the same ECInstanceId is seen multiple times, the changedInstanceIds will be modified accordingly, i.e. if an id 'x' was updated but now we see 'x' was deleted, we will remove 'x'
   * from the set of updatedIds and add it to the set of deletedIds for the appropriate class type.
   * @note Custom element 'Insert' and 'Update' will mark element's parent model hierarchy and their modeled elements as 'Updated' in [[ChangedInstanceIds.model]] and [[ChangedInstanceIds.element]]. Parent models have to be marked as 'Updated' to make sure that added change is not skipped by transformer. Transformer starts processing elements from RepositoryModel and then visits all child models. Modeled elements hierarchy is marked as updated to trigger their inserts in case a new model (or its parent) needs to be inserted.
   * @note Custom element 'Insert' will also mark element aspects and all element relationships as inserted.
   * @note It is the responsibility of the caller to ensure that the provided id is, in fact an element.
   * @note In most cases, this method does not need to be called. Its only for consumers to mimic changes as if they were found in a changeset, which should only be useful in certain cases such as the changing of filter criteria for a preexisting master branch relationship.
   * @note In data processing with filter criteria scenarios it is important to consistently filter out models and their modeled elements that were previously removed from target via [[addCustomModelChange]] or [[shouldExportElement]] apis.
   * @beta
   */
  public async addCustomElementChange(
    changeType: SqliteChangeOp,
    ids: Id64Arg
  ): Promise<void> {
    if (Id64.sizeOf(ids) === 0) {
      return;
    }

    for (const id of Id64.iterable(ids)) {
      this.handleChange(this.element, changeType, id);
    }

    if (changeType === "Deleted") {
      return;
    }

    const idsSet = Id64.toIdSet(ids, true);
    // Parent models have to be marked as 'Updated' to make sure that added change is not skipped by transformer. Transformer starts processing elements from RepositoryModel and then visits all child models.
    // Transformer handles update as insert if element is not found in target, for this reason modeled elements will be also marked as updated to trigger their inserts in case a new model (or its parent) needs to be inserted. Otherwise error would be thrown about missing modeled element while inserting new model.
    const parentModelIds = await this.markParentModelsAsUpdated(idsSet);

    // Aspects and relationships of inserted data needs to be marked as inserted otherwise those would not be exported
    if (changeType === "Inserted") {
      // Adding parents as well as we are not sure if those were inserted or updated
      parentModelIds.forEach((parentId) => {
        idsSet.add(parentId);
      });

      await this.markElementAspectsAsInserted(idsSet);
      // Marking only ElementRefersToElements.classFullName as only those are exported in exportRelationships()
      await this.markElementRelationshipsAsInserted(
        ElementRefersToElements.classFullName,
        idsSet
      );
    }
  }

  /**
   * This method should only be called inside [IModelTransformer.addCustomChanges].
   * Adds the provided change to the model changes maintained by this instance of ChangedInstanceIds.
   * If the same ECInstanceId is seen multiple times, the changedInstanceIds will be modified accordingly, i.e. if an id 'x' was updated but now we see 'x' was deleted, we will remove 'x'
   * from the set of updatedIds and add it to the set of deletedIds for the appropriate class type.
   * Will add same change to the model's modeledElement by calling [[ChangedInstanceIds.addCustomElementChange]] which will register more needed changes. This is to ensure the changes from the model and its modeledElement get exported together.
   * @note It is the responsibility of the caller to ensure that the provided id is, in fact a model.
   * @note In most cases, this method does not need to be called. Its only for consumers to mimic changes as if they were found in a changeset, which should only be useful in certain cases such as the changing of filter criteria for a preexisting master branch relationship.
   * @note In data processing with filter criteria scenarios it is important to consistently filter out models and their modeled elements that were previously removed from target via [[addCustomModelChange]] or [[shouldExportElement]] apis.
   * @beta
   */
  public async addCustomModelChange(
    changeType: SqliteChangeOp,
    ids: Id64Arg
  ): Promise<void> {
    // Also add the model's modeledElement to the element changes. The modeledElement and model go hand in hand and have the same id.
    await this.addCustomElementChange(changeType, ids);
    for (const id of Id64.iterable(ids)) {
      this.handleChange(this.model, changeType, id);
    }
  }

  /**
   * This method should only be called inside [IModelTransformer.addCustomChanges].
   * Adds the provided change to the aspect changes maintained by this instance of ChangedInstanceIds
   * If the same ECInstanceId is seen multiple times, the changedInstanceIds will be modified accordingly, i.e. if an id 'x' was updated but now we see 'x' was deleted, we will remove 'x'
   * from the set of updatedIds and add it to the set of deletedIds for the appropriate class type.
   * @note It is the responsibility of the caller to ensure that the provided id is, in fact an aspect.
   * @param elementIds Owning element IDs. Required when `changeType` is
   *        `Deleted` or the source aspect row is unavailable.
   * @note In most cases, this method does not need to be called. Its only for consumers to mimic changes as if they were found in a changeset, which should only be useful in certain cases such as the changing of filter criteria for a preexisting master branch relationship.
   * @beta
   */
  public addCustomAspectChange(
    changeType: SqliteChangeOp,
    ids: Id64Arg,
    elementIds?: Id64Arg
  ): void {
    if (elementIds !== undefined) {
      for (const elementId of Id64.iterable(elementIds)) {
        this._aspectOwnerElementIds.add(elementId);
      }
    }
    for (const id of Id64.iterable(ids)) {
      if (elementIds === undefined && changeType === "Deleted") {
        ITwinError.throwError({
          iTwinErrorId: {
            scope: "@itwin/imodel-transformer",
            key: "missing-aspect-owner",
          },
          message:
            "Custom deleted ElementAspect changes require the owning element ID.",
        });
      }
      if (elementIds === undefined && changeType !== "Deleted") {
        const ownerElementId = this.tryGetAspectOwnerElementId(id);
        if (ownerElementId === undefined) {
          ITwinError.throwError({
            iTwinErrorId: {
              scope: "@itwin/imodel-transformer",
              key: "missing-aspect-owner",
            },
            message:
              "Custom ElementAspect changes require the owning element ID when the source aspect is unavailable.",
          });
        }
        this._aspectOwnerElementIds.add(ownerElementId);
      }

      this.handleChange(this.aspect, changeType, id);
    }
  }

  private recordCustomAspectChange(
    changeType: SqliteChangeOp,
    aspectId: Id64String,
    ownerElementId: Id64String
  ): void {
    this._aspectOwnerElementIds.add(ownerElementId);
    this.handleChange(this.aspect, changeType, aspectId);
  }

  /**
   * There is an optimization in [IModelExporter.exportModelContents] which doesn't try to export elements within a model unless the model itself is marked as `Updated` or 'Inserted' in sourceDbChanges. This method is used in [[addCustomElementChange]] and [[addCustomModelChange]] to add the parent model hierarchy to the 'updatedIds' so that the custom element changes are exported.
   * Transformer will insert 'Updated' model to target if it does not exist there already. To handle such case, modeled elements of parent models are also marked as updated. This is done, because model can not be inserted without it's modeled element.
   */
  private async markParentModelsAsUpdated(elementIds: Id64Set) {
    const params = new QueryBinder().bindIdSet("elementIds", elementIds);

    const ecQuery = `
    WITH RECURSIVE hierarchy (parentId) AS (
        SELECT Model.Id FROM bis.Element WHERE InVirtualSet(:elementIds, ECInstanceId)
        UNION
        SELECT ParentModel.id
        FROM bis.Model e
            INNER JOIN hierarchy h ON h.parentId = e.ECInstanceId
        )
        SELECT parentId FROM hierarchy where parentId is not null
    `;
    const parentModelIds = new Set<Id64String>();
    for await (const row of this._db.createQueryReader(ecQuery, params, {
      usePrimaryConn: true,
    })) {
      // Transformer handles update as insert when element does not exist in target.
      // Which means that in scenario where child and parent model are filtered out from target,
      // and child element is inserted trough custom change, its parent model will be marked as updated.
      // Transformer then will:
      //  1. Handle parent update as insert (since it does not exist in target).
      //  2. Will insert child element (otherwise this insert would be ignored due to missing parent).
      this.handleChange(this.model, "Updated", row.parentId);
      this.handleChange(this.element, "Updated", row.parentId);
      parentModelIds.add(row.parentId);
    }
    return parentModelIds;
  }

  private async markElementRelationshipsAsInserted(
    relationshipClassName: string,
    elementIds: Id64Set
  ) {
    const ecQuery = `SELECT ECInstanceId FROM ${relationshipClassName}
        WHERE InVirtualSet(:elementIds, TargetECInstanceId)
        OR InVirtualSet(:elementIds, SourceECInstanceId)`;

    const queryBinder = new QueryBinder().bindIdSet("elementIds", elementIds);
    const queryReader = this._db.createQueryReader(ecQuery, queryBinder, {
      usePrimaryConn: true,
    });

    for await (const row of queryReader) {
      this.handleChange(this.relationship, "Inserted", row.ECInstanceId);
    }
  }

  private async markElementAspectsAsInserted(elementIds: Id64Set) {
    for (const aspectClassName of [
      ElementUniqueAspect.classFullName,
      ElementMultiAspect.classFullName,
    ]) {
      const ecQuery = `Select ECInstanceId, Element.Id from ${aspectClassName} where InVirtualSet(:elementIds, Element.Id)`;
      const queryBinder = new QueryBinder().bindIdSet("elementIds", elementIds);
      const queryReader = this._db.createQueryReader(ecQuery, queryBinder, {
        usePrimaryConn: true,
      });
      for await (const row of queryReader) {
        const [aspectId, elementId] = row.toArray();
        this.recordCustomAspectChange("Inserted", aspectId, elementId);
      }
    }
  }

  private handleChange(
    changedInstanceOps: ChangedInstanceOps,
    changeType: SqliteChangeOp,
    id: Id64String
  ) {
    // if changeType is a delete and we already have the id in the inserts then we can remove the id from the inserts.
    // if changeType is a delete and we already have the id in the updates then we can remove the id from the updates AND add it to the deletes.
    // if changeType is an insert and we already have the id in the deletes then we can remove the id from the deletes AND add it to the inserts.
    if (changeType === "Inserted") {
      changedInstanceOps.insertIds.add(id);
      changedInstanceOps.deleteIds.delete(id);
    } else if (changeType === "Updated") {
      if (!changedInstanceOps.insertIds.has(id))
        changedInstanceOps.updateIds.add(id);
    } else if (changeType === "Deleted") {
      // If we've inserted the entity at some point already and now we're seeing a delete. We can simply remove the entity from our inserted ids without adding it to deletedIds.
      if (changedInstanceOps.insertIds.has(id))
        changedInstanceOps.insertIds.delete(id);
      else {
        changedInstanceOps.updateIds.delete(id);
        changedInstanceOps.deleteIds.add(id);
      }
    }
  }

  /**
   * Initializes a new ChangedInstanceIds object with information taken from a range of changesets.
   * @public
   */
  public static async initialize(
    opts: ChangedInstanceIdsInitOptions
  ): Promise<ChangedInstanceIds | undefined> {
    if ("changedInstanceIds" in opts) return opts.changedInstanceIds;

    const iModelId = opts.iModel.iModelId;

    const startChangeset =
      "startChangeset" in opts ? opts.startChangeset : undefined;
    const changesetRanges =
      startChangeset !== undefined
        ? [
            [
              startChangeset.index ??
                (
                  await BriefcaseManager.queryChangeset({
                    iModelId,
                    changeset: {
                      id: startChangeset.id ?? opts.iModel.changeset.id,
                    },
                  })
                ).index,
              opts.iModel.changeset.index ??
                (
                  await BriefcaseManager.queryChangeset({
                    iModelId,
                    changeset: { id: opts.iModel.changeset.id },
                  })
                ).index,
            ],
          ]
        : "changesetRanges" in opts
          ? opts.changesetRanges
          : undefined;
    const csFileProps =
      changesetRanges !== undefined
        ? (
            await Promise.all(
              changesetRanges.map(async ([first, end]) =>
                BriefcaseManager.downloadChangesets({
                  iModelId,
                  range: { first, end },
                  targetDir: BriefcaseManager.getChangeSetsPath(iModelId),
                })
              )
            )
          ).flat()
        : "csFileProps" in opts
          ? opts.csFileProps
          : undefined;

    if (csFileProps === undefined) return undefined;

    const changedInstanceIds = new ChangedInstanceIds(opts.iModel);

    for (const csFile of csFileProps) {
      const csReader = SqliteChangesetReader.openFile({
        fileName: csFile.pathname,
        db: opts.iModel,
        disableSchemaCheck: true,
      });
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      const csAdaptor = new ChangesetECAdaptor(csReader);
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      const ecChangeUnifier = new PartialECChangeUnifier(opts.iModel);
      while (csAdaptor.step()) {
        ecChangeUnifier.appendFrom(csAdaptor);
      }
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      const changes: ChangedECInstance[] = [...ecChangeUnifier.instances];

      for (const change of changes) {
        // Change is recorded at table level, not EC entity level.
        // This `change.$meta.op` operation overwrite is needed to properly handle scenario when:
        // 1. Source has an EC class with less than 32 properties. There are existing elements for that class.
        // 2. Class is then updated to have more than 32 properties. Which means overflow table is now needed to store its elements.
        //  During schema update all elements that belong to updated class, will be expanded into overflow table.
        // 3. Changeset will have a record about `insert` operation into overflow table for already existing elements.
        // This fix will overwrite such 'insert' and 'delete' operations to 'update' as no changes are done to main table.
        // It ensures that changes will be processed and squashed correctly.
        if (
          change.$meta &&
          (change.$meta.op === "Inserted" || change.$meta.op === "Deleted") &&
          change.$meta.tables.every((e) => e.endsWith("Overflow"))
        ) {
          change.$meta.op = "Updated";
        }
        await changedInstanceIds.addChange(change);
      }
      csReader.close();
    }
    return changedInstanceIds;
  }
}
