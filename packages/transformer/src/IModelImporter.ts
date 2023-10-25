/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import { CompressedId64Set, Id64, Id64String, IModelStatus, Logger } from "@itwin/core-bentley";
import {
  AxisAlignedBox3d, Base64EncodedString, ElementAspectProps, ElementProps, EntityProps, IModel, IModelError, ModelProps, PrimitiveTypeCode,
  PropertyMetaData, RelatedElement, SubCategoryProps,
} from "@itwin/core-common";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { ElementAspect, ElementMultiAspect, Entity, IModelDb, Relationship, RelationshipProps, SourceAndTarget, SubCategory } from "@itwin/core-backend";
import type { IModelTransformOptions } from "./IModelTransformer";
import * as assert from "assert";
import { deleteElementTreeCascade } from "./ElementCascadingDeleter";

const loggerCategory: string = TransformerLoggerCategory.IModelImporter;

/** Options provided to [[IModelImporter.optimizeGeometry]] specifying post-processing optimizations to be applied to the iModel's geometry.
 * @beta
 */
export interface OptimizeGeometryOptions {
  /** If true, identify any [GeometryPart]($backend)s that are referenced exactly once. For each such part,
   * replace the reference in the element's geometry stream with the part's own geometry stream, then delete the part.
   */
  inlineUniqueGeometryParts?: boolean;
}

/** Options provided to the [[IModelImporter]] constructor.
 * @beta
 */
export interface IModelImportOptions {
  /** If `true` (the default), compute the projectExtents of the target iModel after elements are imported.
   * The computed projectExtents will either include or exclude *outliers* depending on the `excludeOutliers` flag that defaults to `false`.
   * @see [[IModelImporter.autoExtendProjectExtents]]
   * @see [IModelImporter Options]($docs/learning/transformer/index.md#IModelImporter)
   */
  autoExtendProjectExtents?: boolean | { excludeOutliers: boolean };
  /** See [IModelTransformOptions]($transformer) */
  preserveElementIdsForFiltering?: boolean;
  /** If `true`, simplify the element geometry for visualization purposes. For example, convert b-reps into meshes.
   * @default false
   */
  simplifyElementGeometry?: boolean;
  /** If 'true', try to handle conflicting code value changes by using temporary NULL values.
   * Code values can be resolved by calling [[IModelImporter.resolveDuplicateCodeValues]].
   * @default false
   */
  handleElementCodeDuplicates?: boolean;
}

/** Base class for importing data into an iModel.
 * @see [iModel Transformation and Data Exchange]($docs/learning/transformer/index.md)
 * @see [IModelExporter]($transformer)
 * @see [IModelTransformer]($transformer)
 * @beta
 */
export class IModelImporter implements Required<IModelImportOptions> {
  /** The read/write target iModel. */
  public readonly targetDb: IModelDb;

  /** resolved initialization options for the importer
   * @beta
   */
  public readonly options: Required<IModelImportOptions>;

  /** If `true` (the default), compute the projectExtents of the target iModel after elements are imported.
   * The computed projectExtents will either include or exclude *outliers* depending on the `excludeOutliers` flag that defaults to `false`.
   * @see [[IModelImportOptions.autoExtendProjectExtents]]
   * @see [IModelImporter Options]($docs/learning/transformer/index.md#IModelImporter)
   * @deprecated in 3.x. Use [[IModelImporter.options.autoExtendProjectExtents]] instead
   */
  public get autoExtendProjectExtents(): Required<IModelImportOptions>["autoExtendProjectExtents"] {
    return this.options.autoExtendProjectExtents;
  }
  public set autoExtendProjectExtents(val: Required<IModelImportOptions>["autoExtendProjectExtents"]) {
    this.options.autoExtendProjectExtents = val;
  }

  /**
   * See [IModelTransformOptions.preserveElementIdsForFiltering]($transformer)
   * @deprecated in 3.x. Use [[IModelImporter.options.preserveElementIdsForFiltering]] instead
   */
  public get preserveElementIdsForFiltering(): boolean {
    return this.options.preserveElementIdsForFiltering;
  }
  public set preserveElementIdsForFiltering(val: boolean) {
    this.options.preserveElementIdsForFiltering = val;
  }

  /**
   * See [[IModelImportOptions.simplifyElementGeometry]]
   * @deprecated in 3.x. Use [[IModelImporter.options.simplifyElementGeometry]] instead
   */
  public get simplifyElementGeometry(): boolean {
    return this.options.simplifyElementGeometry;
  }
  public set simplifyElementGeometry(val: boolean) {
    this.options.simplifyElementGeometry = val;
  }

  private static _realityDataSourceLinkPartitionStaticId: Id64String = "0xe";

  private _duplicateCodeValueMap?: Map<Id64String, string>;

  /**
   * A map of conflicting element code values.
   * In cases where updating an element's code value results in a codeValue conflict, the element's code value will instead be updated to 'NULL'.
   * The actual codeValue will be stored in this ElementId->CodeValue map.
   * This is needed cases where the duplicate target element is set to be deleted in the future, or when elements are switching code values with one another and we need a temporary code value.
   * To resolve code values to their intended values call [[IModelImporter.resolveDuplicateCodeValues]].
   *
   * @returns undefined if no duplicates were detected.
   */
  public get duplicateCodeValueMap(): Map<Id64String, string> | undefined {
    return this._duplicateCodeValueMap;
  }

  public get handleElementCodeDuplicates(): boolean {
    return this.options.handleElementCodeDuplicates;
  }
  public set handleElementCodeDuplicates(val: boolean) {
    this.options.handleElementCodeDuplicates = val;
  }

  /** The set of elements that should not be updated by this IModelImporter.
   * Defaults to the elements that are always present (even in an "empty" iModel) and therefore do not need to be updated
   * @note Adding an element to this set is typically necessary when remapping a source element to one that already exists in the target and already has the desired properties.
   */
  public readonly doNotUpdateElementIds = new Set<Id64String>([
    IModel.rootSubjectId,
    IModel.dictionaryId,
    IModelImporter._realityDataSourceLinkPartitionStaticId,
  ]);
  /** The number of entity changes before incremental progress should be reported via the [[onProgress]] callback. */
  public progressInterval: number = 1000;
  /** Tracks the current total number of entity changes. */
  private _progressCounter: number = 0;
  /** */
  private _modelPropertiesToIgnore = new Set<string>([
    "geometryGuid", // cannot compare GeometricModel.GeometryGuid values across iModels
  ]);

  /** Construct a new IModelImporter
   * @param targetDb The target IModelDb
   * @param options The options that specify how the import should be done.
   */
  public constructor(targetDb: IModelDb, options?: IModelImportOptions) {
    this.targetDb = targetDb;
    this.options = {
      autoExtendProjectExtents: options?.autoExtendProjectExtents ?? true,
      preserveElementIdsForFiltering: options?.preserveElementIdsForFiltering ?? false,
      simplifyElementGeometry: options?.simplifyElementGeometry ?? false,
      handleElementCodeDuplicates: options?.handleElementCodeDuplicates ?? false,
    };
  }

  /** Import the specified ModelProps (either as an insert or an update) into the target iModel. */
  public importModel(modelProps: ModelProps): void {
    if ((undefined === modelProps.id) || !Id64.isValidId64(modelProps.id))
      throw new IModelError(IModelStatus.InvalidId, "Model Id not provided, should be the same as the ModeledElementId");

    if (this.doNotUpdateElementIds.has(modelProps.id)) {
      Logger.logInfo(loggerCategory, `Do not update target model ${modelProps.id}`);
      return;
    }
    try {
      const model = this.targetDb.models.getModel(modelProps.id); // throws IModelError.NotFound if model does not exist
      if (hasEntityChanged(model, modelProps, this._modelPropertiesToIgnore)) {
        this.onUpdateModel(modelProps);
      }
    } catch (error) {
      // catch NotFound error and insertModel
      if (error instanceof IModelError && error.errorNumber === IModelStatus.NotFound) {
        this.onInsertModel(modelProps);
        return;
      }
      throw error;
    }
  }

  /** Create a new Model from the specified ModelProps and insert it into the target iModel.
   * @note A subclass may override this method to customize insert behavior but should call `super.onInsertModel`.
   */
  protected onInsertModel(modelProps: ModelProps): Id64String {
    try {
      const modelId: Id64String = this.targetDb.models.insertModel(modelProps);
      Logger.logInfo(loggerCategory, `Inserted ${this.formatModelForLogger(modelProps)}`);
      this.trackProgress();
      return modelId;
    } catch (error) {
      if (!this.targetDb.containsClass(modelProps.classFullName)) {
        // replace standard insert error with something more helpful
        const errorMessage = `Model class "${modelProps.classFullName}" not found in the target iModel. Was the latest version of the schema imported?`;
        throw new IModelError(IModelStatus.InvalidName, errorMessage);
      }
      throw error; // throw original error
    }
  }

  /** Update an existing Model in the target iModel from the specified ModelProps.
   * @note A subclass may override this method to customize update behavior but should call `super.onUpdateModel`.
   */
  protected onUpdateModel(modelProps: ModelProps): void {
    this.targetDb.models.updateModel(modelProps);
    Logger.logInfo(loggerCategory, `Updated ${this.formatModelForLogger(modelProps)}`);
    this.trackProgress();
  }

  /** Format a Model for the Logger. */
  private formatModelForLogger(modelProps: ModelProps): string {
    return `${modelProps.classFullName} [${modelProps.id!}]`;
  }

  /** Import the specified ElementProps (either as an insert or an update) into the target iModel. */
  public importElement(elementProps: ElementProps): Id64String {
    if (undefined !== elementProps.id && this.doNotUpdateElementIds.has(elementProps.id)) {
      Logger.logInfo(loggerCategory, `Do not update target element ${elementProps.id}`);
      return elementProps.id;
    }
    if (this.options.preserveElementIdsForFiltering) {
      if (elementProps.id === undefined) {
        throw new IModelError(IModelStatus.BadElement, `elementProps.id must be defined during a preserveIds operation`);
      }
      // Categories are the only element that onInserted will immediately insert a new element (their default subcategory)
      // since default subcategories always exist and always will be inserted after their categories, we treat them as an update
      // to prevent duplicate inserts.
      // Otherwise we always insert during a preserveElementIdsForFiltering operation
      if (isSubCategory(elementProps) && isDefaultSubCategory(elementProps)) {
        this.onUpdateElement(elementProps);
      } else {
        this.onInsertElement(elementProps);
      }
    } else {
      if (undefined !== elementProps.id) {
        try {
          this.onUpdateElement(elementProps);
        } catch(err) {
          if (this.handleElementCodeDuplicates && (err as IModelError).errorNumber === IModelStatus.DuplicateCode) {
            assert(elementProps.code.value !== undefined, "NULL code values are always considered unique and cannot clash");
            this.trackCodeValueDuplicate(elementProps.id, elementProps.code.value);
            elementProps.code.value = undefined;
            this.onUpdateElement(elementProps);
          }
        }
      } else {
        this.onInsertElement(elementProps); // targetElementProps.id assigned by insertElement
      }
    }
    return elementProps.id!;
  }

  /** Create a new Element from the specified ElementProps and insert it into the target iModel.
   * @returns The Id of the newly inserted Element.
   * @note A subclass may override this method to customize insert behavior but should call `super.onInsertElement`.
   */
  protected onInsertElement(elementProps: ElementProps): Id64String {
    try {
      const elementId = this.targetDb.nativeDb.insertElement(
        elementProps,
        { forceUseId: this.options.preserveElementIdsForFiltering },
      );
      // set the id like [IModelDb.insertElement]($backend), does, the raw nativeDb method does not
      elementProps.id = elementId;
      Logger.logInfo(loggerCategory, `Inserted ${this.formatElementForLogger(elementProps)}`);
      this.trackProgress();
      if (this.options.simplifyElementGeometry) {
        this.targetDb.nativeDb.simplifyElementGeometry({ id: elementId, convertBReps: true });
        Logger.logInfo(loggerCategory, `Simplified element geometry for ${this.formatElementForLogger(elementProps)}`);
      }
      return elementId;
    } catch (error) {
      if (!this.targetDb.containsClass(elementProps.classFullName)) {
        // replace standard insert error with something more helpful
        const errorMessage = `Element class "${elementProps.classFullName}" not found in the target iModel. Was the latest version of the schema imported?`;
        throw new IModelError(IModelStatus.InvalidName, errorMessage);
      }
      throw error; // throw original error
    }
  }

  /** Update an existing Element in the target iModel from the specified ElementProps.
   * @note A subclass may override this method to customize update behavior but should call `super.onUpdateElement`.
   */
  protected onUpdateElement(elementProps: ElementProps): void {
    if (!elementProps.id) {
      throw new IModelError(IModelStatus.InvalidId, "ElementId not provided");
    }
    this.targetDb.elements.updateElement(elementProps);
    Logger.logInfo(loggerCategory, `Updated ${this.formatElementForLogger(elementProps)}`);
    this.trackProgress();
    if (this.options.simplifyElementGeometry) {
      this.targetDb.nativeDb.simplifyElementGeometry({ id: elementProps.id, convertBReps: true });
      Logger.logInfo(loggerCategory, `Simplified element geometry for ${this.formatElementForLogger(elementProps)}`);
    }
  }

  /** Delete the specified Element (and all its children) from the target iModel.
   * Will delete special elements like definition elements and subjects.
   * @note A subclass may override this method to customize delete behavior but should call `super.onDeleteElement`.
   */
  protected onDeleteElement(elementId: Id64String): void {
    deleteElementTreeCascade(this.targetDb, elementId);
    Logger.logInfo(loggerCategory, `Deleted element ${elementId} and its descendants`);
    this.trackProgress();
  }

  /** Delete the specified Element from the target iModel. */
  public deleteElement(elementId: Id64String): void {
    if (this.doNotUpdateElementIds.has(elementId)) {
      Logger.logInfo(loggerCategory, `Do not delete target element ${elementId}`);
      return;
    }
    this.onDeleteElement(elementId);
  }

  /** Delete the specified Model from the target iModel.
   * @note A subclass may override this method to customize delete behavior but should call `super.onDeleteModel`.
   */
  protected onDeleteModel(modelId: Id64String): void {
    this.targetDb.models.deleteModel(modelId);
    Logger.logInfo(loggerCategory, `Deleted model ${modelId}`);
    this.trackProgress();
  }

  /** Delete the specified Model from the target iModel. */
  public deleteModel(modelId: Id64String): void {
    this.onDeleteModel(modelId);
  }

  /** Format an Element for the Logger. */
  private formatElementForLogger(elementProps: ElementProps): string {
    const namePiece: string = elementProps.code.value ? `${elementProps.code.value} ` : elementProps.userLabel ? `${elementProps.userLabel} ` : "";
    return `${elementProps.classFullName} ${namePiece}[${elementProps.id}]`;
  }

  /** Import an ElementUniqueAspect into the target iModel. */
  public importElementUniqueAspect(aspectProps: ElementAspectProps): Id64String {
    const aspects: ElementAspect[] = this.targetDb.elements.getAspects(aspectProps.element.id, aspectProps.classFullName);
    if (aspects.length === 0) {
      return this.onInsertElementAspect(aspectProps);
    } else if (hasEntityChanged(aspects[0], aspectProps)) {
      aspectProps.id = aspects[0].id;
      this.onUpdateElementAspect(aspectProps);
    }
    return aspects[0].id;
  }

  /** Import the collection of ElementMultiAspects into the target iModel.
   * @param aspectPropsArray The ElementMultiAspects to import
   * @param filterFunc Optional filter func that is used to exclude target ElementMultiAspects that were added during iModel transformation from the update detection logic.
   * @note For insert vs. update reasons, it is important to process all ElementMultiAspects owned by an Element at once since we don't have aspect-specific provenance.
   * @returns the array of ids of the resulting ElementMultiAspects, in the same order of the aspectPropsArray parameter
   */
  public importElementMultiAspects(
    aspectPropsArray: ElementAspectProps[],
    /** caller must use this to enforce any aspects added by IModelTransformer are not considered for update */
    filterFunc: (a: ElementMultiAspect) => boolean = () => true
  ): Id64String[] {
    const result = new Array<Id64String | undefined>(aspectPropsArray.length).fill(undefined);

    if (aspectPropsArray.length === 0) {
      return result as Id64String[];
    }

    const elementId: Id64String = aspectPropsArray[0].element.id;
    // Determine the set of ElementMultiAspect classes to consider
    const aspectClassFullNames = new Set<string>();
    aspectPropsArray.forEach((aspectsProps: ElementAspectProps): void => {
      aspectClassFullNames.add(aspectsProps.classFullName);
    });

    // Handle ElementMultiAspects in groups by class
    aspectClassFullNames.forEach((aspectClassFullName: string) => {
      const proposedAspects = aspectPropsArray
        .map((props, index) => ({ props, index }))
        .filter(({props}) => aspectClassFullName === props.classFullName);

      const currentAspects = this.targetDb.elements
        .getAspects(elementId, aspectClassFullName)
        .map((props, index) => ({ props, index }) as const)
        .filter(({props}) => filterFunc(props));

      if (proposedAspects.length >= currentAspects.length) {
        proposedAspects.forEach(({props, index: resultIndex}, index) => {
          let id: Id64String;
          if (index < currentAspects.length) {
            id = currentAspects[index].props.id;
            props.id = id;
            if (hasEntityChanged(currentAspects[index].props, props)) {
              this.onUpdateElementAspect(props);
            }
            id = props.id;
          } else {
            id = this.onInsertElementAspect(props);
          }
          result[resultIndex] = id;
        });
      } else {
        currentAspects.forEach(({props, index: resultIndex}, index) => {
          let id: Id64String;
          if (index < proposedAspects.length) {
            id = props.id;
            proposedAspects[index].props.id = id;
            if (hasEntityChanged(props, proposedAspects[index].props)) {
              this.onUpdateElementAspect(proposedAspects[index].props);
            }
            result[resultIndex] = id;
          } else {
            this.onDeleteElementAspect(props);
          }
        });
      }
    });

    assert(result.every((r) => typeof r !== undefined));
    return result as Id64String[];
  }

  /** Insert the ElementAspect into the target iModel.
   * @note A subclass may override this method to customize insert behavior but should call `super.onInsertElementAspect`.
   */
  protected onInsertElementAspect(aspectProps: ElementAspectProps): Id64String {
    try {
      const id = this.targetDb.elements.insertAspect(aspectProps);
      Logger.logInfo(loggerCategory, `Inserted ${this.formatElementAspectForLogger(aspectProps)}`);
      this.trackProgress();
      return id;
    } catch (error) {
      if (!this.targetDb.containsClass(aspectProps.classFullName)) {
        // replace standard insert error with something more helpful
        const errorMessage = `ElementAspect class "${aspectProps.classFullName}" not found in the target iModel. Was the latest version of the schema imported?`;
        throw new IModelError(IModelStatus.InvalidName, errorMessage);
      }
      throw error; // throw original error
    }
  }

  /** Update the ElementAspect within the target iModel.
   * @note A subclass may override this method to customize update behavior but should call `super.onUpdateElementAspect`.
   */
  protected onUpdateElementAspect(aspectProps: ElementAspectProps): void {
    this.targetDb.elements.updateAspect(aspectProps);
    Logger.logInfo(loggerCategory, `Updated ${this.formatElementAspectForLogger(aspectProps)}`);
    this.trackProgress();
  }

  /** Delete the specified ElementAspect from the target iModel.
   * @note A subclass may override this method to customize delete behavior but should call `super.onDeleteElementAspect`.
   */
  protected onDeleteElementAspect(targetElementAspect: ElementAspect): void {
    this.targetDb.elements.deleteAspect(targetElementAspect.id);
    Logger.logInfo(loggerCategory, `Deleted ${this.formatElementAspectForLogger(targetElementAspect)}`);
    this.trackProgress();
  }

  /** Format an ElementAspect for the Logger. */
  private formatElementAspectForLogger(elementAspectProps: ElementAspectProps | ElementAspect): string {
    return `${elementAspectProps.classFullName} elementId=[${elementAspectProps.element.id}]`;
  }

  /** Import the specified RelationshipProps (either as an insert or an update) into the target iModel.
   * @returns The instance Id of the inserted or updated Relationship.
   */
  public importRelationship(relationshipProps: RelationshipProps): Id64String {
    if ((undefined === relationshipProps.sourceId) || !Id64.isValidId64(relationshipProps.sourceId)) {
      Logger.logInfo(loggerCategory, `Ignoring ${relationshipProps.classFullName} instance because of invalid RelationshipProps.sourceId`);
      return Id64.invalid;
    }
    if ((undefined === relationshipProps.targetId) || !Id64.isValidId64(relationshipProps.targetId)) {
      Logger.logInfo(loggerCategory, `Ignoring ${relationshipProps.classFullName} instance because of invalid RelationshipProps.targetId`);
      return Id64.invalid;
    }
    // check for an existing relationship
    const relSourceAndTarget: SourceAndTarget = { sourceId: relationshipProps.sourceId, targetId: relationshipProps.targetId };
    const relationship: Relationship | undefined = this.targetDb.relationships.tryGetInstance(relationshipProps.classFullName, relSourceAndTarget);
    if (undefined !== relationship) { // if relationship found, update it
      relationshipProps.id = relationship.id;
      if (hasEntityChanged(relationship, relationshipProps)) {
        this.onUpdateRelationship(relationshipProps);
      }
      return relationshipProps.id;
    } else {
      return this.onInsertRelationship(relationshipProps);
    }
  }

  /** Create a new Relationship from the specified RelationshipProps and insert it into the target iModel.
   * @returns The instance Id of the newly inserted relationship.
   * @note A subclass may override this method to customize insert behavior but should call `super.onInsertRelationship`.
   */
  protected onInsertRelationship(relationshipProps: RelationshipProps): Id64String {
    try {
      const targetRelInstanceId: Id64String = this.targetDb.relationships.insertInstance(relationshipProps);
      Logger.logInfo(loggerCategory, `Inserted ${this.formatRelationshipForLogger(relationshipProps)}`);
      this.trackProgress();
      return targetRelInstanceId;
    } catch (error) {
      if (!this.targetDb.containsClass(relationshipProps.classFullName)) {
        // replace standard insert error with something more helpful
        const errorMessage = `Relationship class "${relationshipProps.classFullName}" not found in the target iModel. Was the latest version of the schema imported?`;
        throw new IModelError(IModelStatus.InvalidName, errorMessage);
      }
      throw error; // throw original error
    }
  }

  /** Update an existing Relationship in the target iModel from the specified RelationshipProps.
   * @note A subclass may override this method to customize update behavior but should call `super.onUpdateRelationship`.
   */
  protected onUpdateRelationship(relationshipProps: RelationshipProps): void {
    if (!relationshipProps.id) {
      throw new IModelError(IModelStatus.InvalidId, "Relationship instance Id not provided");
    }
    this.targetDb.relationships.updateInstance(relationshipProps);
    Logger.logInfo(loggerCategory, `Updated ${this.formatRelationshipForLogger(relationshipProps)}`);
    this.trackProgress();
  }

  /** Delete the specified Relationship from the target iModel. */
  protected onDeleteRelationship(relationshipProps: RelationshipProps): void {
    this.targetDb.relationships.deleteInstance(relationshipProps);
    Logger.logInfo(loggerCategory, `Deleted relationship ${this.formatRelationshipForLogger(relationshipProps)}`);
    this.trackProgress();
  }

  /** Delete the specified Relationship from the target iModel. */
  public deleteRelationship(relationshipProps: RelationshipProps): void {
    this.onDeleteRelationship(relationshipProps);
  }

  /** Format a Relationship for the Logger. */
  private formatRelationshipForLogger(relProps: RelationshipProps): string {
    return `${relProps.classFullName} sourceId=[${relProps.sourceId}] targetId=[${relProps.targetId}]`;
  }

  /** Tracks incremental progress */
  private trackProgress(): void {
    this._progressCounter++;
    if (0 === (this._progressCounter % this.progressInterval)) {
      this.onProgress();
    }
  }

  /** This method is called when IModelImporter has made incremental progress based on the [[progressInterval]] setting.
   * @note A subclass may override this method to report custom progress but should call `super.onProgress`.
   */
  protected onProgress(): void { }

  /** Optionally compute the projectExtents for the target iModel depending on the options for this IModelImporter.
   * @note This method is automatically called from [IModelTransformer.processChanges]($transformer) and [IModelTransformer.processAll]($transformer).
   * @see [IModelDb.computeProjectExtents]($backend), [[autoExtendProjectExtents]]
   */
  public computeProjectExtents(): void {
    const computedProjectExtents = this.targetDb.computeProjectExtents({ reportExtentsWithOutliers: true, reportOutliers: true });
    Logger.logInfo(loggerCategory, `Current projectExtents=${JSON.stringify(this.targetDb.projectExtents)}`);
    Logger.logInfo(loggerCategory, `Computed projectExtents without outliers=${JSON.stringify(computedProjectExtents.extents)}`);
    Logger.logInfo(loggerCategory, `Computed projectExtents with outliers=${JSON.stringify(computedProjectExtents.extentsWithOutliers)}`);
    if (this.options.autoExtendProjectExtents) {
      const excludeOutliers: boolean = typeof this.options.autoExtendProjectExtents === "object" ? this.options.autoExtendProjectExtents.excludeOutliers : false;
      const newProjectExtents: AxisAlignedBox3d = excludeOutliers ? computedProjectExtents.extents : computedProjectExtents.extentsWithOutliers!;
      if (!newProjectExtents.isAlmostEqual(this.targetDb.projectExtents)) {
        this.targetDb.updateProjectExtents(newProjectExtents);
        Logger.logInfo(loggerCategory, `Updated projectExtents=${JSON.stringify(this.targetDb.projectExtents)}`);
      }
      if (!excludeOutliers && computedProjectExtents.outliers && computedProjectExtents.outliers.length > 0) {
        Logger.logInfo(loggerCategory, `${computedProjectExtents.outliers.length} outliers detected within projectExtents`);
      }
    } else {
      if (!this.targetDb.projectExtents.containsRange(computedProjectExtents.extents)) {
        Logger.logWarning(loggerCategory, "Current project extents may be too small");
      }
      if (computedProjectExtents.outliers && computedProjectExtents.outliers.length > 0) {
        Logger.logInfo(loggerCategory, `${computedProjectExtents.outliers.length} outliers detected within projectExtents`);
      }
    }
  }

  /** Examine the geometry streams of every [GeometricElement3d]($backend) in the target iModel and apply the specified optimizations.
   * @note This method is automatically called from [[IModelTransformer.processChanges]] and [[IModelTransformer.processAll]] if
   * [[IModelTransformOptions.optimizeGeometry]] is defined.
   */
  public optimizeGeometry(options: OptimizeGeometryOptions): void {
    if (options.inlineUniqueGeometryParts) {
      const result = this.targetDb.nativeDb.inlineGeometryPartReferences();
      Logger.logInfo(loggerCategory, `Inlined ${result.numRefsInlined} references to ${result.numCandidateParts} geometry parts and deleted ${result.numPartsDeleted} parts.`);
    }
  }

  /**
   * You may override this to store arbitrary json state in a exporter state dump, useful for some resumptions
   * @see [[IModelTransformer.saveStateToFile]]
   */
  protected getAdditionalStateJson(): any {
    return {};
  }

  /**
   * You may override this to load arbitrary json state in a transformer state dump, useful for some resumptions
   * @see [[IModelTransformer.loadStateFromFile]]
   */
  protected loadAdditionalStateJson(_additionalState: any): void {}

  /**
   * Reload our state from a JSON object
   * Intended for [[IModelTransformer.resumeTransformation]]
   * @internal
   * You can load custom json from the importer save state for custom importers by overriding [[IModelImporter.loadAdditionalStateJson]]
   */
  public loadStateFromJson(state: IModelImporterState): void {
    if (state.importerClass !== this.constructor.name)
      throw Error("resuming from a differently named importer class, it is not necessarily valid to resume with a different importer class");
    // ignore readonly since this runs right after construction in [[IModelTransformer.resumeTransformation]]
    (this.options as IModelTransformOptions) = state.options;
    if (this.targetDb.iModelId !== state.targetDbId)
      throw Error("can only load importer state when the same target is reused");
    // TODO: fix upstream, looks like a bad case for the linter rule when casting away readonly for this generic
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    (this.doNotUpdateElementIds as Set<Id64String>) = CompressedId64Set.decompressSet(state.doNotUpdateElementIds);
    this._duplicateCodeValueMap =  state.duplicateCodeValueMap ? new Map(Object.entries(state.duplicateCodeValueMap)) : undefined;
    this.loadAdditionalStateJson(state.additionalState);
  }

  /**
   * Serialize state to a JSON object
   * Intended for [[IModelTransformer.resumeTransformation]]
   * @internal
   * You can add custom json to the importer save state for custom importers by overriding [[IModelImporter.getAdditionalStateJson]]
   */
  public saveStateToJson(): IModelImporterState {
    return {
      importerClass: this.constructor.name,
      options: this.options,
      targetDbId: this.targetDb.iModelId || this.targetDb.nativeDb.getFilePath(),
      doNotUpdateElementIds: CompressedId64Set.compressSet(this.doNotUpdateElementIds),
      duplicateCodeValueMap: this._duplicateCodeValueMap ? Object.fromEntries(this._duplicateCodeValueMap) : undefined,
      additionalState: this.getAdditionalStateJson(),
    };
  }

  public resolveDuplicateCodeValues(): void {
    if (!this.handleElementCodeDuplicates || !this._duplicateCodeValueMap)
      return;

    for (const [elementId, codeValue] of this._duplicateCodeValueMap) {
      const element = this.targetDb.elements.getElement(elementId);
      element.code.value = codeValue;
      element.update();
    }
  }

  private trackCodeValueDuplicate(targetElementId: Id64String, codeValue: string) {
    if (!this._duplicateCodeValueMap)
      this._duplicateCodeValueMap = new Map<Id64String, string> ();

    this._duplicateCodeValueMap.set(targetElementId, codeValue);
  }
}

/**
 * The JSON format of a serialized IModelimporter instance
 * Used for starting an importer in the middle of an imxport operation,
 * such as resuming a crashed transformation
 *
 * @note Must be kept synchronized with IModelImxporter
 * @internal
 */
export interface IModelImporterState {
  importerClass: string;
  options: IModelImportOptions;
  targetDbId: string;
  doNotUpdateElementIds: CompressedId64Set;
  duplicateCodeValueMap?: Record<Id64String, string>;
  additionalState?: any;
}

/** Returns true if a change within an Entity is detected.
 * @param entity The current persistent Entity.
 * @param entityProps The new EntityProps to compare against
 * @note This method should only be called if changeset information is not available.
 * @internal
 */
export function hasEntityChanged(entity: Entity, entityProps: EntityProps, namesToIgnore?: Set<string>): boolean {
  let changed: boolean = false;
  entity.forEachProperty((propertyName: string, propertyMeta: PropertyMetaData) => {
    if (!changed) {
      if (namesToIgnore && namesToIgnore.has(propertyName)) {
        // skip
      } else if (PrimitiveTypeCode.Binary === propertyMeta.primitiveType) {
        changed = hasBinaryValueChanged(entity.asAny[propertyName], (entityProps as any)[propertyName]);
      } else if (propertyMeta.isNavigation) {
        changed = hasNavigationValueChanged(entity.asAny[propertyName], (entityProps as any)[propertyName]);
      } else {
        changed = hasValueChanged(entity.asAny[propertyName], (entityProps as any)[propertyName]);
      }
    }
  });
  return changed;
}

/** Returns true if the specified binary values are different. */
function hasBinaryValueChanged(binaryProperty1: any, binaryProperty2: any): boolean {
  const jsonString1 = JSON.stringify(binaryProperty1, Base64EncodedString.replacer);
  const jsonString2 = JSON.stringify(binaryProperty2, Base64EncodedString.replacer);
  return jsonString1 !== jsonString2;
}

/** Returns true if the specified navigation property values are different. */
function hasNavigationValueChanged(navigationProperty1: any, navigationProperty2: any): boolean {
  const relatedElement1 = RelatedElement.fromJSON(navigationProperty1);
  const relatedElement2 = RelatedElement.fromJSON(navigationProperty2);
  const jsonString1 = JSON.stringify(relatedElement1);
  const jsonString2 = JSON.stringify(relatedElement2);
  return jsonString1 !== jsonString2;
}

/** Returns true if the specified navigation property values are different. */
function hasValueChanged(property1: any, property2: any): boolean {
  return JSON.stringify(property1) !== JSON.stringify(property2);
}

/** check if element props are a subcategory */
function isSubCategory(props: ElementProps): props is SubCategoryProps {
  return props.classFullName === SubCategory.classFullName;
}

/** check if element props are a subcategory without loading the element */
function isDefaultSubCategory(props: SubCategoryProps): boolean {
  if (props.id === undefined)
    return false;

  if (!Id64.isId64(props.id))
    throw new IModelError(IModelStatus.BadElement, `subcategory had invalid id`);
  if (props.parent?.id === undefined)
    throw new IModelError(IModelStatus.BadElement, `subcategory with id ${props.id} had no parent`);
  return props.id === IModelDb.getDefaultSubCategoryId(props.parent.id);
}
