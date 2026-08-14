/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64Array, Id64Set, Id64String } from "@itwin/core-bentley";
import {
  Category,
  CategorySelector,
  DisplayStyle,
  DisplayStyle3d,
  ExternalSourceAspect,
  GeometricModel3d,
  IModelDb,
  ModelSelector,
  SpatialCategory,
  SpatialModel,
  SpatialViewDefinition,
  SubCategory,
  ViewDefinition,
  withEditTxn,
} from "@itwin/core-backend";
import { IModel, QueryBinder } from "@itwin/core-common";

export namespace ElementUtils {
  async function queryElementIds(
    iModelDb: IModelDb,
    classFullName: string
  ): Promise<Id64Set> {
    const elementIds = new Set<Id64String>();
    const sql = `SELECT ECInstanceId FROM ${classFullName}`;
    for await (const row of iModelDb.createQueryReader(sql, undefined, {
      usePrimaryConn: true,
    })) {
      elementIds.add(row.id);
    }
    return elementIds;
  }

  export async function validateModelSelectors(
    iModelDb: IModelDb
  ): Promise<void> {
    const modelSelectorIds = await queryElementIds(
      iModelDb,
      ModelSelector.classFullName
    );
    modelSelectorIds.forEach((modelSelectorId: Id64String) => {
      const modelSelector = iModelDb.elements.getElement<ModelSelector>(
        modelSelectorId,
        ModelSelector
      );
      validateModelSelector(modelSelector);
    });
  }

  function validateModelSelector(modelSelector: ModelSelector): void {
    const iModelDb = modelSelector.iModel;
    modelSelector.models.forEach((modelId: Id64String) => {
      iModelDb.models.getModel<GeometricModel3d>(modelId, GeometricModel3d); // will throw Error if not a valid GeometricModel3d
    });
  }

  export async function validateCategorySelectors(
    iModelDb: IModelDb
  ): Promise<void> {
    const categorySelectorIds = await queryElementIds(
      iModelDb,
      CategorySelector.classFullName
    );
    categorySelectorIds.forEach((categorySelectorId: Id64String) => {
      const categorySelector = iModelDb.elements.getElement<CategorySelector>(
        categorySelectorId,
        CategorySelector
      );
      validateCategorySelector(categorySelector);
    });
  }

  function validateCategorySelector(categorySelector: CategorySelector): void {
    const iModelDb = categorySelector.iModel;
    categorySelector.categories.forEach((categoryId: Id64String) => {
      iModelDb.elements.getElement<Category>(categoryId, Category); // will throw Error if not a valid Category
    });
  }

  export async function validateDisplayStyles(
    iModelDb: IModelDb
  ): Promise<void> {
    const displayStyleIds = await queryElementIds(
      iModelDb,
      DisplayStyle.classFullName
    );
    displayStyleIds.forEach((displayStyleId: Id64String) => {
      const displayStyle = iModelDb.elements.getElement<DisplayStyle>(
        displayStyleId,
        DisplayStyle
      );
      validateDisplayStyle(displayStyle);
    });
  }

  function validateDisplayStyle(displayStyle: DisplayStyle): void {
    const iModelDb = displayStyle.iModel;
    if (displayStyle.settings?.subCategoryOverrides) {
      for (const subCategoryId of displayStyle.settings.subCategoryOverrides.keys()) {
        iModelDb.elements.getElement<SubCategory>(subCategoryId, SubCategory); // will throw Error if not a valid SubCategory
      }
    }
    if (displayStyle.settings?.excludedElementIds) {
      for (const elementId of displayStyle.settings.excludedElementIds) {
        iModelDb.elements.getElement(elementId); // will throw Error if not a valid Element
      }
    }
  }

  export async function queryProvenanceScopeIds(
    iModelDb: IModelDb
  ): Promise<Id64Set> {
    const elementIds = new Set<Id64String>();
    if (iModelDb.containsClass(ExternalSourceAspect.classFullName)) {
      const sql = `SELECT Element.Id AS id FROM ${ExternalSourceAspect.classFullName} WHERE Kind=:kind`;
      const bindings = new QueryBinder().bindString(
        "kind",
        ExternalSourceAspect.Kind.Scope
      );
      for await (const row of iModelDb.createQueryReader(sql, bindings, {
        usePrimaryConn: true,
      })) {
        elementIds.add(row.id);
      }
    }
    return elementIds;
  }

  /** Generate and insert a ViewDefinition that views all models and all SpatialCategories.
   * @param iModelDb The ViewDefinition will be inserted into this IModelDb.
   * @param name The name (CodeValue) for the inserted ViewDefinition.
   * @param makeDefault If `true` make the inserted ViewDefinition the default view.
   * @returns The Id of the ViewDefinition that was found or inserted.
   */
  export async function insertViewDefinition(
    iModelDb: IModelDb,
    name: string
  ): Promise<Id64String> {
    const definitionModelId = IModel.dictionaryId;
    const viewCode = ViewDefinition.createCode(
      iModelDb,
      definitionModelId,
      name
    );
    let viewId = iModelDb.elements.queryElementIdByCode(viewCode);
    if (viewId === undefined) {
      const modelIds = await queryModelIds(
        iModelDb,
        SpatialModel.classFullName
      );
      const categoryIds = await querySpatialCategoryIds(iModelDb);
      viewId = withEditTxn(iModelDb, "insert ViewDefinition", (txn) => {
        const modelSelectorId = ModelSelector.insert(
          txn,
          definitionModelId,
          name,
          modelIds
        );
        const categorySelectorId = CategorySelector.insert(
          txn,
          definitionModelId,
          name,
          categoryIds
        );
        const displayStyleId = DisplayStyle3d.insert(
          txn,
          definitionModelId,
          name
        );
        return SpatialViewDefinition.insertWithCamera(
          txn,
          definitionModelId,
          name,
          modelSelectorId,
          categorySelectorId,
          displayStyleId,
          iModelDb.projectExtents
        );
      });
    }
    return viewId;
  }

  async function queryModelIds(
    iModelDb: IModelDb,
    modelClassFullName: string
  ): Promise<Id64Array> {
    const modelIds: Id64Array = [];
    const sql = `SELECT ECInstanceId FROM ${modelClassFullName} WHERE IsTemplate=false`;
    for await (const row of iModelDb.createQueryReader(sql, undefined, {
      usePrimaryConn: true,
    })) {
      modelIds.push(row.id);
    }
    return modelIds;
  }

  async function querySpatialCategoryIds(
    iModelDb: IModelDb
  ): Promise<Id64Array> {
    const categoryIds: Id64Array = [];
    const sql = `SELECT ECInstanceId FROM ${SpatialCategory.classFullName}`;
    for await (const row of iModelDb.createQueryReader(sql, undefined, {
      usePrimaryConn: true,
    })) {
      categoryIds.push(row.id);
    }
    return categoryIds;
  }
}
