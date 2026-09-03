/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { TestTransformerModule } from "./TestTransformerModule";

export interface RegressionTestCase<T> {
  testCase: T;
  functionNameToValidate: keyof TestTransformerModule;
}

export interface RegressionTestDefinition<T> {
  testCaseName: string;
  testCase: T;
  moduleName: string;
  transformerModule: TestTransformerModule;
}

export function getRegressionTestDefinitions<T>(
  testCases: ReadonlyMap<string, RegressionTestCase<T>>,
  transformerModules: ReadonlyMap<string, TestTransformerModule>
): RegressionTestDefinition<T>[] {
  const definitions: RegressionTestDefinition<T>[] = [];
  for (const [testCaseName, testCaseDefinition] of testCases) {
    for (const [moduleName, transformerModule] of transformerModules) {
      if (transformerModule[testCaseDefinition.functionNameToValidate]) {
        definitions.push({
          testCaseName,
          testCase: testCaseDefinition.testCase,
          moduleName,
          transformerModule,
        });
      }
    }
  }
  return definitions;
}
