/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { IModel } from "@itwin/core-common";

/*
 * Tests where we perform "identity" transforms, that is just rebuilding an entire identical iModel (minus IDs)
 * through the transformation process.
 */

export default function identityTransformer(iModel: IModel){
  console.log(iModel.name);
};