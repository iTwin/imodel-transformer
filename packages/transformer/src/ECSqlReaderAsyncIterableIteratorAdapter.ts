/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { ECSqlReader, QueryRowProxy } from "@itwin/core-common";

/**
 * Adapter class for ECSqlReader for AsyncIterableIterator interface.
 * It allows to iterate through query results in itwin 3.x using for await() syntax like using itwin 4.x version.
 * @internal
 */
export class ECSqlReaderAsyncIterableIteratorAdapter implements AsyncIterableIterator<QueryRowProxy> {

  public constructor(private _ecSqlReader: ECSqlReader) { }

  public [Symbol.asyncIterator](): AsyncIterableIterator<QueryRowProxy> {
    return this;
  }

  public async next(): Promise<IteratorResult<QueryRowProxy, any>> {
    const done = !(await this._ecSqlReader.step());
    return {
      done,
      value: this._ecSqlReader.current,
    };
  }
}

/**
 * Wraps ECSqlReader with ECSqlReaderAsyncIterableIteratorAdapter if it's needed.
 * @param ecSqlReader ECSqlReader isntance from itwin 3.x or 4.x version
 * @internal
 */
export function ensureECSqlReaderIsAsyncIterableIterator(ecSqlReader: ECSqlReader & AsyncIterableIterator<QueryRowProxy> | Omit<ECSqlReader, keyof AsyncIterableIterator<QueryRowProxy>>): AsyncIterableIterator<QueryRowProxy> {
  if (Symbol.asyncIterator in ecSqlReader) { // using itwin 4.x
    return ecSqlReader;
  } else { // using itwin 3.x
    return new ECSqlReaderAsyncIterableIteratorAdapter(ecSqlReader as ECSqlReader);
  }
}
