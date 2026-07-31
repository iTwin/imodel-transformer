/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EntityKey, EntityMap } from "../../EntityMap";

describe("EntityMap", () => {
  const firstKey = "0x1" as EntityKey;
  const secondKey = "0x2" as EntityKey;

  it("stores, reads, iterates, and deletes values by entity key", () => {
    const map = new EntityMap<string>();

    expect(map.setByKey(firstKey, "first")).toBe(map);
    expect(map.setByKey(secondKey, "second")).toBe(map);
    expect(map.size).toBe(2);
    expect(map.getByKey(firstKey)).toBe("first");
    expect([...map.keys()]).to.have.members([firstKey, secondKey]);
    expect([...map.values()]).to.have.members(["first", "second"]);
    const expected = new Map<EntityKey, string>([
      [firstKey, "first"],
      [secondKey, "second"],
    ]);
    expect(new Map(map.entries())).toEqual(expected);
    expect(new Map(map)).toEqual(expected);

    expect(map.deleteByKey(firstKey)).toBe(true);
    expect(map.getByKey(firstKey)).toBeUndefined();
    expect(map.size).toBe(1);
  });

  it("clears all values", () => {
    const map = new EntityMap<string>();
    map.setByKey(firstKey, "first");

    map.clear();

    expect(map.size).toBe(0);
    expect(map.getByKey(firstKey)).toBeUndefined();
  });
});
