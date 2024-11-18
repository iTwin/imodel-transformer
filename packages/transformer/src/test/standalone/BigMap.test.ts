/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BigMap } from "../../BigMap.js";
import { assert } from "chai";

describe("BigMap", function () {
  // Test map keys will be assigned into 2 different submaps when BigMap is created
  const testMap = new Map([
    ["0x123f", "testVal1"],
    ["0x1231", "testVal2"],
  ]);

  const createBigMap = (map: Map<string, string>): BigMap<string> => {
    const bigMap = new BigMap<string>();
    for (const entry of map.entries()) {
      bigMap.set(entry[0], entry[1]);
    }
    return bigMap;
  };

  describe("keys", function () {
    it("should iterate all keys", async function () {
      const bigMap = createBigMap(testMap);
      assert.sameMembers([...bigMap.keys()], [...testMap.keys()]);
    });
  });

  describe("values", function () {
    it("should get all values", async function () {
      const bigMap = createBigMap(testMap);
      assert.sameMembers([...bigMap.values()], [...testMap.values()]);
    });
  });

  describe("entries", function () {
    it("should get all values", async function () {
      const bigMap = createBigMap(testMap);
      const actualMap = new Map([...bigMap.entries()]);
      assert.deepEqual(actualMap, testMap);
    });
  });

  describe("iterator", function () {
    it("should get all values", async function () {
      const bigMap = createBigMap(testMap);

      const actualMap = new Map();
      for (const entry of bigMap) {
        actualMap.set(entry[0], entry[1]);
      }
      assert.deepEqual(actualMap, testMap);
    });
  });

  describe("toStringTag", function () {
    it("should return type name", async function () {
      const typeName = Object.prototype.toString.call(new BigMap<string>());
      assert.equal(typeName, "[object BigMap]");
    });
  });

  describe("has", function () {
    it("should return true when value was set", async function () {
      const bigMap = new BigMap<string>();
      const key = "0x123f";
      bigMap.set(key, "12134");
      assert.isTrue(bigMap.has(key));
    });
  });

  describe("set", function () {
    it("should set when key has submap", async function () {
      const bigMap = new BigMap<string>();
      assert.doesNotThrow(() => bigMap.set("0x13", "12134"));
      assert.equal(bigMap.size, 1);
    });

    it("should throw when key has no submap", async function () {
      const bigMap = new BigMap<string>();
      assert.throw(
        () => bigMap.set("g", "12134"),
        "Tried to set g, but that key has no submap"
      );
    });
  });
});
