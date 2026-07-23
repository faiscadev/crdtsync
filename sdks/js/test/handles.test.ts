import { describe, expect, it } from "vitest";
import { CrdtList, CrdtMap, CrdtText, Doc } from "../src/index.js";

describe("CrdtMap", () => {
  it("round-trips native scalar values", () => {
    const map = new Doc().getMap("root");
    map.set("s", "hello");
    map.set("n", 42);
    map.set("big", 9007199254740993n);
    map.set("b", true);
    map.set("nil", null);
    map.set("bin", Uint8Array.of(1, 2, 3));

    expect(map.get("s")).toBe("hello");
    expect(map.get("n")).toBe(42);
    expect(map.get("big")).toBe(9007199254740993n);
    expect(map.get("b")).toBe(true);
    expect(map.get("nil")).toBe(null);
    expect(map.get("bin")).toEqual(Uint8Array.of(1, 2, 3));
  });

  it("keeps a string and a look-alike byte array distinct", () => {
    const map = new Doc().getMap("root");
    map.set("str", "AB");
    map.set("bin", Uint8Array.of(0x41, 0x42)); // same bytes as "AB"
    expect(map.get("str")).toBe("AB");
    expect(map.get("bin")).toEqual(Uint8Array.of(0x41, 0x42));
  });

  it("rejects a non-integer number and a plain object", () => {
    const map = new Doc().getMap("root");
    expect(() => map.set("f", 1.5)).toThrow();
    // biome-ignore lint/suspicious/noExplicitAny: deliberately testing a rejected type
    expect(() => map.set("o", { a: 1 } as any)).toThrow();
  });

  it("rejects an integer outside the 64-bit range instead of wrapping", () => {
    const map = new Doc().getMap("root");
    expect(() => map.set("over", 2n ** 63n)).toThrow(RangeError);
    expect(() => map.set("under", -(2n ** 63n) - 1n)).toThrow(RangeError);
    expect(() => map.set("huge", 1e30)).toThrow(RangeError);
    // The boundary values themselves are storable.
    map.set("max", 2n ** 63n - 1n);
    map.set("min", -(2n ** 63n));
    expect(map.get("max")).toBe(2n ** 63n - 1n);
    expect(map.get("min")).toBe(-(2n ** 63n));
  });

  it("does not lose a value stored under a non-utf-8 binary key", () => {
    const map = new Doc().getMap("root");
    const key = Uint8Array.of(0xff, 0xfe);
    map.set(key, "kept");
    expect(map.get(key)).toBe("kept");
    // entries() reads the value by the raw key bytes, so it survives even though
    // the key renders as a best-effort string.
    expect(map.entries().map(([, v]) => v)).toContain("kept");
    expect(map.size).toBe(1);
  });

  it("reports has / delete / keys / entries / size", () => {
    const map = new Doc().getMap("root");
    map.set("a", 1).set("b", 2);
    expect(map.size).toBe(2);
    expect(map.has("a")).toBe(true);
    expect(map.has("z")).toBe(false);
    expect(map.keys().sort()).toEqual(["a", "b"]);
    expect(new Map(map.entries()).get("b")).toBe(2);

    map.delete("a");
    expect(map.has("a")).toBe(false);
    expect(map.size).toBe(1);
  });

  it("composes nested map handles", () => {
    const root = new Doc().getMap("root");
    root.getMap("child").set("k", "v");
    const child = root.get("child");
    expect(child).toBeInstanceOf(CrdtMap);
    expect((child as CrdtMap).get("k")).toBe("v");
  });

  it("is iterable", () => {
    const map = new Doc().getMap("root");
    map.set("x", 1).set("y", 2);
    expect(new Map([...map]).size).toBe(2);
  });
});

describe("CrdtList", () => {
  it("inserts, pushes, reads, and deletes scalar items", () => {
    const list = new Doc().getList("items");
    list.push("a").push("b").insert(1, "c");
    expect(list.length).toBe(3);
    expect(list.toArray()).toEqual(["a", "c", "b"]);
    list.delete(0);
    expect(list.toArray()).toEqual(["c", "b"]);
  });

  it("resolves as a handle from a parent map", () => {
    const root = new Doc().getMap("root");
    root.getList("xs").push(7);
    const xs = root.get("xs");
    expect(xs).toBeInstanceOf(CrdtList);
    expect((xs as CrdtList).get(0)).toBe(7);
  });
});

describe("CrdtText", () => {
  it("edits by codepoint index", () => {
    const text = new Doc().getText("body");
    text.insert(0, "hello world");
    text.delete(5, 6);
    text.insert(5, "!");
    expect(text.toString()).toBe("hello!");
    expect(text.length).toBe(6);
  });

  it("resolves as a handle from a parent map", () => {
    const root = new Doc().getMap("root");
    root.getText("t").insert(0, "hi");
    expect(root.get("t")).toBeInstanceOf(CrdtText);
    expect((root.get("t") as CrdtText).toString()).toBe("hi");
  });
});
