import { describe, expect, it } from "vitest";
import { remarshalChange } from "../src/changes.js";
import { encodePath } from "../src/path.js";

const utf8 = (s: string) => new TextEncoder().encode(s);

describe("remarshalChange", () => {
  it("handles a mark change without a path field instead of crashing", () => {
    // A mark diff object carries name/value but NO path (crates/wasm change_to_js).
    const add = remarshalChange({
      op: "markAdd",
      name: utf8("bold"),
      value: { t: "bool", v: true },
    });
    expect(add.change).toEqual({ kind: "mark", op: "add", name: "bold", new: true });
    expect(add.pathBytes.length).toBe(0);

    const changed = remarshalChange({
      op: "markChange",
      name: utf8("color"),
      old: { t: "bool", v: false },
      new: { t: "bool", v: true },
    });
    expect(changed.change).toEqual({
      kind: "mark",
      op: "change",
      name: "color",
      old: false,
      new: true,
    });
  });

  it("keeps a large counter value exact as a bigint", () => {
    const big = 2n ** 60n;
    const c = remarshalChange({
      op: "counter",
      path: encodePath(["c"]),
      old: big,
      new: big + 1n,
    });
    expect(c.change).toEqual({ kind: "counter", path: ["c"], old: big, new: big + 1n });
  });

  it("reports a removal as remove, not add", () => {
    const r = remarshalChange({ op: "remove", path: encodePath(["x"]), kind: "map" });
    expect(r.change).toEqual({ kind: "remove", path: ["x"], valueKind: "map" });
  });
});
