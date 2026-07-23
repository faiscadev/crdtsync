import { describe, expect, it } from "vitest";
import { Doc, type UpdateEvent } from "../src/index.js";

// Wire two docs so each applies the other's local update ops — the base sync
// model with no provider. After exchanging ops they must converge.
function connect(a: Doc, b: Doc): void {
  a.on("update", (e: UpdateEvent) => {
    if (e.origin === "local") b.applyUpdate(e.ops);
  });
  b.on("update", (e: UpdateEvent) => {
    if (e.origin === "local") a.applyUpdate(e.ops);
  });
}

describe("convergence", () => {
  it("mirrors map, list, and text edits between two docs", () => {
    const a = new Doc();
    const b = new Doc();
    connect(a, b);

    a.getMap("root").set("title", "Doc");
    a.getMap("root").set("count", 3);
    a.getList("items").push("x").push("y");
    a.getText("body").insert(0, "hello");

    expect(b.getMap("root").get("title")).toBe("Doc");
    expect(b.getMap("root").get("count")).toBe(3);
    expect(b.getList("items").toArray()).toEqual(["x", "y"]);
    expect(b.getText("body").toString()).toBe("hello");
  });

  it("converges concurrent edits to identical state", () => {
    const a = new Doc();
    const b = new Doc();

    // Collect every local op from the start; edit both offline, then exchange.
    const opsA: Uint8Array[] = [];
    const opsB: Uint8Array[] = [];
    a.on("update", (e) => {
      if (e.origin === "local") opsA.push(e.ops);
    });
    b.on("update", (e) => {
      if (e.origin === "local") opsB.push(e.ops);
    });

    a.getMap("root").set("fromA", 1);
    b.getMap("root").set("fromB", 2);
    a.getText("body").insert(0, "AAA");
    b.getText("body").insert(0, "BBB");

    for (const ops of opsA) b.applyUpdate(ops);
    for (const ops of opsB) a.applyUpdate(ops);

    // Both maps hold both keys; both texts hold the same 6 codepoints.
    expect(a.getMap("root").get("fromA")).toBe(1);
    expect(a.getMap("root").get("fromB")).toBe(2);
    expect(b.getMap("root").get("fromA")).toBe(1);
    expect(b.getMap("root").get("fromB")).toBe(2);
    expect(a.getText("body").toString()).toBe(b.getText("body").toString());
    expect(a.getText("body").length).toBe(6);
  });
});
