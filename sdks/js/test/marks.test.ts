import { describe, expect, it } from "vitest";
import { Doc } from "../src/index.js";

// A mark's flavor (boolean / value / object) is schema-driven in the core; with no
// bound schema a mark is an object-flavor range annotation (it tracks the covered
// element ids). These tests exercise authoring, reading, deletion, and — the point
// of the wasm mark-ops fix — that a local mark's ops actually broadcast.

describe("marks", () => {
  it("authors a mark over a range and reads it back by name", () => {
    const doc = new Doc();
    const text = doc.getText("body");
    text.insert(0, "hello world");

    const id = text.mark(0, 5, "comment", "note-1");
    expect(id).toBeDefined();

    const at0 = text.marksAt(0);
    expect(at0.map((m) => m.name)).toContain("comment");
    expect(text.marksAt(8)).toEqual([]); // outside the range
  });

  it("removes a mark by handle", () => {
    const doc = new Doc();
    const text = doc.getText("body");
    text.insert(0, "abcdef");

    const id = text.mark(0, 6, "comment", "x");
    if (!id) throw new Error("mark failed");
    expect(text.marksAt(2).map((m) => m.name)).toContain("comment");

    text.deleteMark(id);
    expect(text.marksAt(2)).toEqual([]);
  });

  it("syncs a mark to a peer — its ops are broadcast, not discarded", () => {
    const a = new Doc();
    const b = new Doc();
    a.on("update", (e) => e.origin === "local" && b.applyUpdate(e.ops));

    a.getText("t").insert(0, "hello");
    const id = a.getText("t").mark(0, 5, "comment", "hi");
    expect(id).toBeDefined();

    // Before the fix, WasmDocument.mark discarded the ops, so nothing reached b.
    expect(b.getText("t").marksAt(1).map((m) => m.name)).toContain("comment");
  });

  it("reports a mark change through reactivity", () => {
    const doc = new Doc();
    const text = doc.getText("t");
    text.insert(0, "hello");

    const kinds: string[] = [];
    doc.on("update", (e) => {
      for (const c of e.changes) if (c.kind === "mark") kinds.push(c.op);
    });
    text.mark(0, 5, "comment", "x");
    expect(kinds).toContain("add");
  });
});
