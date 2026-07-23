import { describe, expect, it } from "vitest";
import { Doc } from "../src/index.js";

describe("Text cursors (RelativePosition)", () => {
  it("tracks a position across inserts and deletes before it", () => {
    const doc = new Doc();
    const text = doc.getText("body");
    text.insert(0, "hello world");

    // A cursor at "world" (index 6).
    const pos = text.relativePosition(6);
    expect(pos).toBeDefined();
    if (!pos) return;
    expect(text.resolve(pos)).toBe(6);

    // Insert before it — the cursor shifts to stay on "world".
    text.insert(0, ">> ");
    expect(text.resolve(pos)).toBe(9);

    // Delete before it — the cursor shifts back.
    text.delete(0, 3);
    expect(text.resolve(pos)).toBe(6);
  });

  it("survives a concurrent remote edit and still points at the same content", () => {
    const a = new Doc();
    const b = new Doc();
    a.on("update", (e) => e.origin === "local" && b.applyUpdate(e.ops));
    b.on("update", (e) => e.origin === "local" && a.applyUpdate(e.ops));

    a.getText("t").insert(0, "abcdef");
    const cursor = a.getText("t").relativePosition(4); // before "e"
    expect(a.getText("t").resolve(cursor as NonNullable<typeof cursor>)).toBe(4);

    // b inserts at the front concurrently; a's cursor tracks past it.
    b.getText("t").insert(0, "XYZ");
    expect(a.getText("t").resolve(cursor as NonNullable<typeof cursor>)).toBe(7);
    expect(a.getText("t").toString()).toBe("XYZabcdef");
  });

  it("honors gravity: an after-cursor stays right of an insert at the index", () => {
    const doc = new Doc();
    const text = doc.getText("t");
    text.insert(0, "ab");
    const before = text.relativePosition(1, "before");
    const after = text.relativePosition(1, "after");
    text.insert(1, "XX");
    // The whole test only asserts both resolve and differ by the inserted span.
    const b = text.resolve(before as NonNullable<typeof before>);
    const a = text.resolve(after as NonNullable<typeof after>);
    expect(typeof b).toBe("number");
    expect(typeof a).toBe("number");
    expect((a as number) - (b as number)).toBe(2);
  });
});
