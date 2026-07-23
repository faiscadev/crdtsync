import { describe, expect, it } from "vitest";
import { CrdtXml, Doc } from "../src/index.js";

describe("CrdtXml", () => {
  it("installs an element and edits its children by index", () => {
    const doc = new Doc();
    const root = doc.getXml("doc");
    root.element("doc");
    expect(root.tag).toBe("doc");

    root.insertElement(0, "p");
    root.insertText(1, "hello");
    expect(root.length).toBe(2);

    root.deleteChild(0);
    expect(root.length).toBe(1);
  });

  it("resolves as an Xml handle from a parent map", () => {
    const doc = new Doc();
    doc.getMap("root").getXml("body").fragment();
    const body = doc.getMap("root").get("body");
    expect(body).toBeInstanceOf(CrdtXml);
    expect((body as CrdtXml).tag).toBeUndefined(); // a fragment is tagless
  });

  it("tree-moves a child to another element preserving the tree", () => {
    const doc = new Doc();
    const a = doc.getXml("a");
    a.element("a").insertElement(0, "x").insertElement(1, "y");
    const b = doc.getXml("b");
    b.element("b");

    a.move(0, b, 0); // move a's child 0 into b
    expect(a.length).toBe(1);
    expect(b.length).toBe(1);
  });

  it("converges xml edits between two docs", () => {
    const p = new Doc();
    const q = new Doc();
    p.on("update", (e) => e.origin === "local" && q.applyUpdate(e.ops));

    p.getXml("doc").element("doc").insertElement(0, "p").insertText(1, "hi");

    const qd = q.getXml("doc");
    expect(qd.tag).toBe("doc");
    expect(qd.length).toBe(2);
  });
});
