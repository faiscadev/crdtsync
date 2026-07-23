import { describe, expect, it } from "vitest";
import { Doc } from "../src/index.js";

const enc = new TextEncoder();

// A schema declaring the three mark flavors over a root text body. A bound schema
// gives a named mark its declared flavor; unbound, every mark is object-flavor.
const MARK_SCHEMA = JSON.stringify({
  schema: "doc",
  version: 1,
  root: "Doc",
  types: {
    Doc: { kind: "map", children: { body: "Body" } },
    Body: { kind: "text" },
  },
  marks: {
    bold: { flavor: "boolean" },
    link: { flavor: "value" },
    comment: { flavor: "object" },
  },
});

// A schema bounding a root text to 5 characters, so overflowing it needs a repair.
const REPAIR_SCHEMA = JSON.stringify({
  schema: "notes",
  version: 1,
  root: "Doc",
  types: {
    Doc: { kind: "map", children: { body: "Body" } },
    Body: { kind: "text", max: 5 },
  },
});

describe("schema binding", () => {
  it("binds a valid schema and rejects malformed bytes", () => {
    const doc = new Doc();
    expect(doc.setSchema(enc.encode(MARK_SCHEMA))).toBe(true);
    expect(doc.setSchema(enc.encode("not json"))).toBe(false);
    expect(doc.setSchema(Uint8Array.of(0xff, 0xfe))).toBe(false); // non-utf8
  });

  it("gives a named mark its schema-declared flavor", () => {
    const doc = new Doc();
    doc.setSchema(enc.encode(MARK_SCHEMA));
    const text = doc.getText("body");
    text.insert(0, "hello world");

    text.mark(0, 5, "bold", true); // boolean flavor
    text.mark(0, 5, "link", "https://example.com"); // value flavor
    text.mark(0, 5, "comment", "note-1"); // object flavor

    const marks = text.marksAt(1);
    expect(marks.find((m) => m.name === "bold")?.value).toBe(true);
    expect(marks.find((m) => m.name === "link")?.value).toBe("https://example.com");
    // An object mark still resolves to the covering element ids.
    expect(Array.isArray(marks.find((m) => m.name === "comment")?.value)).toBe(true);
  });

  it("resolves a mark as object-flavor when no schema is bound", () => {
    const doc = new Doc();
    const text = doc.getText("body");
    text.insert(0, "hello world");
    text.mark(0, 5, "bold", true);

    // Unbound, even a would-be boolean mark is an object-flavor range annotation.
    const bold = text.marksAt(1).find((m) => m.name === "bold");
    expect(Array.isArray(bold?.value)).toBe(true);
  });
});

describe("repair signal", () => {
  it("fires onRepaired for a local edit that overflows a bounded sequence", () => {
    const doc = new Doc();
    doc.setSchema(enc.encode(REPAIR_SCHEMA));

    const repaired: (string | number)[][] = [];
    doc.on("repair", (e) => {
      for (const p of e.paths) repaired.push(p);
    });

    doc.getText("body").insert(0, "hello world"); // 11 > max 5
    expect(repaired).toContainEqual(["body"]);
  });

  it("fires nothing for a conforming edit", () => {
    const doc = new Doc();
    doc.setSchema(enc.encode(REPAIR_SCHEMA));

    let fired = 0;
    doc.on("repair", (e) => {
      fired += e.paths.length;
    });
    doc.getText("body").insert(0, "hi"); // within max 5
    expect(fired).toBe(0);
  });

  it("fires nothing when no schema is bound", () => {
    const doc = new Doc();
    let fired = 0;
    doc.on("repair", () => {
      fired += 1;
    });
    doc.getText("body").insert(0, "a very long body well over any bound");
    expect(fired).toBe(0);
  });

  it("stops delivering after the listener unsubscribes", () => {
    const doc = new Doc();
    doc.setSchema(enc.encode(REPAIR_SCHEMA));
    let fired = 0;
    const listener = (): void => {
      fired += 1;
    };
    doc.on("repair", listener);
    doc.getText("body").insert(0, "overflowing"); // fires once
    doc.off("repair", listener);
    doc.getText("body").insert(0, "more overflow");
    expect(fired).toBe(1);
  });
});
