import { describe, expect, it } from "vitest";
import { type Change, Doc, type UpdateEvent } from "../src/index.js";

describe("Doc.on('update')", () => {
  it("delivers a diff-derived value change with native old/new", () => {
    const doc = new Doc();
    const m = doc.getMap("root");
    m.set("k", 1); // creates the map (an "add"); observed below is the update

    const events: UpdateEvent[] = [];
    doc.on("update", (e) => events.push(e));
    m.set("k", 2);

    expect(events).toHaveLength(1);
    expect(events[0].origin).toBe("local");
    expect(events[0].ops.length).toBeGreaterThan(0);
    expect(events[0].changes).toContainEqual({
      kind: "update",
      path: ["root", "k"],
      old: 1,
      new: 2,
    });
  });

  it("reports a list insert with its index and native values", () => {
    const doc = new Doc();
    const list = doc.getList("xs");
    list.push("a"); // creates the list

    const changes: Change[] = [];
    doc.on("update", (e) => changes.push(...e.changes));
    list.push("b");

    expect(changes).toContainEqual({
      kind: "listInsert",
      path: ["xs"],
      index: 1,
      values: ["b"],
    });
  });

  it("reports a text insert with its index and text", () => {
    const doc = new Doc();
    const text = doc.getText("t");
    text.insert(0, "a"); // creates the text

    const changes: Change[] = [];
    doc.on("update", (e) => changes.push(...e.changes));
    text.insert(1, "bc");

    expect(changes).toContainEqual({ kind: "textInsert", path: ["t"], index: 1, text: "bc" });
  });

  it("does not deliver the in-flight event to a listener added during dispatch", () => {
    const doc = new Doc();
    const m = doc.getMap("root");
    m.set("k", 1);

    let added = false;
    let lateFired = 0;
    doc.on("update", () => {
      if (!added) {
        added = true;
        doc.on("update", () => lateFired++);
      }
    });

    m.set("k", 2); // adds the late listener; it must not see this event
    expect(lateFired).toBe(0);
    m.set("k", 3); // now the late listener fires
    expect(lateFired).toBe(1);
  });

  it("does not compute changes once every listener has unsubscribed", () => {
    const doc = new Doc();
    const m = doc.getMap("root");
    m.set("k", 1);
    const listener = (): void => {
      throw new Error("should not fire after off()");
    };
    doc.on("update", listener);
    doc.off("update", listener);
    expect(() => m.set("k", 2)).not.toThrow();
  });
});

describe("handle.observe", () => {
  it("fires only for changes under the observed subtree", () => {
    const doc = new Doc();
    const root = doc.getMap("root");
    root.getMap("a").set("x", 1);
    root.getMap("b").set("y", 1);

    const aEvents: Change[][] = [];
    root.getMap("a").observe((e) => aEvents.push(e.changes));

    root.getMap("a").set("x", 2); // under "a" — observed
    root.getMap("b").set("y", 2); // under "b" — not observed

    expect(aEvents).toHaveLength(1);
    expect(aEvents[0]).toContainEqual({ kind: "update", path: ["root", "a", "x"], old: 1, new: 2 });
  });

  it("stops delivering after unsubscribe", () => {
    const doc = new Doc();
    const m = doc.getMap("root");
    m.set("k", 1);
    let fired = 0;
    const off = m.observe(() => fired++);
    m.set("k", 2);
    off();
    m.set("k", 3);
    expect(fired).toBe(1);
  });
});

describe("remote-origin changes", () => {
  it("tags an applied peer update as remote and re-marshals its changes", () => {
    const a = new Doc();
    const b = new Doc();
    // Forward every local edit on a into b from the start.
    a.on("update", (e) => {
      if (e.origin === "local") b.applyUpdate(e.ops);
    });

    a.getMap("root").set("k", 1); // b receives the creation (no listener on b yet)

    const bEvents: UpdateEvent[] = [];
    b.on("update", (e) => bEvents.push(e));
    a.getMap("root").set("k", 2); // forwarded to b as a remote update

    expect(bEvents).toHaveLength(1);
    expect(bEvents[0].origin).toBe("remote");
    expect(bEvents[0].changes).toContainEqual({
      kind: "update",
      path: ["root", "k"],
      old: 1,
      new: 2,
    });
  });
});
