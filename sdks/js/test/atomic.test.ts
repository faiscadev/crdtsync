import { describe, expect, it } from "vitest";
import { Doc, type UpdateEvent } from "../src/index.js";

describe("atomic transactions", () => {
  it("groups edits into a single update", () => {
    const doc = new Doc();
    const m = doc.getMap("root");
    m.set("init", 0); // create the map outside the transaction

    const events: UpdateEvent[] = [];
    doc.on("update", (e) => events.push(e));

    doc.transact(() => {
      m.set("a", 1);
      m.set("b", 2);
      m.set("c", 3);
    });

    expect(events).toHaveLength(1); // one batched update, not three
    expect(events[0].origin).toBe("local");
    expect(m.get("a")).toBe(1);
    expect(m.get("c")).toBe(3);
  });

  it("applies the whole batch atomically on a peer", () => {
    const a = new Doc();
    const b = new Doc();
    a.on("update", (e) => e.origin === "local" && b.applyUpdate(e.ops));

    a.getMap("root").set("init", 0);
    a.transact(() => {
      a.getMap("root").set("x", 1);
      a.getMap("root").set("y", 2);
      a.getList("log").push("entry");
    });

    expect(b.getMap("root").get("x")).toBe(1);
    expect(b.getMap("root").get("y")).toBe(2);
    expect(b.getList("log").toArray()).toEqual(["entry"]);
  });

  it("flattens a nested transaction into the outer one", () => {
    const doc = new Doc();
    const m = doc.getMap("root");
    m.set("init", 0);

    const events: UpdateEvent[] = [];
    doc.on("update", (e) => events.push(e));

    doc.transact(() => {
      m.set("a", 1);
      doc.transact(() => {
        m.set("b", 2);
      });
      m.set("c", 3);
    });

    expect(events).toHaveLength(1);
    expect(m.get("b")).toBe(2);
  });
});
