import { describe, expect, it } from "vitest";
import { Doc, MintExhausted } from "../src/index.js";

// A stamp is drawn from the replica's own id space, and the space is finite. When
// it runs out an edit is *refused* — nothing emitted, nothing changed — because
// the alternative is re-issuing an id that is already live, which every peer drops
// as a replay. Refusal is the right answer and silence is not: every mutator
// returns the same empty ops an inert edit returns, so without a throw the
// application reports a write that never happened.

function cid(first: number): Uint8Array {
  const b = new Uint8Array(16);
  b[0] = first;
  return b;
}

// An op body opens with its author's 16-byte client id, its 8-byte sequence and
// the stamp's 8-byte lamport — so the sequence runs from body offset 16 and the
// lamport from 24, both past the frame's 4-byte length prefix.
const OP_SEQ = 4 + 16;
const LAMPORT = 4 + 16 + 8;
// An op-id sequence the receiving replica has not spent, so the plant is not
// deduplicated away as one of that replica's own ops.
const UNSPENT_SEQ = 9999n;
// The last id of the space: `u64::MAX >> 1`. A stamp may legally sit there, which
// is why one op is enough to spend its author's mint.
const CEILING = 0x7fffffffffffffffn;

/** One op frame from a doc authored under `client`, its stamp moved to `lamport`. */
function stampedAt(client: Uint8Array, lamport: bigint): Uint8Array {
  const d = new Doc({ clientId: client });
  const emitted: Uint8Array[] = [];
  d.on("update", (e) => {
    if (e.origin === "local") emitted.push(e.ops);
  });
  d.getMap("root").set("k", 1);
  const log = emitted[0];
  const view = new DataView(log.buffer, log.byteOffset, log.byteLength);
  const len = view.getUint32(0, true);
  const frame = log.slice(0, 4 + len);
  const framed = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  framed.setBigUint64(OP_SEQ, UNSPENT_SEQ, true);
  framed.setBigUint64(LAMPORT, lamport, true);
  return frame;
}

/** The plant that spends the space outright. */
function planted(client: Uint8Array): Uint8Array {
  return stampedAt(client, CEILING);
}

/** A plant that leaves a handful of ids — enough for a single-id edit, not for a
 *  ten-codepoint run. */
function nearlySpent(client: Uint8Array): Uint8Array {
  return stampedAt(client, CEILING - 6n);
}

describe("a spent id space", () => {
  it("throws rather than reporting a write that never happened", () => {
    const me = cid(1);
    const d = new Doc({ clientId: me });
    // A peer authoring under this replica's own client id needs one admissible op
    // to put the id space at its end.
    d.applyUpdate(planted(me));

    expect(() => d.getMap("root").set("k", 1)).toThrow(MintExhausted);
    expect(d.getMap("root").get("k")).toBeUndefined();
  });

  it("throws from a transaction and from a returning mutator alike", () => {
    const me = cid(2);
    const d = new Doc({ clientId: me });
    d.applyUpdate(planted(me));

    expect(() =>
      d.transact(() => {
        d.getList("items").insert(0, "a");
      }),
    ).toThrow(MintExhausted);

    // `mark` returns the mark's handle, so it goes through the returning funnel.
    const text = new Doc({ clientId: cid(3) });
    text.getText("t").insert(0, "hello");
    text.applyUpdate(planted(cid(3)));
    expect(() => text.getText("t").mark(0, 3, "bold", true)).toThrow(MintExhausted);
  });

  it("still ships what the refused call emitted before the refusal", () => {
    // One handle call is one core transaction, and a refusal cuts it at the edit
    // that could not mint — so a refused call can carry ops that did. They are
    // applied to this replica already; withholding them would leave it ahead of
    // every peer.
    const me = cid(5);
    const d = new Doc({ clientId: me });
    const updates: Uint8Array[] = [];
    d.on("update", (e) => {
      if (e.origin === "local") updates.push(e.ops);
    });
    d.applyUpdate(nearlySpent(me));

    // The text does not exist, so this emits a container-create the space still has
    // room for, then a ten-codepoint run it does not.
    expect(() => d.getText("t").insert(0, "abcdefghij")).toThrow(MintExhausted);
    expect(updates.length).toBe(1);
    expect(d.getText("t").toString()).toBe("");
  });

  it("reports the refusal even when a listener throws", () => {
    // The refusal is the answer to the call the application made; a listener's own
    // failure must not take its place, or the edit reads as a listener bug and the
    // write that never happened is reported as one that did.
    const me = cid(7);
    const d = new Doc({ clientId: me });
    const boom = new Error("listener");
    d.applyUpdate(nearlySpent(me));
    d.on("update", (e) => {
      if (e.origin === "local") throw boom;
    });

    // The container-create is delivered (and the listener throws on it), then the
    // ten-codepoint run is refused.
    try {
      d.getText("t").insert(0, "abcdefghij");
      expect.unreachable("the refusal was swallowed");
    } catch (e) {
      expect(e).toBeInstanceOf(MintExhausted);
      expect((e as MintExhausted).cause).toBe(boom);
    }
  });

  it("does not report an inert edit as a refusal", () => {
    // An inert edit and a refused one both emit nothing, which is the whole reason
    // the query exists — so an edit that resolves to nothing must answer for itself
    // rather than inherit the previous edit's refusal.
    const me = cid(6);
    const d = new Doc({ clientId: me });
    d.getText("t").insert(0, "ab");
    d.applyUpdate(nearlySpent(me));

    expect(() => d.getText("t").insert(0, "abcdefghij")).toThrow(MintExhausted);
    // An XML insert on a path that holds no XML node resolves to nothing.
    expect(() => d.getXml("nope").insertElement(0, "p")).not.toThrow();
    // A blob too large to inline answers `false` — its own answer, not a refusal.
    expect(() => d.getText("t").insert(0, "abcdefghij")).toThrow(MintExhausted);
    expect(d.getMap("m").setBlob("big", "application/octet-stream", new Uint8Array(4097))).toBe(
      false,
    );
    // And the replica really did still have room.
    expect(() => d.getText("t").insert(0, "z")).not.toThrow();
  });
});
