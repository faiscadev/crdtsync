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
// the stamp's 8-byte lamport — so the lamport runs from body offset 24, past the
// frame's 4-byte length prefix.
const LAMPORT = 4 + 16 + 8;
// The last id of the space: `u64::MAX >> 1`. A stamp may legally sit there, which
// is why one op is enough to spend its author's mint.
const CEILING = 0x7fffffffffffffffn;

/** One op frame from a doc authored under `client`, its stamp moved to the last
 *  id of the space. */
function planted(client: Uint8Array): Uint8Array {
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
  framed.setBigUint64(LAMPORT, CEILING, true);
  return frame;
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

    const text = new Doc({ clientId: cid(3) });
    text.applyUpdate(planted(cid(3)));
    expect(() => text.getText("t").insert(0, "hello")).toThrow(MintExhausted);
  });

  it("leaves an ordinary edit alone", () => {
    const d = new Doc({ clientId: cid(4) });
    d.getMap("root").set("k", 1);
    // An inert edit emits nothing either, and that is not a refusal.
    d.getMap("root").set("k", 1);
    expect(d.getMap("root").get("k")).toBe(1);
  });
});
