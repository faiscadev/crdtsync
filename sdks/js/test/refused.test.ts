import { describe, expect, it } from "vitest";
import { Doc } from "../src/index.js";

// A fold reports how many ops it took now *and* how many no replica will ever
// hold, because the two zeros mean opposite things. A buffered op is waiting on
// a create a later update carries; a refused one is a bug in whoever wrote it,
// and offline, P2P and relayed peers reach this fold with no server between them
// to reject it first.

function cid(first: number): Uint8Array {
  const b = new Uint8Array(16);
  b[0] = first;
  return b;
}

// An op log frames each op as a u32 length then its body; split it back apart.
function frames(log: Uint8Array): Uint8Array[] {
  const view = new DataView(log.buffer, log.byteOffset, log.byteLength);
  const out: Uint8Array[] = [];
  for (let at = 0; at < log.length; ) {
    const len = view.getUint32(at, true);
    out.push(log.subarray(at, at + 4 + len));
    at += 4 + len;
  }
  return out;
}

function log(...ops: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(ops.reduce((n, op) => n + op.length, 0));
  let at = 0;
  for (const op of ops) {
    out.set(op, at);
    at += op.length;
  }
  return out;
}

// An op body opens with its author's 16-byte client id, its 8-byte sequence and
// the stamp's 8-byte lamport, so the stamp's own client id runs from body offset
// 32 — past the frame's 4-byte length prefix. Naming another client there mints
// node ids inside that client's id space, which no replica will ever hold.
const STAMP_CLIENT = 4 + 16 + 8 + 8;

function forgeStampClient(frame: Uint8Array, author: Uint8Array): Uint8Array {
  // Read the field back first, so a codec reordering fails here by name rather
  // than as an unexplained "nothing was refused" further down.
  expect([...frame.subarray(STAMP_CLIENT, STAMP_CLIENT + 16)]).toEqual([...author]);
  const forged = frame.slice();
  forged.fill(0xff, STAMP_CLIENT, STAMP_CLIENT + 16);
  return forged;
}

/** A doc that wrote twice into one map, with its `create`, `write` and `later`
 *  ops. The first write into a map is two ops — the container create, then the
 *  write into it; a second write is one op, targeting that container. */
function openedMap(first: number): [Uint8Array, Uint8Array, Uint8Array] {
  const d = new Doc({ clientId: cid(first) });
  const emitted: Uint8Array[] = [];
  d.on("update", (e) => {
    if (e.origin === "local") emitted.push(e.ops);
  });
  d.getMap("root").set("k", 1);
  d.getMap("root").set("k2", 2);
  expect(emitted.length).toBe(2);
  const opened = frames(emitted[0]);
  expect(opened.length).toBe(2);
  return [opened[0], opened[1], frames(emitted[1])[0]];
}

describe("refused ops", () => {
  it("counts a permanent refusal apart from a buffered one", () => {
    const [create, write, later] = openedMap(1);

    const forged = forgeStampClient(later, cid(1));
    const b = new Doc({ clientId: cid(2) });
    const outcome = b.applyUpdate(log(forged, write));
    expect(outcome.applied).toBe(0);
    expect(outcome.refused).toBe(1);

    // The buffered op was waiting, not refused: the create releases it. The
    // forged one is gone for good, though its target is now reachable.
    expect(b.applyUpdate(log(create))).toEqual({ applied: 1, refused: 0 });
    expect(b.getMap("root").get("k")).toBe(1);
    expect(b.getMap("root").get("k2")).toBeUndefined();

    // A replay of what already landed is a duplicate, never a refusal.
    expect(b.applyUpdate(log(create, write))).toEqual({ applied: 0, refused: 0 });
  });

  it("applies the rest of a batch carrying one forgery", () => {
    const [create, write, later] = openedMap(1);

    // The everyday shape: one forgery riding a stream of honest ops. The refusal
    // is per op, not per batch.
    const b = new Doc({ clientId: cid(2) });
    const outcome = b.applyUpdate(log(forgeStampClient(later, cid(1)), create, write));
    expect(outcome).toEqual({ applied: 2, refused: 1 });
    expect(b.getMap("root").get("k")).toBe(1);
  });

  it("judges a malformed batch as neither applied nor refused", () => {
    const a = new Doc({ clientId: cid(1) });
    // Nothing decoded, so there is no op to judge.
    expect(a.applyUpdate(new Uint8Array([0xff, 0xff, 0xff, 0xff]))).toEqual({
      applied: -1,
      refused: 0,
    });
  });
});
