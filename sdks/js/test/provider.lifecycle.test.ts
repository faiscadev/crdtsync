import { describe, expect, it } from "vitest";
import { Provider, connect } from "../src/index.js";

// Drive the connection lifecycle deterministically with fake sockets — no server.

interface FakeHandlers {
  onopen: (() => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: (() => void) | null;
  onerror: ((event: unknown) => void) | null;
}

/** A socket that never opens and closes shortly after construction (refused). */
class RefusedSocket implements FakeHandlers {
  binaryType = "";
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  constructor(_url: string) {
    setTimeout(() => {
      this.readyState = 3;
      this.onclose?.();
    }, 2);
  }
  send(): void {}
  close(): void {
    this.readyState = 3;
  }
}

/** A socket that opens but the server never replies (a stuck handshake). */
class SilentSocket implements FakeHandlers {
  binaryType = "";
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  constructor(_url: string) {
    setTimeout(() => {
      this.readyState = 1;
      this.onopen?.();
    }, 2);
  }
  send(): void {}
  close(): void {
    this.readyState = 3;
    this.onclose?.();
  }
}

/** A socket the test scripts: it opens, records what was sent, and delivers frames
 * on demand. */
class ScriptedSocket implements FakeHandlers {
  static last: ScriptedSocket | null = null;
  binaryType = "";
  readyState = 0;
  sent: Uint8Array[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  constructor(_url: string) {
    ScriptedSocket.last = this;
    setTimeout(() => {
      this.readyState = 1;
      this.onopen?.();
    }, 2);
  }
  send(data: Uint8Array): void {
    this.sent.push(data);
  }
  close(): void {
    this.readyState = 3;
    this.onclose?.();
  }
  deliver(frame: Uint8Array): void {
    this.onmessage?.({
      data: frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength),
    });
  }
}

// Server frames: a tag byte, then the message's fields, all little-endian.
const TAG_OPS = 2;
const TAG_AUTH_OK = 7;
const TAG_FRONTIER = 54;

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

function u32(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n, true);
  return b;
}

function u64(n: bigint): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, n, true);
  return b;
}

function authOk(actor = "anonymous"): Uint8Array {
  const bytes = new TextEncoder().encode(actor);
  return concat([Uint8Array.of(TAG_AUTH_OK), u32(bytes.length), bytes]);
}

/** The catch-up reply for an empty room. */
function opsFrame(channel: number): Uint8Array {
  return concat([Uint8Array.of(TAG_OPS), u32(channel)]);
}

/** The frame a redacted catch-up leads with: the sequences its delta withholds and
 * the id-space position their ops reach. It carries none of the room. */
function frontier(channel: number, seqs: bigint[], reach = 0n): Uint8Array {
  return concat([
    Uint8Array.of(TAG_FRONTIER),
    u32(channel),
    u64(reach),
    u32(seqs.length),
    ...seqs.map(u64),
  ]);
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe("provider connection lifecycle", () => {
  it("rejects connect() when the socket is refused and reconnect is off", async () => {
    await expect(
      connect("ws://127.0.0.1:1", "room", {
        WebSocket: RefusedSocket as never,
        reconnect: false,
      }),
    ).rejects.toThrow();
  });

  it("rejects connect() on a timeout when the handshake never completes", async () => {
    await expect(
      connect("ws://127.0.0.1:1", "room", {
        WebSocket: SilentSocket as never,
        connectTimeoutMs: 40,
      }),
    ).rejects.toThrow(/timed out/);
  });

  it("rejects a pending connect() when close() is called first", async () => {
    const provider = new Provider("ws://127.0.0.1:1", "room", {
      WebSocket: SilentSocket as never,
    });
    const pending = provider.whenConnected();
    provider.close();
    await expect(pending).rejects.toThrow();
    expect(provider.state).toBe("disconnected");
  });

  it("does not complete the initial sync on a redacted catch-up's frontier", async () => {
    // The frontier leads the delta and carries none of the room, so opening the
    // socket to app traffic on it would let an edit author against an empty replica.
    // The gate is an allowlist on the catch-up reply's own tag and channel, so a
    // frame this SDK has never seen cannot reopen the window either.
    ScriptedSocket.last = null;
    const provider = new Provider("ws://127.0.0.1:1", "room", {
      WebSocket: ScriptedSocket as never,
      reconnect: false,
    });
    try {
      await sleep(20);
      const socket = ScriptedSocket.last as ScriptedSocket;
      socket.deliver(authOk());
      await sleep(10);

      socket.deliver(frontier(0, [0n, 1n], 7n));
      await sleep(20);
      expect(provider.state).not.toBe("connected");

      socket.deliver(opsFrame(0));
      await provider.whenConnected();
      expect(provider.state).toBe("connected");
    } finally {
      provider.close();
    }
  });
});
