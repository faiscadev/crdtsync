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
});
