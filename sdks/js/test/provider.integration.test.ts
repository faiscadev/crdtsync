import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { type Provider, connect } from "../src/index.js";

// The real crdtsync server, spawned in relay mode (no admin plane, no data dir),
// so two providers can sync a room over a real WebSocket. Skipped when the server
// binary is absent — build it with `cargo build -p crdtsync-server`.
const serverBin =
  process.env.CRDTSYNC_SERVER_BIN ??
  fileURLToPath(new URL("../../../target/debug/crdtsync-server", import.meta.url));

const hasServer = existsSync(serverBin);
const suite = hasServer ? describe : describe.skip;

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => resolve(port));
    });
  });
}

function startServer(port: number): Promise<ChildProcess> {
  const child = spawn(serverBin, [], {
    env: { ...process.env, CRDTSYNC_ADDR: `127.0.0.1:${port}` },
    stdio: ["ignore", "ignore", "pipe"],
  });
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("server did not start")), 10_000);
    child.stderr?.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("serving on")) {
        clearTimeout(timer);
        resolve(child);
      }
    });
    child.on("exit", (code) => reject(new Error(`server exited early (${code})`)));
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for a condition");
    await new Promise((r) => setTimeout(r, 15));
  }
}

suite("provider sync over a real server", () => {
  let server: ChildProcess;
  let url: string;
  const providers: Provider[] = [];

  beforeAll(async () => {
    const port = await freePort();
    server = await startServer(port);
    url = `ws://127.0.0.1:${port}`;
  });

  afterAll(() => {
    for (const p of providers) p.close();
    server?.kill("SIGKILL");
  });

  async function join(room: string): Promise<Provider> {
    const provider = await connect(url, room, { WebSocket });
    providers.push(provider);
    return provider;
  }

  it("syncs map, list, and text edits between two clients", async () => {
    const room = `room-${Date.now()}-a`;
    const a = await join(room);
    const b = await join(room);
    expect(a.state).toBe("connected");

    a.doc.getMap("root").set("title", "Hello");
    a.doc.getMap("root").set("n", 7);
    a.doc.getList("items").push("x").push("y");
    a.doc.getText("body").insert(0, "hi");

    await waitFor(() => b.doc.getMap("root").get("title") === "Hello");
    await waitFor(() => b.doc.getList("items").length === 2);
    expect(b.doc.getMap("root").get("n")).toBe(7);
    expect(b.doc.getList("items").toArray()).toEqual(["x", "y"]);
    await waitFor(() => b.doc.getText("body").toString() === "hi");
  });

  it("fires reactivity on a remote update", async () => {
    const room = `room-${Date.now()}-b`;
    const a = await join(room);
    const b = await join(room);

    const remote: string[] = [];
    b.doc.on("update", (e) => {
      if (e.origin === "remote") {
        for (const c of e.changes) if (c.kind === "update") remote.push(String(c.new));
      }
    });

    a.doc.getMap("root").set("k", "first");
    a.doc.getMap("root").set("k", "second");

    await waitFor(() => remote.includes("second"));
    expect(b.doc.getMap("root").get("k")).toBe("second");
  });

  it("catches a late joiner up to existing state", async () => {
    const room = `room-${Date.now()}-c`;
    const a = await join(room);
    a.doc.getMap("root").set("early", "value");
    // Give the edit a moment to reach the server before b subscribes.
    await new Promise((r) => setTimeout(r, 100));

    const b = await join(room);
    await waitFor(() => b.doc.getMap("root").get("early") === "value");
  });

  it("publishes awareness without error", async () => {
    const room = `room-${Date.now()}-d`;
    const a = await join(room);
    expect(() => a.setAwareness("cursor", "10")).not.toThrow();
  });
});
