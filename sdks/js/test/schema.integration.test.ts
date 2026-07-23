import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { type Provider, type RepairStep, connect } from "../src/index.js";

// Schema binding over a networked Doc: a bound schema drives mark flavor on the
// client-channel replica, and a remote edit that overflows a bound fires the
// repair signal through the provider's applyRemote bracket. Skipped when the
// server binary is absent — build it with `cargo build -p crdtsync-server`.
const serverBin =
  process.env.CRDTSYNC_SERVER_BIN ??
  fileURLToPath(new URL("../../../target/debug/crdtsync-server", import.meta.url));

const hasServer = existsSync(serverBin);
const suite = hasServer ? describe : describe.skip;

const enc = new TextEncoder();
const REPAIR_SCHEMA = enc.encode(
  JSON.stringify({
    schema: "notes",
    version: 1,
    root: "Doc",
    types: {
      Doc: { kind: "map", children: { body: "Body" } },
      Body: { kind: "text", max: 5 },
    },
  }),
);

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

suite("schema binding over a real server", () => {
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

  it("fires the repair signal on a remote edit against a locally-bound schema", async () => {
    const room = `schema-${Date.now()}`;
    const a = await join(room);
    const b = await join(room);

    // Only a binds the schema — schema binding is a local replica concern.
    expect(a.doc.setSchema(REPAIR_SCHEMA)).toBe(true);

    const repaired: RepairStep[][] = [];
    a.doc.on("repair", (e) => {
      for (const p of e.paths) repaired.push(p);
    });

    // b (no schema) overflows the bounded body; a folds the ops and repairs.
    b.doc.getText("body").insert(0, "hello world");

    await waitFor(() => repaired.some((p) => p.length === 1 && p[0] === "body"));
    expect(a.doc.getText("body").toString()).toBe("hello world");
  });
});
