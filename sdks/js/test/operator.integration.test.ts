import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { type Provider, SubjectKind, connect } from "../src/index.js";
import { DiffKind, Effect } from "../src/index.js";
import { Capability } from "../src/index.js";

// The operator-tier surface (versions / branches / diff / clone / ACL) exercised
// against the real crdtsync server over a real WebSocket — the async request/reply
// path, distinct from handle-graph editing. Skipped when the server binary is
// absent — build it with `cargo build -p crdtsync-server`.
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

// A version captures the server's current room state, so an edit must reach the
// server before it is captured — settle briefly after editing.
async function settle(ms = 150): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

suite("operator surface over a real server", () => {
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

  it("creates, lists, fetches, renames, and deletes versions", async () => {
    const room = `op-${Date.now()}-versions`;
    const a = await join(room);
    a.doc.getMap("root").set("k", "v");
    await settle();

    expect(await a.createVersion("v1")).toContain("v1");
    expect(await a.listVersions()).toContain("v1");

    const snapshot = await a.fetchVersion("v1");
    expect(snapshot).toBeInstanceOf(Uint8Array);
    expect(snapshot.length).toBeGreaterThan(0);

    await expect(a.fetchVersion("missing")).rejects.toThrow();

    expect(await a.renameVersion("v1", "v2")).toEqual(["v2"]);
    expect(await a.deleteVersion("v2")).toEqual([]);
  });

  it("lists, forks, and deletes branches", async () => {
    const room = `op-${Date.now()}-branches`;
    const a = await join(room);
    a.doc.getMap("root").set("seed", 1);
    await settle();

    const initial = await a.listBranches();
    expect(initial.map((b) => b.name)).toContain("main");

    const forked = await a.forkBranch("feature", "main");
    expect(forked.map((b) => b.name)).toContain("feature");

    const afterDelete = await a.deleteBranch("feature");
    expect(afterDelete.map((b) => b.name)).not.toContain("feature");
  });

  it("forks and restores a branch from a saved version", async () => {
    const room = `op-${Date.now()}-restore`;
    const a = await join(room);
    a.doc.getMap("root").set("stage", "one");
    await settle();
    await a.createVersion("snap");

    const fromVersion = await a.forkBranchFromVersion("fromsnap", "snap");
    expect(fromVersion.map((b) => b.name)).toContain("fromsnap");

    const restored = await a.restoreBranch("restored", "snap");
    expect(restored.map((b) => b.name)).toContain("restored");
  });

  it("diffs two saved versions", async () => {
    const room = `op-${Date.now()}-diff`;
    const a = await join(room);
    a.doc.getMap("root").set("count", 1);
    await settle();
    await a.createVersion("va");

    a.doc.getMap("root").set("count", 2);
    await settle();
    await a.createVersion("vb");

    const changes = await a.diff(DiffKind.Versions, "va", "vb");
    expect(changes.length).toBeGreaterThan(0);
  });

  it("clones a room, reporting creation only once", async () => {
    const room = `op-${Date.now()}-clone`;
    const a = await join(room);
    a.doc.getMap("root").set("original", true);
    await settle();

    const dst = `${room}-copy`;
    expect(await a.cloneRoom(dst)).toBe(true);
    expect(await a.cloneRoom(dst)).toBe(false);
  });

  it("grants and revokes an ACL tuple", async () => {
    const room = `op-${Date.now()}-acl`;
    const a = await join(room);

    const id = a.aclGrant({
      subject: { kind: SubjectKind.Anyone },
      capability: Capability.Read,
      effect: Effect.Allow,
    });
    expect(id).toBeInstanceOf(Uint8Array);
    expect(id.length).toBe(16);

    expect(() => a.aclRevoke(id)).not.toThrow();
  });
});
