// A sync provider binds a `Doc` to a crdtsync server over a WebSocket. It owns
// the wire session (a `WasmClient`) and one room channel; the `Doc` is backed by
// that channel, so local edits are framed + outboxed and sent, and inbound frames
// fold into the same replica and fire the doc's reactivity. On a dropped socket
// the outbox holds unacked edits; on reconnect the provider resumes the channel
// from its caught-up position and resends the outbox, so edits made offline
// converge once the link returns.

import { ClientBackend } from "./backend.js";
import { type Change, remarshalChange } from "./changes.js";
import { Doc } from "./doc.js";
import { type AclGrant, type Branch, type DiffKind, resolveGrant } from "./operator.js";
import { type Key, keyBytes, keyString } from "./path.js";
import { WasmClient, actorKey, protocolHeader } from "./wasm/crdtsync_wasm.js";

export type ConnectionState = "connecting" | "connected" | "disconnected";

// One reply-frame notification `WasmClient.takeReplies` drains, correlating an
// awaited operator request to the reply that satisfies it.
type ReplyTag =
  | { kind: "versions"; channel: number }
  | { kind: "versionState"; channel: number; name: Uint8Array }
  | { kind: "branches"; room: Uint8Array }
  | { kind: "diff"; room: Uint8Array }
  | { kind: "clone"; dst: Uint8Array };

// An operator request awaiting its reply. `matches` tests whether a drained reply
// tag satisfies it; `settle` reads the result and resolves; `fail` rejects.
interface PendingRequest {
  matches(tag: ReplyTag): boolean;
  settle(): void;
  fail(err: Error): void;
}

// The minimal WebSocket surface the provider drives — the browser `WebSocket`
// and the Node `ws` package both satisfy it.
interface WebSocketLike {
  binaryType: string;
  readyState: number;
  send(data: Uint8Array): void;
  close(): void;
  onopen: (() => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: (() => void) | null;
  onerror: ((event: unknown) => void) | null;
}
type WebSocketCtor = new (url: string) => WebSocketLike;
const WS_OPEN = 1;

export interface ProviderOptions {
  /** A fixed 16-byte client id; a random one is minted when omitted. */
  clientId?: Uint8Array;
  /** The credential sent in the Auth frame (a dev server accepts any). */
  credential?: string | Uint8Array;
  /** A WebSocket implementation — required on a Node before global `WebSocket`
   * (e.g. `import { WebSocket } from "ws"`); defaults to `globalThis.WebSocket`. */
  WebSocket?: WebSocketCtor;
  /** Reconnect automatically on an unexpected close (default true). */
  reconnect?: boolean;
  /** The reconnect backoff ceiling in ms (default 10000). */
  maxReconnectDelayMs?: number;
  /** How long the first connection may take before `whenConnected` rejects (default 15000). */
  connectTimeoutMs?: number;
  /** How long an operator request (version/branch/diff/clone) waits for its reply
   * before it rejects (default 15000). */
  requestTimeoutMs?: number;
  /** A server Error mid-session — the code is the server `ErrorCode` (6 is UpdateRequired). */
  onError?: (code: number) => void;
  /** Op batches the server refused since the last frame (`{ channel, reason, ops }`). */
  onOpsRejected?: (rejected: unknown[]) => void;
  /** Room redirects to a leader the server issued (`{ room, leaderAddr }`). */
  onRedirect?: (redirects: unknown[]) => void;
}

export type StateListener = (state: ConnectionState) => void;

/** Open a provider and resolve once the room's initial state has synced. */
export function connect(
  url: string,
  room: string,
  options: ProviderOptions = {},
): Promise<Provider> {
  const provider = new Provider(url, room, options);
  return provider.whenConnected().then(() => provider);
}

export class Provider {
  /** The document this provider keeps in sync. Live immediately; empty until connected. */
  readonly doc: Doc;

  private readonly client: WasmClient;
  private readonly channel: number;
  private readonly room: Uint8Array;
  private readonly subscribeFrame: Uint8Array;
  private readonly credential: Uint8Array;
  private readonly requestTimeoutMs: number;
  // Operator requests awaiting a reply, oldest first. The server answers one
  // socket's requests in order, so the oldest pending is the one an out-of-band
  // error or a socket close belongs to.
  private readonly pending: PendingRequest[] = [];
  private readonly WebSocketImpl: WebSocketCtor;
  private readonly url: string;
  private readonly reconnectEnabled: boolean;
  private readonly maxReconnectDelayMs: number;
  private readonly onError?: (code: number) => void;
  private readonly onOpsRejected?: (rejected: unknown[]) => void;
  private readonly onRedirect?: (redirects: unknown[]) => void;

  private ws?: WebSocketLike;
  // "auth" awaits the AuthOk (the first frame on a socket); "catchup" awaits the
  // initial subscribe reply; "ready" is synced.
  private phase: "auth" | "catchup" | "ready" = "auth";
  private stateValue: ConnectionState = "connecting";
  private reconnectAttempt = 0;
  private closed = false;
  private connectedOnce = false;
  private settled = false;
  private connectTimer?: ReturnType<typeof setTimeout>;
  private connectedResolve?: () => void;
  private connectedReject?: (err: Error) => void;
  private readonly connectedPromise: Promise<void>;
  private readonly stateListeners = new Set<StateListener>();

  constructor(url: string, room: string, options: ProviderOptions = {}) {
    const impl = options.WebSocket ?? (globalThis as { WebSocket?: WebSocketCtor }).WebSocket;
    if (!impl) {
      throw new Error(
        "crdtsync: no WebSocket available — pass options.WebSocket (e.g. from the `ws` package)",
      );
    }
    this.WebSocketImpl = impl;
    this.url = url;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 15_000;
    this.reconnectEnabled = options.reconnect ?? true;
    this.maxReconnectDelayMs = options.maxReconnectDelayMs ?? 10_000;
    this.onError = options.onError;
    this.onOpsRejected = options.onOpsRejected;
    this.onRedirect = options.onRedirect;
    this.credential =
      options.credential === undefined
        ? keyBytes("anonymous")
        : typeof options.credential === "string"
          ? keyBytes(options.credential)
          : options.credential;

    const clientId = options.clientId ?? randomClientId();
    this.client = new WasmClient(clientId);
    this.room = keyBytes(room);
    const sub = this.client.subscribe(this.room);
    this.channel = sub.channel;
    this.subscribeFrame = sub.frame;
    this.doc = Doc.networked(new ClientBackend(this.client, this.channel), (frame) =>
      this.sendIfOpen(frame),
    );

    this.connectedPromise = new Promise<void>((resolve, reject) => {
      this.connectedResolve = resolve;
      this.connectedReject = reject;
    });
    // Bound the first connection so `whenConnected()` never hangs on a dead server.
    this.connectTimer = setTimeout(() => {
      if (!this.connectedOnce) this.fatal(new Error("crdtsync: connection timed out"));
    }, options.connectTimeoutMs ?? 15_000);
    this.open();
  }

  /** The current connection state. */
  get state(): ConnectionState {
    return this.stateValue;
  }

  /** Resolve once the room's initial state has synced; reject if the first
   * connection fails (a server error, a timeout, or `close()` before sync). */
  whenConnected(): Promise<void> {
    return this.connectedPromise;
  }

  /** Observe connection-state transitions. Returns an unsubscribe. */
  onState(listener: StateListener): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  /** Publish an ephemeral awareness entry (presence) for this client. */
  setAwareness(key: string, value: string | Uint8Array): void {
    const frame = this.client.setAwareness(this.channel, keyBytes(key), keyBytes(value));
    if (frame) this.sendIfOpen(frame);
  }

  // ── Operator-tier surface: versions, branches, ACL, diff, clone ──────────────
  // Each of these (bar ACL, which rides the op path) is an async request/reply:
  // frame a request, send it, await the matching reply, read the result back. The
  // room this provider is subscribed to is the implicit target.

  /** The room's saved version names, in order. */
  listVersions(): Promise<string[]> {
    return this.request(
      this.client.listVersions(this.channel),
      (t) => t.kind === "versions" && t.channel === this.channel,
      () => this.readVersions(),
    );
  }

  /** Capture the room's current state as version `name`; resolves to the room's
   * version names after the capture. */
  createVersion(name: Key): Promise<string[]> {
    return this.request(
      this.client.createVersion(this.channel, keyBytes(name)),
      (t) => t.kind === "versions" && t.channel === this.channel,
      () => this.readVersions(),
    );
  }

  /** Rename version `from` to `to`; resolves to the room's version names after. */
  renameVersion(from: Key, to: Key): Promise<string[]> {
    return this.request(
      this.client.renameVersion(this.channel, keyBytes(from), keyBytes(to)),
      (t) => t.kind === "versions" && t.channel === this.channel,
      () => this.readVersions(),
    );
  }

  /** Delete version `name`; resolves to the room's version names after. */
  deleteVersion(name: Key): Promise<string[]> {
    return this.request(
      this.client.deleteVersion(this.channel, keyBytes(name)),
      (t) => t.kind === "versions" && t.channel === this.channel,
      () => this.readVersions(),
    );
  }

  /** The captured snapshot of version `name` (a canonical state buffer). Rejects
   * if the room has no such version. */
  fetchVersion(name: Key): Promise<Uint8Array> {
    const nameBytes = keyBytes(name);
    return this.request(
      this.client.fetchVersion(this.channel, nameBytes),
      // A hit answers with the state; a miss with the version list — accept both,
      // then a missing cached state is the not-found error.
      (t) =>
        (t.kind === "versionState" &&
          t.channel === this.channel &&
          bytesEqual(t.name, nameBytes)) ||
        (t.kind === "versions" && t.channel === this.channel),
      () => {
        const state = this.client.versionState(this.channel, nameBytes);
        if (!state) throw new Error(`crdtsync: no version named ${describe(name)}`);
        return state;
      },
    );
  }

  /** The room's branches, in order. */
  listBranches(): Promise<Branch[]> {
    return this.request(this.client.listBranches(this.room), this.branchReply, () =>
      this.readBranches(),
    );
  }

  /** Fork branch `name` off branch `from`'s HEAD; resolves to the room's branches. */
  forkBranch(name: Key, from: Key): Promise<Branch[]> {
    return this.request(
      this.client.forkBranch(this.room, keyBytes(name), keyBytes(from)),
      this.branchReply,
      () => this.readBranches(),
    );
  }

  /** Fork branch `name` off the snapshot of `version`; resolves to the branches. */
  forkBranchFromVersion(name: Key, version: Key): Promise<Branch[]> {
    return this.request(
      this.client.forkBranchFromVersion(this.room, keyBytes(name), keyBytes(version)),
      this.branchReply,
      () => this.readBranches(),
    );
  }

  /** Restore the room to `version` as a fresh branch `name`; resolves to the
   * branches. */
  restoreBranch(name: Key, version: Key): Promise<Branch[]> {
    return this.request(
      this.client.restoreBranch(this.room, keyBytes(name), keyBytes(version)),
      this.branchReply,
      () => this.readBranches(),
    );
  }

  /** Publish the room's active editor branch onto the read-only `published`
   * branch; resolves to the branches. */
  publishBranch(published: Key): Promise<Branch[]> {
    return this.request(
      this.client.publishBranch(this.room, keyBytes(published)),
      this.branchReply,
      () => this.readBranches(),
    );
  }

  /** Delete branch `name` (the default `main` is never deletable); resolves to
   * the branches. */
  deleteBranch(name: Key): Promise<Branch[]> {
    return this.request(this.client.deleteBranch(this.room, keyBytes(name)), this.branchReply, () =>
      this.readBranches(),
    );
  }

  /** The structural diff turning state `a` into state `b`, over the room's saved
   * versions (`DiffKind.Versions`) or its branches' HEADs (`DiffKind.Branches`). */
  diff(kind: DiffKind, a: Key, b: Key): Promise<Change[]> {
    return this.request(
      this.client.diffQuery(this.room, kind, keyBytes(a), keyBytes(b)),
      (t) => t.kind === "diff" && bytesEqual(t.room, this.room),
      () => this.readDiff(),
    );
  }

  /** Duplicate the room's live state into a fresh room `dst`. Resolves to whether
   * `dst` was created (`false` when it already existed). */
  cloneRoom(dst: Key): Promise<boolean> {
    const dstBytes = keyBytes(dst);
    return this.request(
      this.client.cloneRoom(this.room, dstBytes),
      (t) => t.kind === "clone" && bytesEqual(t.dst, dstBytes),
      () => this.client.cloneResult(dstBytes) === true,
    );
  }

  /** Author a doc-ACL grant over the room, routed through the op path (acked and
   * resent like an edit). Returns the tuple id `revokeAcl` names it by. */
  aclGrant(grant: AclGrant): Uint8Array {
    const g = resolveGrant(grant);
    // The grantor is a 16-byte doc-ACL actor key: default to this connection's
    // authenticated actor, keyed the same way a matched `Actor` subject is.
    const actor = this.client.actor();
    const grantor = grant.grantor ?? (actor ? actorKey(actor) : undefined);
    if (!grantor) {
      throw new Error("crdtsync: no authenticated actor to credit the grant; pass grant.grantor");
    }
    const result = this.client.aclGrant(
      this.channel,
      g.subjectKind,
      g.subject,
      g.grantKind,
      g.capability,
      g.role,
      g.effect,
      g.path,
      grantor,
    ) as { id: Uint8Array; frame: Uint8Array } | undefined;
    if (!result) throw new Error("crdtsync: the room's channel is not held");
    this.sendIfOpen(result.frame);
    return result.id;
  }

  /** Revoke a doc-ACL tuple by the id `aclGrant` returned, routed through the op
   * path. */
  aclRevoke(tupleId: Uint8Array): void {
    this.sendIfOpen(this.client.aclRevoke(this.channel, tupleId));
  }

  private readonly branchReply = (t: ReplyTag): boolean =>
    t.kind === "branches" && bytesEqual(t.room, this.room);

  private readVersions(): string[] {
    return this.client.versions(this.channel).map(keyString);
  }

  private readBranches(): Branch[] {
    const raw = this.client.branches(this.room) as RawBranch[];
    return raw.map((b) => ({
      name: keyString(b.name),
      forkPoint: b.forkPoint,
      head: b.head,
      published: b.published,
    }));
  }

  private readDiff(): Change[] {
    const raw = this.client.diff(this.room) as unknown[] | null;
    if (!raw) return [];
    return raw.map((r) => remarshalChange(r as never).change);
  }

  // Frame + send an operator request and await the reply that satisfies `matches`,
  // returning what `read` extracts at the moment the reply folds. Rejects if the
  // channel isn't held, the provider isn't connected, or the reply never comes.
  private request<T>(
    frame: Uint8Array | undefined,
    matches: (tag: ReplyTag) => boolean,
    read: () => T,
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      if (frame === undefined) {
        reject(new Error("crdtsync: the room's channel is not held"));
        return;
      }
      if (this.phase !== "ready" || this.ws?.readyState !== WS_OPEN) {
        reject(new Error("crdtsync: not connected"));
        return;
      }
      const timer = setTimeout(() => {
        const i = this.pending.indexOf(entry);
        if (i >= 0) this.pending.splice(i, 1);
        reject(new Error("crdtsync: operator request timed out"));
      }, this.requestTimeoutMs);
      const entry: PendingRequest = {
        matches,
        settle: () => {
          clearTimeout(timer);
          try {
            resolve(read());
          } catch (e) {
            reject(e as Error);
          }
        },
        fail: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      };
      this.pending.push(entry);
      this.sendIfOpen(frame);
    });
  }

  /** Close the connection and stop reconnecting. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.connectTimer);
    this.ws?.close();
    this.setState("disconnected");
    this.failAllPending(new Error("crdtsync: closed"));
    this.reject(new Error("crdtsync: closed before it synced"));
  }

  private open(): void {
    this.phase = "auth";
    this.setState("connecting");
    const ws = new this.WebSocketImpl(this.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    ws.onopen = (): void => {
      ws.send(protocolHeader());
      ws.send(this.client.hello());
      ws.send(this.client.auth(this.credential));
    };
    ws.onmessage = (event): void => this.onMessage(toBytes(event.data));
    ws.onclose = (): void => this.onClose();
    ws.onerror = (): void => {
      /* a following close drives reconnect */
    };
  }

  private onMessage(data: Uint8Array): void {
    if (this.phase === "auth") {
      // The first frame on any socket is the AuthOk (or a server Error). Once it
      // folds cleanly this socket has authenticated — send Subscribe (or Resume on
      // a reconnect) and replay the outbox. `actor()` can't gate this: it stays set
      // across reconnects, so it would pass before the new socket re-authenticates.
      const err = this.receive(data);
      if (err !== null) {
        this.handleServerError(err);
        return;
      }
      this.sendIfOpen(this.connectedOnce ? this.resumeFrame() : this.subscribeFrame);
      this.resendOutbox();
      if (this.connectedOnce) {
        this.markConnected(); // reconnect: the replica persists; deltas stream as ops
      } else {
        this.phase = "catchup";
      }
      return;
    }

    // Bracket the receive so the doc's diff-based reactivity fires for inbound ops.
    let err: number | null = null;
    this.doc.applyRemote(() => {
      err = this.receive(data);
    });
    if (err !== null) {
      // A version/branch/diff/clone request the server refused answers with an
      // Error, not a reply, so it rejects the request awaiting it. The room's
      // leader answers that room's requests over this one socket in order, so the
      // refused request is the oldest still pending. The only mid-session Error
      // that is *not* a request refusal is UpdateRequired (6), a session-level
      // push — it routes to onError instead (edits refuse via onOpsRejected, never
      // an Error frame; handshake errors land before any request can be pending).
      if (err !== 6 && this.pending.length > 0) {
        this.pending.shift()?.fail(new Error(`crdtsync: server refused the request (code ${err})`));
      } else {
        this.handleServerError(err);
      }
    } else {
      this.drainReplies();
    }
    this.drainSignals();

    // The subscribe reply (a catch-up Ops/Snapshot) advances last_seen_seq; a bare
    // awareness frame does not, so it can't prematurely mark the initial sync done.
    if (this.phase === "catchup" && this.client.lastSeenSeq(this.channel) !== undefined) {
      this.markConnected();
    }
  }

  /** Fold one inbound frame; return the server `ErrorCode` when it was an Error. */
  private receive(data: Uint8Array): number | null {
    try {
      this.client.receive(data);
      return null;
    } catch (e) {
      return typeof e === "number" ? e : -1;
    }
  }

  private handleServerError(code: number): void {
    if (!this.connectedOnce) {
      // A handshake-time error (bad auth, unsupported version) is fatal.
      this.fatal(new Error(`crdtsync: server rejected the connection (code ${code})`));
    } else {
      this.onError?.(code);
    }
  }

  private drainSignals(): void {
    const rejected = this.client.takeRejected() as unknown[];
    if (Array.isArray(rejected) && rejected.length > 0) this.onOpsRejected?.(rejected);
    const redirects = this.client.takeRedirects() as unknown[];
    if (Array.isArray(redirects) && redirects.length > 0) {
      // A version/branch mutation routed to a non-leader is redirected, not
      // answered — reject the request awaiting it so it doesn't hang to timeout.
      if (this.pending.length > 0) {
        this.pending.shift()?.fail(new Error("crdtsync: request redirected to the room's leader"));
      }
      this.onRedirect?.(redirects);
    }
  }

  // Match each drained reply to the oldest pending request that accepts it, then
  // settle that request by reading the reply's result.
  private drainReplies(): void {
    const replies = this.client.takeReplies() as ReplyTag[];
    for (const tag of replies) {
      const index = this.pending.findIndex((p) => p.matches(tag));
      if (index >= 0) {
        const [entry] = this.pending.splice(index, 1);
        entry.settle();
      }
    }
  }

  private failAllPending(err: Error): void {
    const entries = this.pending.splice(0);
    for (const entry of entries) entry.fail(err);
  }

  private markConnected(): void {
    this.phase = "ready";
    this.connectedOnce = true;
    this.reconnectAttempt = 0;
    clearTimeout(this.connectTimer);
    this.setState("connected");
    this.resolve();
  }

  private resumeFrame(): Uint8Array {
    return this.client.resume(this.channel) ?? this.subscribeFrame;
  }

  private resendOutbox(): void {
    if (this.client.outboxLen(this.channel) > 0) {
      const frame = this.client.resend(this.channel);
      if (frame) this.sendIfOpen(frame);
    }
  }

  private fatal(err: Error): void {
    this.closed = true;
    clearTimeout(this.connectTimer);
    this.ws?.close();
    this.setState("disconnected");
    this.reject(err);
  }

  private onClose(): void {
    if (this.closed) return;
    // A dropped socket abandons any request awaiting a reply — it was never
    // outboxed, so the next socket won't answer it.
    this.failAllPending(new Error("crdtsync: connection lost before the reply arrived"));
    this.setState("disconnected");
    if (!this.reconnectEnabled) {
      if (!this.connectedOnce) this.reject(new Error("crdtsync: closed before it synced"));
      return;
    }
    const delay = Math.min(this.maxReconnectDelayMs, 250 * 2 ** this.reconnectAttempt);
    this.reconnectAttempt += 1;
    setTimeout(() => {
      if (!this.closed) this.open();
    }, delay);
  }

  private sendIfOpen(frame: Uint8Array): void {
    if (frame.length > 0 && this.ws?.readyState === WS_OPEN) this.ws.send(frame);
  }

  private resolve(): void {
    if (this.settled) return;
    this.settled = true;
    this.connectedResolve?.();
  }

  private reject(err: Error): void {
    if (this.settled) return;
    this.settled = true;
    clearTimeout(this.connectTimer);
    this.connectedReject?.(err);
  }

  private setState(state: ConnectionState): void {
    if (state === this.stateValue) return;
    this.stateValue = state;
    for (const listener of [...this.stateListeners]) listener(state);
  }
}

// The branch record shape `WasmClient.branches` yields, before ergonomic naming.
interface RawBranch {
  name: Uint8Array;
  forkPoint: number;
  head: number;
  published: boolean;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/** Render a key for an error message — a string as itself, bytes as their length. */
function describe(key: Key): string {
  return typeof key === "string" ? `"${key}"` : `${key.length} bytes`;
}

function toBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  throw new TypeError("crdtsync: unexpected non-binary WebSocket frame");
}

function randomClientId(): Uint8Array {
  const id = new Uint8Array(16);
  globalThis.crypto.getRandomValues(id);
  return id;
}
