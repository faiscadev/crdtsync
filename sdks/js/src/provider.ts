// A sync provider binds a `Doc` to a crdtsync server over a WebSocket. It owns
// the wire session (a `WasmClient`) and one room channel; the `Doc` is backed by
// that channel, so local edits are framed + outboxed and sent, and inbound frames
// fold into the same replica and fire the doc's reactivity. On a dropped socket
// the outbox holds unacked edits; on reconnect the provider resumes the channel
// from its caught-up position and resends the outbox, so edits made offline
// converge once the link returns.

import { ClientBackend } from "./backend.js";
import { Doc } from "./doc.js";
import { keyBytes } from "./path.js";
import { WasmClient, protocolHeader } from "./wasm/crdtsync_wasm.js";

export type ConnectionState = "connecting" | "connected" | "disconnected";

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
  private readonly subscribeFrame: Uint8Array;
  private readonly credential: Uint8Array;
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
    const sub = this.client.subscribe(keyBytes(room));
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

  /** Close the connection and stop reconnecting. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.connectTimer);
    this.ws?.close();
    this.setState("disconnected");
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
    if (err !== null) this.handleServerError(err);
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
    if (Array.isArray(redirects) && redirects.length > 0) this.onRedirect?.(redirects);
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
