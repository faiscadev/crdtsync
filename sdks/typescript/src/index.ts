/**
 * crdtsync TypeScript SDK.
 *
 * Client-side API for browser + Node apps connecting to a crdtsync server.
 * Sibling of the OCaml / Python / Go / Rust / JVM SDKs.
 *
 * Surface lands per SDK-1 through SDK-11 — see ../../KANBAN.md:
 *  - Document open / connect
 *  - Map / List / Text / Register / Counter wrappers + observers
 *  - Anchors / RelativePosition
 *  - doc.transact()
 *  - Grapheme-aware Text helpers (Intl.Segmenter)
 *  - UUID v7 + sessionStorage client_id persistence
 *  - Reconnect logic + per-channel last_seen_seq
 *  - Token / auth helpers
 *  - Blob upload / fetch / url helpers (presigned URL flow + inline ≤4KB)
 *  - Error envelope decoding
 *
 * Design: see ../../ARCHITECTURE.md.
 */

export const version = "0.0.0";
