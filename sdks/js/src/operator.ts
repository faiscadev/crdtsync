// The operator-tier surface: versions, branches, ACL, diff, and room clone. This
// is a different interaction model from handle-graph editing — an async request
// framed over the wire, then its reply awaited and read back — so it lives on the
// `Provider` (which owns the socket), not on a handle. The enums and record shapes
// here mirror the Python SDK so the operator vocabulary is one cross-SDK contract.

import { type Key, encodePath, keyBytes } from "./path.js";

/** Who a doc-ACL grant targets. `Actor` names a 16-byte actor id, `Group` a
 * membership name; the rest are the well-known classes. */
export enum SubjectKind {
  Actor = 0,
  Group = 1,
  Authenticated = 2,
  Anonymous = 3,
  Anyone = 4,
}

/** A direct power a grant confers over a subtree. */
export enum Capability {
  Read = 0,
  Write = 1,
  PublishAwareness = 2,
  Own = 3,
}

/** Whether a grant allows or denies. */
export enum Effect {
  Allow = 0,
  Deny = 1,
}

/** Which pair of a room's states a `diff` compares. */
export enum DiffKind {
  /** Two of the room's saved versions. */
  Versions = 0,
  /** Two of the room's branches' HEADs. */
  Branches = 1,
}

/** The subject a grant targets. An `Actor` carries a 16-byte actor id (derive one
 * from a credential with `actorKey`); a `Group` a membership name; the classes
 * carry nothing. */
export type AclSubject =
  | { readonly kind: SubjectKind.Actor; readonly id: Uint8Array }
  | { readonly kind: SubjectKind.Group; readonly name: Key }
  | { readonly kind: SubjectKind.Authenticated }
  | { readonly kind: SubjectKind.Anonymous }
  | { readonly kind: SubjectKind.Anyone };

/** A doc-ACL grant to author. A grant confers exactly one of a `capability` or a
 * `role`. `effect` defaults to `Allow`, `path` to the whole document (root), and
 * `grantor` to this connection's authenticated actor. */
export interface AclGrant {
  readonly subject: AclSubject;
  readonly capability?: Capability;
  readonly role?: Key;
  readonly effect?: Effect;
  /** The subtree the grant covers, as an ergonomic key path; omit for the root. */
  readonly path?: readonly Key[];
  /** The 16-byte actor crediting the grant; defaults to this connection's actor. */
  readonly grantor?: Uint8Array;
}

/** One branch of a room. `forkPoint` is the history position it shares with its
 * parent, `head` its own high-water position, `published` whether it is a
 * read-only publish target. */
export interface Branch {
  readonly name: string;
  readonly forkPoint: number;
  readonly head: number;
  readonly published: boolean;
}

/** The C-ABI discriminants a grant resolves to: subject kind + bytes, grant kind
 * (0 capability / 1 role) + capability code + role bytes, effect, path bytes. */
export interface ResolvedGrant {
  subjectKind: number;
  subject: Uint8Array;
  grantKind: number;
  capability: number;
  role: Uint8Array;
  effect: number;
  path: Uint8Array;
}

const EMPTY = new Uint8Array();

/** Resolve an ergonomic grant to the wasm `aclGrant` argument discriminants,
 * enforcing the exactly-one-of capability/role rule. */
export function resolveGrant(grant: AclGrant): ResolvedGrant {
  if ((grant.capability === undefined) === (grant.role === undefined)) {
    throw new TypeError("crdtsync: a grant confers exactly one of a capability or a role");
  }
  const [subjectKind, subject] = resolveSubject(grant.subject);
  const usesRole = grant.role !== undefined;
  return {
    subjectKind,
    subject,
    grantKind: usesRole ? 1 : 0,
    capability: usesRole ? 0 : (grant.capability as Capability),
    role: usesRole ? keyBytes(grant.role as Key) : EMPTY,
    effect: grant.effect ?? Effect.Allow,
    path: encodePath(grant.path ?? []),
  };
}

function resolveSubject(subject: AclSubject): [number, Uint8Array] {
  switch (subject.kind) {
    case SubjectKind.Actor:
      if (subject.id.length !== 16) {
        throw new TypeError(`crdtsync: an actor id must be 16 bytes, got ${subject.id.length}`);
      }
      return [SubjectKind.Actor, subject.id];
    case SubjectKind.Group:
      return [SubjectKind.Group, keyBytes(subject.name)];
    default:
      return [subject.kind, EMPTY];
  }
}
