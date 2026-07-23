import { describe, expect, it } from "vitest";
import { Capability, Effect, SubjectKind } from "../src/index.js";
import { resolveGrant } from "../src/operator.js";

// The pure grant-resolution marshaling — the exactly-one-of capability/role rule
// and subject discriminants — independent of any server.
describe("resolveGrant", () => {
  it("resolves a capability grant to an Anyone subject at the root", () => {
    const g = resolveGrant({ subject: { kind: SubjectKind.Anyone }, capability: Capability.Read });
    expect(g.subjectKind).toBe(SubjectKind.Anyone);
    expect(g.subject.length).toBe(0);
    expect(g.grantKind).toBe(0);
    expect(g.capability).toBe(Capability.Read);
    expect(g.role.length).toBe(0);
    expect(g.effect).toBe(Effect.Allow);
    expect(g.path.length).toBe(0);
  });

  it("resolves a role grant and a keyed subtree path", () => {
    const g = resolveGrant({
      subject: { kind: SubjectKind.Group, name: "editors" },
      role: "reviewer",
      effect: Effect.Deny,
      path: ["doc", "section"],
    });
    expect(g.subjectKind).toBe(SubjectKind.Group);
    expect(g.subject.length).toBeGreaterThan(0);
    expect(g.grantKind).toBe(1);
    expect(g.role.length).toBeGreaterThan(0);
    expect(g.effect).toBe(Effect.Deny);
    expect(g.path.length).toBeGreaterThan(0);
  });

  it("rejects a grant with both a capability and a role", () => {
    expect(() =>
      resolveGrant({
        subject: { kind: SubjectKind.Anyone },
        capability: Capability.Write,
        role: "x",
      }),
    ).toThrow(/exactly one/);
  });

  it("rejects a grant with neither a capability nor a role", () => {
    expect(() => resolveGrant({ subject: { kind: SubjectKind.Anyone } })).toThrow(/exactly one/);
  });

  it("rejects an actor subject whose id is not 16 bytes", () => {
    expect(() =>
      resolveGrant({
        subject: { kind: SubjectKind.Actor, id: new Uint8Array(8) },
        capability: Capability.Read,
      }),
    ).toThrow(/16 bytes/);
  });
});
