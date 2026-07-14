package crdtsync

import (
	"bytes"
	"math"
	"testing"
)

func TestActorKeyIsDeterministicAnd16Bytes(t *testing.T) {
	k := ActorKey([]byte("user:alice"))
	if len(k) != 16 {
		t.Fatalf("the actor key is 16 bytes, got %d", len(k))
	}
	if !bytes.Equal(k, ActorKey([]byte("user:alice"))) {
		t.Fatal("the derivation is stable")
	}
	if bytes.Equal(k, ActorKey([]byte("user:bob"))) {
		t.Fatal("a different actor derives a different key")
	}
}

func TestAclGrantToAnActorKeyConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	subject := ActorKey([]byte("user:bob"))
	grantor := ActorKey([]byte("user:alice"))
	id, ops := a.AclGrant(SubjectActor, subject, CapabilityGrant(CapOwn), Allow, path("doc"), grantor)
	if len(id) != 16 || len(ops) == 0 {
		t.Fatal("a grant to an actor key emits an id and ops")
	}
	if n := b.Apply(ops); n < 1 {
		t.Fatalf("the grant converges, applied %d", n)
	}
}

func TestAclGrantReturnsIdAndConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	id, ops := a.AclGrant(SubjectActor, cid(7), CapabilityGrant(CapRead), Allow, path("doc"), cid(1))
	if len(id) != 16 {
		t.Fatalf("a grant hands back a 16-byte id, got %d", len(id))
	}
	if len(ops) == 0 {
		t.Fatal("a grant emits ops")
	}
	if n := b.Apply(ops); n < 1 {
		t.Fatalf("the grant tuple converges on a peer, applied %d", n)
	}
}

func TestAclRevokeByReturnedID(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	// A role deny to a group marshals through.
	id, ops := a.AclGrant(SubjectGroup, key("editors"), RoleGrant(key("reviewer")), Deny, path("doc"), cid(1))
	if len(id) != 16 || len(ops) == 0 {
		t.Fatal("a role/deny/group grant emits an id and ops")
	}
	b.Apply(ops)

	rev := a.AclRevoke(id)
	if len(rev) == 0 {
		t.Fatal("revoking a held tuple emits ops")
	}
	if n := b.Apply(rev); n < 1 {
		t.Fatalf("the revoke converges, applied %d", n)
	}
}

func TestAclClassSubjectMarshals(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	id, ops := a.AclGrant(SubjectAuthenticated, nil, CapabilityGrant(CapWrite), Allow, path("doc"), cid(1))
	if len(id) != 16 || len(ops) == 0 {
		t.Fatal("a class-subject grant emits an id and ops")
	}
	if n := b.Apply(ops); n < 1 {
		t.Fatalf("the grant converges, applied %d", n)
	}
}

func TestAclGrantIsTotalOnBadInput(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()

	// A malformed grantor (not 16 bytes) yields nil id and nil ops, no panic.
	id, ops := a.AclGrant(SubjectActor, cid(7), CapabilityGrant(CapOwn), Allow, path("doc"), key("short"))
	if id != nil || ops != nil {
		t.Fatalf("a malformed grantor is inert, got id=%v ops=%v", id, ops)
	}

	// An unknown subject kind is inert too.
	id, ops = a.AclGrant(SubjectKind(99), cid(7), CapabilityGrant(CapRead), Allow, path("doc"), cid(1))
	if id != nil || ops != nil {
		t.Fatal("an unknown subject kind is inert")
	}

	// A revoke of an id the replica never held is inert (no ops), not a panic.
	if rev := a.AclRevoke(make([]byte, 16)); len(rev) != 0 {
		t.Fatalf("revoking an unknown id emits nothing, got %d bytes", len(rev))
	}
}

func TestClientAclRoutesThroughTheOutbox(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	b := newClient(t, 2)
	defer b.Close()
	ca, _ := a.Subscribe(key("room-1"))
	cb, _ := b.Subscribe(key("room-1"))
	_ = cb

	id, frame := a.AclGrant(ca, SubjectActor, cid(7), CapabilityGrant(CapWrite), Allow, path("doc"), cid(1))
	if len(id) != 16 || len(frame) == 0 {
		t.Fatal("the client grant frames an Ops message and returns the id")
	}
	if n := a.OutboxLen(ca); n != 1 {
		t.Fatalf("the grant entered the outbox, got %d", n)
	}
	if rc, _ := b.Receive(frame); rc < 1 {
		t.Fatalf("the peer applies the grant: rc=%d", rc)
	}

	rev := a.AclRevoke(ca, id)
	if len(rev) == 0 {
		t.Fatal("the client revoke frames an Ops message")
	}
	if n := a.OutboxLen(ca); n != 2 {
		t.Fatalf("the revoke entered the outbox, got %d", n)
	}
	if rc, _ := b.Receive(rev); rc < 1 {
		t.Fatalf("the peer applies the revoke: rc=%d", rc)
	}

	// An ack through the tip drains the outbox.
	if rc, _ := a.Receive(acceptedThrough(ca, math.MaxUint64)); rc != 1 {
		t.Fatalf("ack applied: rc=%d", rc)
	}
	if n := a.OutboxLen(ca); n != 0 {
		t.Fatalf("ack drained the outbox, got %d", n)
	}
}

func TestClientAclOnAnUnheldChannelIsInert(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()

	id, frame := a.AclGrant(99, SubjectAnyone, nil, CapabilityGrant(CapRead), Allow, path("doc"), cid(1))
	if id != nil || len(frame) != 0 {
		t.Fatal("a grant on an unheld channel is inert")
	}
	if rev := a.AclRevoke(99, make([]byte, 16)); len(rev) != 0 {
		t.Fatal("a revoke on an unheld channel is inert")
	}
}
