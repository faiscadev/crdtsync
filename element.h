#ifndef _CRDT_ELEMENT_H
#define _CRDT_ELEMENT_H

// Element: tagged union over the four value kinds a Map slot can hold —
// SCALAR (inline value), or one of REGISTER / COUNTER / MAP (pointer to
// a separately-allocated composite).
//
// Constructors (element_scalar / _register / _counter / _map) tag the
// kind and stash the payload. element_kind reads the tag back.
//
// element_merge dispatches on dst's kind:
//   - REGISTER → register_merge(dst, src)
//   - COUNTER  → counter_merge(dst, src)
//   - MAP      → map_merge(dst, src)
//   - SCALAR   → host_abort. Scalars do not merge as elements; their LWW
//                lives at the slot level (in Map). Reaching this branch
//                is a programmer error.
//
// Ownership: composites are referenced by refcounted pointer; element_merge
// mutates dst's composite in place and never touches src's. element_acquire /
// _release / _displace / _is_displaced forward to the underlying composite
// (SCALAR is a no-op for acquire/displace and scalar_free on release). Callers
// are responsible for keeping pointed-to composites alive via the refcount.
//
// Sharp edge: element_release on a SCALAR frees the value's string bytes
// (scalar_free), so it is valid ONLY on an OWNED scalar — one produced by
// element_clone, or stored in a container that owns its copy (e.g. a Map slot,
// which clones on set). Do NOT call it on a borrowed-buffer scalar such as
// element_scalar(scalar_string(...)) or on a SCALAR Element returned by
// map_get — that would free memory still owned by the caller or the Map.

#include "counter.h"
#include "elementid.h"
#include "register.h"
#include "scalar.h"

typedef struct Map Map;
typedef struct Register Register;
typedef struct Counter Counter;

typedef enum ElementKind {
    ELEMENT_SCALAR,
    ELEMENT_REGISTER,
    ELEMENT_COUNTER,
    ELEMENT_MAP,
} ElementKind;

typedef struct Element {
    ElementKind kind;
    union {
        Scalar scalar;
        Register *reg;
        Counter *counter;
        Map *map;
    } as;
} Element;

ElementId element_id(Element e);
Element element_scalar(Scalar s);
Element element_register(Register *r);
Element element_counter(Counter *c);
Element element_map(Map *m);

ElementKind element_kind(Element e);
const char *element_kind_name(ElementKind k);
void element_merge(Element dst, Element src);
Element element_clone(Element e);

void element_acquire(Element e);
void element_release(Element e);

void element_displace(Element e);
bool element_is_displaced(Element e);

#endif // _CRDT_ELEMENT_H
