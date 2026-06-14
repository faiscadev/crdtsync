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
// Ownership: composites are referenced by pointer; element_merge mutates
// dst's composite in place and never touches src's. Callers are
// responsible for keeping pointed-to composites alive (typically by
// putting them in the same arena as the containing Map).

#include "counter.h"
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
Element element_clone(Arena *arena, Element e);

#endif // _CRDT_ELEMENT_H
