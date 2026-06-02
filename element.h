#ifndef _CRDT_ELEMENT_H
#define _CRDT_ELEMENT_H

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

Element element_scalar(Scalar s);
Element element_register(Register *r);
Element element_counter(Counter *c);
Element element_map(Map *m);

ElementKind element_kind(Element e);
const char *element_kind_name(ElementKind k);
void element_merge(Element dst, Element src);

#endif // _CRDT_ELEMENT_H
