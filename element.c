#include "element.h"
#include "counter.h"
#include "host.h"
#include "map.h"
#include "register.h"
#include "scalar.h"

ElementId element_id(Element e) {
    switch (e.kind) {
    case ELEMENT_SCALAR:
        host_abort("element_id: scalar elements have no id");
        break;
    case ELEMENT_REGISTER:
        return register_id(e.as.reg);
    case ELEMENT_COUNTER:
        return counter_id(e.as.counter);
    case ELEMENT_MAP:
        return map_id(e.as.map);
    }
}

Element element_scalar(Scalar s) {
    Element e = {.kind = ELEMENT_SCALAR, .as.scalar = s};
    return e;
}

Element element_register(Register *r) {
    Element e = {.kind = ELEMENT_REGISTER, .as.reg = r};
    return e;
}

Element element_counter(Counter *c) {
    Element e = {.kind = ELEMENT_COUNTER, .as.counter = c};
    return e;
}

Element element_map(Map *m) {
    Element e = {.kind = ELEMENT_MAP, .as.map = m};
    return e;
}

ElementKind element_kind(Element e) { return e.kind; }

const char *element_kind_name(ElementKind k) {
    switch (k) {
    case ELEMENT_SCALAR:
        return "SCALAR";
    case ELEMENT_REGISTER:
        return "REGISTER";
    case ELEMENT_COUNTER:
        return "COUNTER";
    case ELEMENT_MAP:
        return "MAP";
    }
}

void element_merge(Element dst, Element src) {
    if (dst.kind != src.kind) {
        host_abortf("element_merge: kind mismatch: dst is %s, src is %s",
                    element_kind_name(dst.kind), element_kind_name(src.kind));
    }

    switch (dst.kind) {
    case ELEMENT_SCALAR:
        host_abort("element_merge: cannot merge scalar elements");
        break;
    case ELEMENT_REGISTER:
        register_merge(dst.as.reg, src.as.reg);
        break;
    case ELEMENT_COUNTER:
        counter_merge(dst.as.counter, src.as.counter);
        break;
    case ELEMENT_MAP:
        map_merge(dst.as.map, src.as.map);
        break;
    }
}

Element element_clone(Element e) {
    Element result;

    switch (e.kind) {
    case ELEMENT_SCALAR: {
        Scalar cloned = scalar_clone(e.as.scalar);

        result = element_scalar(cloned);
    } break;
    case ELEMENT_REGISTER: {
        Register *reg = register_clone(e.as.reg);
        result = element_register(reg);
    } break;
    case ELEMENT_COUNTER: {
        Counter *counter = counter_clone(e.as.counter);
        result = element_counter(counter);
    } break;
    case ELEMENT_MAP: {
        Map *map = map_clone(e.as.map);
        result = element_map(map);
    } break;
    }

    return result;
}

void element_acquire(Element e) {
    switch (e.kind) {
    case ELEMENT_SCALAR:
        // No-op: scalar elements have no refcount.
        break;
    case ELEMENT_REGISTER:
        register_acquire(e.as.reg);
        break;
    case ELEMENT_COUNTER:
        counter_acquire(e.as.counter);
        break;
    case ELEMENT_MAP:
        map_acquire(e.as.map);
        break;
    }
}

void element_release(Element e) {
    switch (e.kind) {
    case ELEMENT_SCALAR:
        // Scalars have no refcount, but an owned (scalar_clone'd) string holds
        // host_malloc'd bytes that must be freed. Valid only on owned scalars
        // — slots always store owned copies.
        scalar_free(e.as.scalar);
        break;
    case ELEMENT_REGISTER:
        register_release(e.as.reg);
        break;
    case ELEMENT_COUNTER:
        counter_release(e.as.counter);
        break;
    case ELEMENT_MAP:
        map_release(e.as.map);
        break;
    }
}

void element_displace(Element e) {
    switch (e.kind) {
    case ELEMENT_SCALAR:
        // No-op: scalar elements are never displaced.
        break;
    case ELEMENT_REGISTER:
        register_displace(e.as.reg);
        break;
    case ELEMENT_COUNTER:
        counter_displace(e.as.counter);
        break;
    case ELEMENT_MAP:
        map_displace(e.as.map);
        break;
    }
}

bool element_is_displaced(Element e) {
    switch (e.kind) {
    case ELEMENT_SCALAR:
        return false; // scalar elements are never displaced
    case ELEMENT_REGISTER:
        return register_is_displaced(e.as.reg);
    case ELEMENT_COUNTER:
        return counter_is_displaced(e.as.counter);
    case ELEMENT_MAP:
        return map_is_displaced(e.as.map);
    }
}
