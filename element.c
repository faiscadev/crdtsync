#include "element.h"
#include "counter.h"
#include "host.h"
#include "map.h"
#include "register.h"
#include "scalar.h"

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

Element element_clone(Arena *arena, Element e) {
    Element result;

    switch (e.kind) {
    case ELEMENT_SCALAR: {
        Scalar cloned = scalar_clone(arena, e.as.scalar);

        result = element_scalar(cloned);
    } break;
    case ELEMENT_REGISTER: {
        Register *reg = register_clone(arena, e.as.reg);
        result = element_register(reg);
    } break;
    case ELEMENT_COUNTER: {
        Counter *counter = counter_clone(arena, e.as.counter);
        result = element_counter(counter);
    } break;
    case ELEMENT_MAP: {
        Map *map = map_clone(arena, e.as.map);
        result = element_map(map);
    } break;
    }

    return result;
}
