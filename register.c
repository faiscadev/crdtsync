#include "register.h"
#include "host.h"
#include "scalar.h"
#include <stdbool.h>

struct Register {
    ElementId id;
    Scalar value;
    Stamp stamp;

    size_t refcount;
    bool displaced;
};

Register *register_create(ElementId id, Scalar value, Stamp stamp) {
    Register *reg = host_malloc(sizeof(Register));
    if (!reg) {
        host_abortf("register_create: host_malloc OOM (requested %zu bytes for "
                    "Register)",
                    sizeof(Register));
    }
    reg->id = id;
    reg->value = scalar_clone(value);
    reg->stamp = stamp;
    reg->refcount = 1;
    reg->displaced = false;
    return reg;
}

ElementId register_id(const Register *reg) { return reg->id; }

Scalar register_read(const Register *reg) { return reg->value; }

void register_set(Register *reg, Scalar value, Stamp stamp) {
    if (stamp_gt(stamp, reg->stamp)) {
        scalar_free(reg->value);
        reg->value = scalar_clone(value);
        reg->stamp = stamp;
    }
}

void register_merge(Register *dst, const Register *src) {
    if (stamp_gt(src->stamp, dst->stamp)) {
        scalar_free(dst->value);
        dst->value = scalar_clone(src->value);
        dst->stamp = src->stamp;
    }
}

Register *register_clone(const Register *reg) {
    Register *clone = host_malloc(sizeof(Register));
    if (!clone) {
        host_abortf("register_clone: host_malloc OOM (requested %zu bytes for "
                    "Register)",
                    sizeof(Register));
    }
    clone->id = reg->id;
    clone->value = scalar_clone(reg->value);
    clone->stamp = reg->stamp;
    clone->refcount = 1;
    clone->displaced = false;
    return clone;
}

void register_acquire(Register *reg) { reg->refcount++; }

void register_release(Register *reg) {
    if (reg->refcount == 0) {
        host_abort("register_release: refcount already zero");
    }
    reg->refcount--;
    if (reg->refcount == 0) {
        scalar_free(reg->value);
        host_free(reg);
    }
}

void register_displace(Register *reg) { reg->displaced = true; }

bool register_is_displaced(const Register *reg) { return reg->displaced; }
