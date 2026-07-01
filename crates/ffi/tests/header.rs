//! The committed C header is what Go/Python compile against, so it must declare
//! the whole ABI. `build.rs` regenerates it from the source on every build; this
//! guards that generation ran and every exported symbol is present.

const HEADER: &str = include_str!("../include/crdtsync.h");

#[test]
fn the_header_declares_every_exported_symbol() {
    let required = [
        // types (the include guard is checked in the_header_is_c_and_cpp_safe)
        "typedef struct CrdtDoc CrdtDoc;",
        "} CrdtBuf;",
        "extern \"C\"",
        // lifecycle + buffer
        "crdtsync_doc_new",
        "crdtsync_doc_free",
        "crdtsync_buf_free",
        // map / scalar edits + reads
        "crdtsync_doc_register_int",
        "crdtsync_doc_inc",
        "crdtsync_doc_set_bytes",
        "crdtsync_doc_delete",
        "crdtsync_doc_get_int",
        "crdtsync_doc_get_counter",
        "crdtsync_doc_get_bytes",
        // list
        "crdtsync_doc_list_insert",
        "crdtsync_doc_list_delete",
        "crdtsync_doc_list_len",
        "crdtsync_doc_list_get",
        // text
        "crdtsync_doc_text_insert",
        "crdtsync_doc_text_delete",
        "crdtsync_doc_text_len",
        "crdtsync_doc_text_get",
        // sync
        "crdtsync_doc_apply",
    ];
    for sym in required {
        assert!(
            HEADER.contains(sym),
            "generated header is missing `{sym}` — did build.rs regenerate it?"
        );
    }
}

#[test]
fn the_header_is_c_and_cpp_safe() {
    // A functional guard needs both halves; the bare name also appears in the
    // `#ifndef` and closing comment, so check the `#define` independently.
    assert!(
        HEADER.contains("#ifndef CRDTSYNC_H"),
        "missing include guard #ifndef"
    );
    assert!(
        HEADER.contains("#define CRDTSYNC_H"),
        "missing include guard #define"
    );
    assert!(
        HEADER.contains("#ifdef __cplusplus"),
        "missing C++ extern \"C\" wrapper"
    );
    assert!(
        HEADER.contains("#include <stdint.h>"),
        "missing fixed-width int types"
    );
}
