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
        "crdtsync_doc_dec",
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
        // diff
        "crdtsync_diff",
        // atomic transactions
        "crdtsync_doc_begin_atomic",
        "crdtsync_doc_commit_atomic",
        // undo / redo
        "typedef struct CrdtUndo CrdtUndo;",
        "crdtsync_undo_new",
        "crdtsync_undo_free",
        "crdtsync_undo_register_int",
        "crdtsync_undo_inc",
        "crdtsync_undo_dec",
        "crdtsync_undo_delete",
        "crdtsync_undo_list_insert",
        "crdtsync_undo_list_delete",
        "crdtsync_undo_text_insert",
        "crdtsync_undo_text_delete",
        "crdtsync_undo_undo",
        "crdtsync_undo_redo",
        "crdtsync_undo_can_undo",
        "crdtsync_undo_can_redo",
        // wire client session
        "typedef struct CrdtClient CrdtClient;",
        "crdtsync_client_new",
        "crdtsync_client_free",
        "crdtsync_client_hello",
        "crdtsync_client_subscribe",
        "crdtsync_client_receive",
        "crdtsync_client_last_seen_seq",
        "crdtsync_client_register_int",
        "crdtsync_client_inc",
        "crdtsync_client_dec",
        "crdtsync_client_set_bytes",
        "crdtsync_client_delete",
        "crdtsync_client_begin_atomic",
        "crdtsync_client_commit_atomic",
        "crdtsync_client_get_int",
        "crdtsync_client_get_bytes",
        // client auth + lifecycle + awareness
        "crdtsync_client_auth",
        "crdtsync_client_actor",
        "crdtsync_client_resume",
        "crdtsync_client_resend",
        "crdtsync_client_outbox_len",
        "crdtsync_client_unsubscribe",
        "crdtsync_client_set_awareness",
        "crdtsync_client_awareness",
        "crdtsync_client_awareness_len",
        // client named versions
        "crdtsync_client_create_version",
        "crdtsync_client_rename_version",
        "crdtsync_client_delete_version",
        "crdtsync_client_list_versions",
        "crdtsync_client_fetch_version",
        "crdtsync_client_version_count",
        "crdtsync_client_version_name",
        "crdtsync_client_version_state",
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
