//! Portable CRDT core.
//!
//! Pure logic — no I/O, no direct time/entropy. Platform effects (randomness,
//! clock) are injected through the [`Host`](host::Host) trait so the same core
//! runs natively (Go/Python via the C ABI) and in wasm (browser/JS).
//!
//! Composites are held as `Rc<RefCell<T>>`. The value graph is a downward tree
//! (Map -> children), so shared handles never form a cycle.
//!
//! Scaffold only — every body is `todo!()`. See RUST_REWRITE_PLAN.md.
#![forbid(unsafe_code)]
#![allow(dead_code)] // scaffold: fields/methods are unimplemented stubs

pub mod anchor;
pub mod host;

pub mod clientid;
pub mod elementid;
pub mod scalar;
pub mod stamp;

pub mod counter;
pub mod element;
pub mod list;
pub mod map;
pub mod register;
pub mod text;

pub mod client;
pub mod codec;
pub mod diff;
pub mod doc;
pub mod op;
pub mod path;
pub mod protocol;
pub mod undo;

pub use anchor::RelativePosition;
pub use client::{ClientError, ClientSession};
pub use clientid::ClientId;
pub use codec::{decode_op, decode_ops, encode_op, encode_ops, DecodeError};
pub use counter::Counter;
pub use doc::{Document, OrphanEvent};
pub use element::{Element, ElementKind};
pub use elementid::ElementId;
pub use host::Host;
pub use list::{Anchor, List, Side};
pub use map::Map;
pub use op::{Op, OpId, OpKind, Tx, TxId};
pub use protocol::{
    decode_header, decode_message, encode_header, encode_message, Channel, ErrorCode, Message,
    ProtocolError,
};
pub use register::Register;
pub use scalar::{BlobRef, Scalar};
pub use stamp::Stamp;
pub use text::Text;
pub use undo::UndoManager;
