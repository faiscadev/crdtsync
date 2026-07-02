//! Binary codec — the stable encoding for ops, on the wire and on disk.
//!
//! An op log is the durable form of a document: a length-framed sequence of
//! encoded ops that replays back to the same state. Encoding is deterministic
//! (one op, one byte string) and little-endian; ids and client are 16 raw
//! bytes, text is UTF-8, bytes and strings are length-prefixed. Decoding is
//! total — malformed input yields a [`DecodeError`], never a panic.

use crate::clientid::ClientId;
use crate::elementid::ElementId;
use crate::list::{Anchor, Side};
use crate::op::{Op, OpId, OpKind, Tx, TxId};
use crate::scalar::{BlobRef, Scalar};
use crate::stamp::Stamp;

/// Why a byte string could not be decoded into an op.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DecodeError {
    /// The input ended before a field was fully read.
    UnexpectedEof,
    /// A tag byte named no known variant.
    BadTag { what: &'static str, tag: u8 },
    /// A text field held bytes that are not valid UTF-8.
    BadUtf8,
    /// Bytes remained after decoding a single op.
    TrailingBytes,
}

/// Encode one op to its byte string.
pub fn encode_op(op: &Op) -> Vec<u8> {
    let mut out = Vec::new();
    put_op(&mut out, op);
    out
}

/// Decode exactly one op; trailing bytes are an error.
pub fn decode_op(bytes: &[u8]) -> Result<Op, DecodeError> {
    let mut cur = Cursor::new(bytes);
    let op = cur.op()?;
    if cur.pos != bytes.len() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(op)
}

/// Encode an op log: each op length-framed so the stream is self-delimiting.
pub fn encode_ops(ops: &[Op]) -> Vec<u8> {
    let mut out = Vec::new();
    for op in ops {
        let body = encode_op(op);
        put_u32(&mut out, len_u32(body.len()));
        out.extend_from_slice(&body);
    }
    out
}

/// Decode a length-framed op log back into ops, in order.
pub fn decode_ops(bytes: &[u8]) -> Result<Vec<Op>, DecodeError> {
    let mut cur = Cursor::new(bytes);
    let mut ops = Vec::new();
    while cur.pos != bytes.len() {
        let len = cur.u32()? as usize;
        let frame = cur.take(len)?;
        ops.push(decode_op(frame)?);
    }
    Ok(ops)
}

// --- encode ---

pub(crate) fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

pub(crate) fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// A length as a u32, failing loudly rather than truncating — a 4 GiB single
/// field is pathological, and a silent wrap would corrupt the stream.
pub(crate) fn len_u32(n: usize) -> u32 {
    u32::try_from(n).expect("codec: length exceeds 4 GiB")
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, len_u32(b.len()));
    out.extend_from_slice(b);
}

pub(crate) fn put_stamp(out: &mut Vec<u8>, s: &Stamp) {
    put_u64(out, s.lamport);
    out.extend_from_slice(&s.client.as_bytes());
}

pub(crate) fn put_scalar(out: &mut Vec<u8>, s: &Scalar) {
    match s {
        Scalar::Null => put_u8(out, 0),
        Scalar::Bool(b) => {
            put_u8(out, 1);
            put_u8(out, *b as u8);
        }
        Scalar::Int(n) => {
            put_u8(out, 2);
            put_i64(out, *n);
        }
        Scalar::Bytes(b) => {
            put_u8(out, 3);
            put_bytes(out, b);
        }
        Scalar::BlobRef(r) => {
            put_u8(out, 4);
            out.extend_from_slice(&r.id);
            put_bytes(out, r.mime.as_bytes());
            put_u64(out, r.size);
            match &r.inline {
                None => put_u8(out, 0),
                Some(bytes) => {
                    put_u8(out, 1);
                    put_bytes(out, bytes);
                }
            }
        }
    }
}

pub(crate) fn put_anchor(out: &mut Vec<u8>, a: &Anchor) {
    match &a.parent {
        None => put_u8(out, 0),
        Some(p) => {
            put_u8(out, 1);
            put_stamp(out, p);
        }
    }
    put_u8(out, side_tag(a.side));
}

fn side_tag(side: Side) -> u8 {
    match side {
        Side::Left => 0,
        Side::Right => 1,
    }
}

fn put_opkind(out: &mut Vec<u8>, kind: &OpKind) {
    match kind {
        OpKind::RegisterSet { key, value } => {
            put_u8(out, 0);
            put_bytes(out, key);
            put_scalar(out, value);
        }
        OpKind::CounterInc { key, amount } => {
            put_u8(out, 1);
            put_bytes(out, key);
            put_u32(out, *amount);
        }
        OpKind::CounterDec { key, amount } => {
            put_u8(out, 2);
            put_bytes(out, key);
            put_u32(out, *amount);
        }
        OpKind::MapSet { key, value } => {
            put_u8(out, 3);
            put_bytes(out, key);
            put_scalar(out, value);
        }
        OpKind::MapDelete { key } => {
            put_u8(out, 4);
            put_bytes(out, key);
        }
        OpKind::MapCreate { key } => {
            put_u8(out, 5);
            put_bytes(out, key);
        }
        OpKind::ListCreate { key } => {
            put_u8(out, 6);
            put_bytes(out, key);
        }
        OpKind::ListInsert { value, anchor } => {
            put_u8(out, 7);
            put_scalar(out, value);
            put_anchor(out, anchor);
        }
        OpKind::ListDelete { id } => {
            put_u8(out, 8);
            put_stamp(out, id);
        }
        OpKind::TextCreate { key } => {
            put_u8(out, 9);
            put_bytes(out, key);
        }
        OpKind::TextInsert { s, anchor } => {
            put_u8(out, 10);
            put_bytes(out, s.as_bytes());
            put_anchor(out, anchor);
        }
        OpKind::TextDelete { ids } => {
            put_u8(out, 11);
            put_u32(out, len_u32(ids.len()));
            for id in ids {
                put_stamp(out, id);
            }
        }
    }
}

fn put_op(out: &mut Vec<u8>, op: &Op) {
    out.extend_from_slice(&op.id.client.as_bytes());
    put_u64(out, op.id.seq);
    put_stamp(out, &op.stamp);
    out.extend_from_slice(&op.target.as_bytes());
    put_opkind(out, &op.kind);
    match &op.tx {
        None => put_u8(out, 0),
        Some(tx) => {
            put_u8(out, 1);
            put_u64(out, tx.id.0);
            put_u32(out, tx.count);
        }
    }
}

// --- decode ---

pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Whether every byte has been consumed — the total-decode check.
    pub(crate) fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }

    /// The bytes not yet consumed.
    pub(crate) fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::UnexpectedEof)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(DecodeError::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    fn array16(&mut self) -> Result<[u8; 16], DecodeError> {
        let mut a = [0u8; 16];
        a.copy_from_slice(self.take(16)?);
        Ok(a)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        let mut a = [0u8; 2];
        a.copy_from_slice(self.take(2)?);
        Ok(u16::from_le_bytes(a))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        let mut a = [0u8; 4];
        a.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(a))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(a))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(i64::from_le_bytes(a))
    }

    pub(crate) fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    pub(crate) fn string(&mut self) -> Result<String, DecodeError> {
        let len = self.u32()? as usize;
        let raw = self.take(len)?;
        std::str::from_utf8(raw)
            .map(str::to_owned)
            .map_err(|_| DecodeError::BadUtf8)
    }

    pub(crate) fn client(&mut self) -> Result<ClientId, DecodeError> {
        Ok(ClientId::from_bytes(self.array16()?))
    }

    pub(crate) fn element_id(&mut self) -> Result<ElementId, DecodeError> {
        Ok(ElementId::from_bytes(self.array16()?))
    }

    pub(crate) fn stamp(&mut self) -> Result<Stamp, DecodeError> {
        let lamport = self.u64()?;
        let client = self.client()?;
        Ok(Stamp { lamport, client })
    }

    pub(crate) fn scalar(&mut self) -> Result<Scalar, DecodeError> {
        match self.u8()? {
            0 => Ok(Scalar::Null),
            1 => match self.u8()? {
                0 => Ok(Scalar::Bool(false)),
                1 => Ok(Scalar::Bool(true)),
                // A bool byte outside {0, 1} is non-canonical: reject it so an
                // encoding round-trips to the same bytes.
                tag => Err(DecodeError::BadTag {
                    what: "scalar bool",
                    tag,
                }),
            },
            2 => Ok(Scalar::Int(self.i64()?)),
            3 => Ok(Scalar::Bytes(self.bytes()?)),
            4 => {
                let id = self.array16()?;
                let mime = self.string()?;
                let size = self.u64()?;
                let inline = match self.u8()? {
                    0 => None,
                    1 => Some(self.bytes()?),
                    tag => {
                        return Err(DecodeError::BadTag {
                            what: "blob inline",
                            tag,
                        })
                    }
                };
                Ok(Scalar::BlobRef(BlobRef {
                    id,
                    mime,
                    size,
                    inline,
                }))
            }
            tag => Err(DecodeError::BadTag {
                what: "scalar",
                tag,
            }),
        }
    }

    pub(crate) fn anchor(&mut self) -> Result<Anchor, DecodeError> {
        let parent = match self.u8()? {
            0 => None,
            1 => Some(self.stamp()?),
            tag => {
                return Err(DecodeError::BadTag {
                    what: "anchor.parent",
                    tag,
                })
            }
        };
        let side = match self.u8()? {
            0 => Side::Left,
            1 => Side::Right,
            tag => return Err(DecodeError::BadTag { what: "side", tag }),
        };
        Ok(Anchor { parent, side })
    }

    fn opkind(&mut self) -> Result<OpKind, DecodeError> {
        Ok(match self.u8()? {
            0 => OpKind::RegisterSet {
                key: self.bytes()?,
                value: self.scalar()?,
            },
            1 => OpKind::CounterInc {
                key: self.bytes()?,
                amount: self.u32()?,
            },
            2 => OpKind::CounterDec {
                key: self.bytes()?,
                amount: self.u32()?,
            },
            3 => OpKind::MapSet {
                key: self.bytes()?,
                value: self.scalar()?,
            },
            4 => OpKind::MapDelete { key: self.bytes()? },
            5 => OpKind::MapCreate { key: self.bytes()? },
            6 => OpKind::ListCreate { key: self.bytes()? },
            7 => OpKind::ListInsert {
                value: self.scalar()?,
                anchor: self.anchor()?,
            },
            8 => OpKind::ListDelete { id: self.stamp()? },
            9 => OpKind::TextCreate { key: self.bytes()? },
            10 => OpKind::TextInsert {
                s: self.string()?,
                anchor: self.anchor()?,
            },
            11 => {
                let count = self.u32()? as usize;
                let mut ids = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    ids.push(self.stamp()?);
                }
                OpKind::TextDelete { ids }
            }
            tag => {
                return Err(DecodeError::BadTag {
                    what: "opkind",
                    tag,
                })
            }
        })
    }

    fn op(&mut self) -> Result<Op, DecodeError> {
        let client = self.client()?;
        let seq = self.u64()?;
        let stamp = self.stamp()?;
        let target = self.element_id()?;
        let kind = self.opkind()?;
        let tx = match self.u8()? {
            0 => None,
            1 => Some(Tx {
                id: TxId(self.u64()?),
                count: self.u32()?,
            }),
            tag => return Err(DecodeError::BadTag { what: "tx", tag }),
        };
        Ok(Op {
            id: OpId { client, seq },
            stamp,
            target,
            kind,
            tx,
        })
    }
}
