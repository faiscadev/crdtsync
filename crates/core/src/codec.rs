//! Binary codec — the stable encoding for ops, on the wire and on disk.
//!
//! An op log is the durable form of a document: a length-framed sequence of
//! encoded ops that replays back to the same state. Encoding is deterministic
//! (one op, one byte string) and little-endian; ids and client are 16 raw
//! bytes, text is UTF-8, bytes and strings are length-prefixed. Decoding is
//! total — malformed input yields a [`DecodeError`], never a panic.

use crate::acl::{AclEffect, AclGrant, AclSubject, Capability};
use crate::anchor::RelativePosition;
use crate::clientid::ClientId;
use crate::elementid::{ElementId, ElementKind};
use crate::list::{Anchor, Side};
use crate::op::{Op, OpId, OpKind, Tx, TxId};
use crate::ranged::{is_composite_payload_kind, RangeAnchor, RangedInit};
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

/// An optional byte string: a present-flag byte then the bytes when `Some`.
pub(crate) fn put_opt_bytes(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => put_u8(out, 0),
        Some(b) => {
            put_u8(out, 1);
            put_bytes(out, b);
        }
    }
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
        Scalar::ElementRef(id) => {
            put_u8(out, 5);
            out.extend_from_slice(&id.as_bytes());
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

/// A `RelativePosition`, tag-prefixed and self-delimiting: a bare boundary, or an
/// item edge carrying its bound stamp.
pub(crate) fn put_rel_position(out: &mut Vec<u8>, pos: &RelativePosition) {
    match pos {
        RelativePosition::Start => put_u8(out, 0),
        RelativePosition::End => put_u8(out, 1),
        RelativePosition::Before(s) => {
            put_u8(out, 2);
            put_stamp(out, s);
        }
        RelativePosition::After(s) => {
            put_u8(out, 3);
            put_stamp(out, s);
        }
    }
}

/// A range endpoint: the sequence id it lives in, then its position within.
pub(crate) fn put_range_anchor(out: &mut Vec<u8>, a: &RangeAnchor) {
    out.extend_from_slice(&a.seq.as_bytes());
    put_rel_position(out, &a.pos);
}

/// A RangedElement's create payload: tag `0` a leaf scalar, tag `1` a composite
/// container kind.
pub(crate) fn put_ranged_init(out: &mut Vec<u8>, init: &RangedInit) {
    match init {
        RangedInit::Scalar(value) => {
            put_u8(out, 0);
            put_scalar(out, value);
        }
        RangedInit::Composite(kind) => {
            put_u8(out, 1);
            put_u8(out, *kind as u8);
        }
    }
}

/// An ACL subject, tag-prefixed: the two carrying a payload (actor id, group
/// name) then their data, the three well-known classes bare.
pub(crate) fn put_acl_subject(out: &mut Vec<u8>, subject: &AclSubject) {
    match subject {
        AclSubject::Actor(id) => {
            put_u8(out, 0);
            out.extend_from_slice(&id.as_bytes());
        }
        AclSubject::Group(name) => {
            put_u8(out, 1);
            put_bytes(out, name);
        }
        AclSubject::Authenticated => put_u8(out, 2),
        AclSubject::Anonymous => put_u8(out, 3),
        AclSubject::Anyone => put_u8(out, 4),
    }
}

/// An ACL grant: tag `0` a capability (its own tag byte), tag `1` a role name.
pub(crate) fn put_acl_grant(out: &mut Vec<u8>, grant: &AclGrant) {
    match grant {
        AclGrant::Capability(cap) => {
            put_u8(out, 0);
            put_u8(out, capability_tag(*cap));
        }
        AclGrant::Role(name) => {
            put_u8(out, 1);
            put_bytes(out, name);
        }
    }
}

fn capability_tag(cap: Capability) -> u8 {
    match cap {
        Capability::Read => 0,
        Capability::Write => 1,
        Capability::PublishAwareness => 2,
        Capability::Own => 3,
    }
}

pub(crate) fn put_acl_effect(out: &mut Vec<u8>, effect: AclEffect) {
    put_u8(
        out,
        match effect {
            AclEffect::Allow => 0,
            AclEffect::Deny => 1,
        },
    );
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
        OpKind::XmlElementCreate { key, tag } => {
            put_u8(out, 12);
            put_bytes(out, key);
            put_bytes(out, tag);
        }
        OpKind::XmlFragmentCreate { key } => {
            put_u8(out, 13);
            put_bytes(out, key);
        }
        OpKind::XmlInsertChild { tag, anchor } => {
            put_u8(out, 14);
            match tag {
                Some(t) => {
                    put_u8(out, 1);
                    put_bytes(out, t);
                }
                None => put_u8(out, 0),
            }
            put_anchor(out, anchor);
        }
        OpKind::XmlMove { node, anchor } => {
            put_u8(out, 15);
            out.extend_from_slice(&node.as_bytes());
            put_anchor(out, anchor);
        }
        OpKind::RangedCreate {
            start,
            end,
            payload,
            name,
        } => {
            put_u8(out, 16);
            put_range_anchor(out, start);
            put_range_anchor(out, end);
            put_ranged_init(out, payload);
            put_opt_bytes(out, name.as_deref());
        }
        OpKind::RangedSetPayload { id, payload } => {
            put_u8(out, 17);
            out.extend_from_slice(&id.as_bytes());
            put_scalar(out, payload);
        }
        OpKind::RangedDelete { id } => {
            put_u8(out, 18);
            out.extend_from_slice(&id.as_bytes());
        }
        OpKind::AclGrant {
            subject,
            grant,
            effect,
            path,
            grantor,
        } => {
            put_u8(out, 19);
            put_acl_subject(out, subject);
            put_acl_grant(out, grant);
            put_acl_effect(out, *effect);
            put_bytes(out, path);
            out.extend_from_slice(&grantor.as_bytes());
        }
        OpKind::AclRevoke { id } => {
            put_u8(out, 20);
            out.extend_from_slice(&id.as_bytes());
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

    /// An optional byte string written by [`put_opt_bytes`].
    pub(crate) fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.bytes()?)),
            tag => Err(DecodeError::BadTag {
                what: "optional bytes present-flag",
                tag,
            }),
        }
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
            5 => Ok(Scalar::ElementRef(self.element_id()?)),
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

    pub(crate) fn rel_position(&mut self) -> Result<RelativePosition, DecodeError> {
        Ok(match self.u8()? {
            0 => RelativePosition::Start,
            1 => RelativePosition::End,
            2 => RelativePosition::Before(self.stamp()?),
            3 => RelativePosition::After(self.stamp()?),
            tag => {
                return Err(DecodeError::BadTag {
                    what: "relative position",
                    tag,
                })
            }
        })
    }

    pub(crate) fn range_anchor(&mut self) -> Result<RangeAnchor, DecodeError> {
        Ok(RangeAnchor {
            seq: self.element_id()?,
            pos: self.rel_position()?,
        })
    }

    fn ranged_init(&mut self) -> Result<RangedInit, DecodeError> {
        match self.u8()? {
            0 => Ok(RangedInit::Scalar(self.scalar()?)),
            1 => Ok(RangedInit::Composite(self.composite_payload_kind()?)),
            tag => Err(DecodeError::BadTag {
                what: "ranged create payload",
                tag,
            }),
        }
    }

    /// Decode one RangedElement composite-payload container kind (Map / List /
    /// Text), reporting the offending byte on an invalid kind. Shared by the op
    /// codec and the state codec, which encode the kind identically.
    pub(crate) fn composite_payload_kind(&mut self) -> Result<ElementKind, DecodeError> {
        let byte = self.u8()?;
        ElementKind::from_tag(byte)
            .filter(|k| is_composite_payload_kind(*k))
            .ok_or(DecodeError::BadTag {
                what: "ranged composite payload kind",
                tag: byte,
            })
    }

    pub(crate) fn acl_subject(&mut self) -> Result<AclSubject, DecodeError> {
        Ok(match self.u8()? {
            0 => AclSubject::Actor(self.client()?),
            1 => AclSubject::Group(self.bytes()?),
            2 => AclSubject::Authenticated,
            3 => AclSubject::Anonymous,
            4 => AclSubject::Anyone,
            tag => {
                return Err(DecodeError::BadTag {
                    what: "acl subject",
                    tag,
                })
            }
        })
    }

    pub(crate) fn acl_grant(&mut self) -> Result<AclGrant, DecodeError> {
        Ok(match self.u8()? {
            0 => AclGrant::Capability(self.capability()?),
            1 => AclGrant::Role(self.bytes()?),
            tag => {
                return Err(DecodeError::BadTag {
                    what: "acl grant",
                    tag,
                })
            }
        })
    }

    fn capability(&mut self) -> Result<Capability, DecodeError> {
        Ok(match self.u8()? {
            0 => Capability::Read,
            1 => Capability::Write,
            2 => Capability::PublishAwareness,
            3 => Capability::Own,
            tag => {
                return Err(DecodeError::BadTag {
                    what: "acl capability",
                    tag,
                })
            }
        })
    }

    pub(crate) fn acl_effect(&mut self) -> Result<AclEffect, DecodeError> {
        Ok(match self.u8()? {
            0 => AclEffect::Allow,
            1 => AclEffect::Deny,
            tag => {
                return Err(DecodeError::BadTag {
                    what: "acl effect",
                    tag,
                })
            }
        })
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
            12 => OpKind::XmlElementCreate {
                key: self.bytes()?,
                tag: self.bytes()?,
            },
            13 => OpKind::XmlFragmentCreate { key: self.bytes()? },
            14 => {
                let tag = match self.u8()? {
                    0 => None,
                    1 => Some(self.bytes()?),
                    other => {
                        return Err(DecodeError::BadTag {
                            what: "xml child tag present-flag",
                            tag: other,
                        })
                    }
                };
                OpKind::XmlInsertChild {
                    tag,
                    anchor: self.anchor()?,
                }
            }
            15 => OpKind::XmlMove {
                node: self.element_id()?,
                anchor: self.anchor()?,
            },
            16 => OpKind::RangedCreate {
                start: self.range_anchor()?,
                end: self.range_anchor()?,
                payload: self.ranged_init()?,
                name: self.opt_bytes()?,
            },
            17 => OpKind::RangedSetPayload {
                id: self.element_id()?,
                payload: self.scalar()?,
            },
            18 => OpKind::RangedDelete {
                id: self.element_id()?,
            },
            19 => OpKind::AclGrant {
                subject: self.acl_subject()?,
                grant: self.acl_grant()?,
                effect: self.acl_effect()?,
                path: self.bytes()?,
                grantor: self.client()?,
            },
            20 => OpKind::AclRevoke {
                id: self.element_id()?,
            },
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
