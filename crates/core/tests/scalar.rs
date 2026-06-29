use crdtsync_core::Scalar;

#[test]
fn variants_hold_their_value() {
    assert_eq!(Scalar::Bool(true), Scalar::Bool(true));
    assert_eq!(Scalar::Int(42), Scalar::Int(42));
    assert_eq!(Scalar::Int(-7), Scalar::Int(-7));
    assert_eq!(Scalar::Int(i64::MIN), Scalar::Int(i64::MIN));
    assert_eq!(Scalar::Int(i64::MAX), Scalar::Int(i64::MAX));
}

// --- equality: same kind, equal payload ---

#[test]
fn null_eq_null() {
    assert_eq!(Scalar::Null, Scalar::Null);
}

#[test]
fn bool_eq_and_neq() {
    assert_eq!(Scalar::Bool(false), Scalar::Bool(false));
    assert_ne!(Scalar::Bool(true), Scalar::Bool(false));
}

#[test]
fn int_eq_and_neq() {
    assert_eq!(Scalar::Int(0), Scalar::Int(0));
    assert_ne!(Scalar::Int(42), Scalar::Int(43));
    assert_ne!(Scalar::Int(1), Scalar::Int(-1));
}

#[test]
fn string_eq_same_content() {
    assert_eq!(
        Scalar::Bytes(b"abc".to_vec()),
        Scalar::Bytes(b"abc".to_vec())
    );
}

#[test]
fn string_neq_same_length_different_content() {
    assert_ne!(
        Scalar::Bytes(b"abc".to_vec()),
        Scalar::Bytes(b"abd".to_vec())
    );
}

#[test]
fn string_neq_same_prefix_different_length() {
    assert_ne!(
        Scalar::Bytes(b"ab".to_vec()),
        Scalar::Bytes(b"abc".to_vec())
    );
}

#[test]
fn empty_strings_equal() {
    assert_eq!(Scalar::Bytes(vec![]), Scalar::Bytes(vec![]));
}

// Binary-safe: embedded NUL bytes are part of the value.
#[test]
fn embedded_nul_is_significant() {
    let a = Scalar::Bytes(vec![0x01, 0x00, 0x02]);
    let b = Scalar::Bytes(vec![0x01, 0x00, 0x02]);
    let c = Scalar::Bytes(vec![0x01, 0x00, 0x03]);
    assert_eq!(a, b);
    assert_ne!(a, c);
}

// --- cross-kind: never equal, even for "obvious" coincidences ---

#[test]
fn cross_kind_never_equal() {
    assert_ne!(Scalar::Null, Scalar::Bool(false));
    assert_ne!(Scalar::Null, Scalar::Int(0));
    assert_ne!(Scalar::Bool(false), Scalar::Int(0));
    assert_ne!(Scalar::Bool(true), Scalar::Int(1));
    assert_ne!(Scalar::Int(42), Scalar::Bytes(b"42".to_vec()));
    assert_ne!(Scalar::Bytes(vec![]), Scalar::Null);
}

// A clone owns independent bytes equal to the source.
#[test]
fn clone_equals_source_and_is_independent() {
    let src = Scalar::Bytes(b"hello".to_vec());
    let c = src.clone();
    assert_eq!(c, src);
    drop(src);
    assert_eq!(c, Scalar::Bytes(b"hello".to_vec()));
}
