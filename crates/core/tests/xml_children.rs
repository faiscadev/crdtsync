//! XmlElement / XmlFragment children through the op layer — inserting nested
//! elements and text runs into a children sequence, deleting them, and
//! converging the tree. Builds on 1b (create + attrs).

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// The children of the XML node in slot `key`, rendered in order: an element as
/// `<tag>`, a text run as its string in quotes.
fn children_of(d: &Document, key: &[u8]) -> Vec<String> {
    let el = d.get(key);
    let children = match &el {
        Some(Element::XmlElement(x)) => x.borrow().children(),
        Some(Element::XmlFragment(f)) => f.borrow().children(),
        _ => return Vec::new(),
    };
    let vals = children.borrow().values();
    vals.iter().map(render).collect()
}

fn render(e: &Element) -> String {
    match e {
        Element::XmlElement(x) => format!("<{}>", String::from_utf8_lossy(x.borrow().tag())),
        Element::Text(t) => format!("{:?}", t.borrow().as_string()),
        Element::Scalar(s) => format!("S{s:?}"),
        _ => "?".to_string(),
    }
}

/// The tag of the `index`-th child element of the node in slot `key`.
fn child_tag(d: &Document, key: &[u8], index: usize) -> Option<Vec<u8>> {
    let el = d.get(key)?;
    let children = match &el {
        Element::XmlElement(x) => x.borrow().children(),
        Element::XmlFragment(f) => f.borrow().children(),
        _ => return None,
    };
    let v = children.borrow().get(index)?;
    match v {
        Element::XmlElement(x) => Some(x.borrow().tag().to_vec()),
        _ => None,
    }
}

#[test]
fn inserts_element_children_in_order() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"h1");
        kids.insert_element(1, b"p");
    });
    assert_eq!(children_of(&d, b"doc"), vec!["<h1>", "<p>"]);
}

#[test]
fn inserts_a_text_run_child() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"p");
        body.children().insert_text(0).insert(0, "hello");
    });
    assert_eq!(children_of(&d, b"doc"), vec!["\"hello\""]);
}

#[test]
fn mixes_elements_and_text() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut p = tx.xml_element(b"doc", b"p");
        let mut kids = p.children();
        kids.insert_text(0).insert(0, "a");
        kids.insert_element(1, b"b");
        kids.insert_text(2).insert(0, "c");
    });
    assert_eq!(children_of(&d, b"doc"), vec!["\"a\"", "<b>", "\"c\""]);
}

#[test]
fn deletes_a_child() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"h1");
        kids.insert_element(1, b"p");
        kids.insert_element(2, b"footer");
    });
    d.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        body.children().delete(1);
    });
    assert_eq!(children_of(&d, b"doc"), vec!["<h1>", "<footer>"]);
}

#[test]
fn a_nested_child_carries_its_own_attrs_and_grandchildren() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        let mut kids = body.children();
        let mut section = kids.insert_element(0, b"section");
        section.attrs().register(b"id", Scalar::Int(3));
        section.children().insert_element(0, b"span");
    });

    assert_eq!(child_tag(&d, b"doc", 0), Some(b"section".to_vec()));
    // Walk into the section to read its attr and grandchild.
    let body = d.get(b"doc").unwrap();
    let Element::XmlElement(body) = body else {
        panic!("body not an element")
    };
    let section = body.borrow().children().borrow().get(0).unwrap();
    let Element::XmlElement(section) = section else {
        panic!("section not an element")
    };
    let s = section.borrow();
    match s.attrs().borrow().get(b"id") {
        Some(Element::Register(r)) => assert_eq!(r.borrow().read().clone(), Scalar::Int(3)),
        other => panic!("attr id missing: {}", other.is_some()),
    }
    let grand = s.children().borrow().get(0).unwrap();
    match grand {
        Element::XmlElement(g) => assert_eq!(g.borrow().tag(), b"span"),
        _ => panic!("grandchild not an element"),
    }
}

#[test]
fn a_fragment_holds_children() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        kids.insert_element(0, b"p");
        kids.insert_text(1).insert(0, "tail");
    });
    assert_eq!(children_of(&d, b"doc"), vec!["<p>", "\"tail\""]);
}

#[test]
fn a_fresh_replica_converges_on_the_tree() {
    let mut src = Document::new(cid(1));
    let ops = src.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        let mut kids = body.children();
        let mut h1 = kids.insert_element(0, b"h1");
        h1.children().insert_text(0).insert(0, "Title");
        kids.insert_element(1, b"p");
    });

    let mut dst = Document::new(cid(2));
    for op in &ops {
        dst.apply(op);
    }
    assert_eq!(children_of(&dst, b"doc"), vec!["<h1>", "<p>"]);
    // The h1's text survived the trip.
    let doc = dst.get(b"doc").unwrap();
    let Element::XmlElement(body) = doc else {
        panic!()
    };
    let h1 = body.borrow().children().borrow().get(0).unwrap();
    match h1 {
        Element::XmlElement(h1) => {
            let h1 = h1.borrow();
            let txt = h1.children().borrow().get(0).unwrap();
            match txt {
                Element::Text(t) => assert_eq!(t.borrow().as_string(), "Title"),
                _ => panic!("h1 child not text"),
            }
        }
        _ => panic!(),
    }
}

#[test]
fn an_atomic_insert_and_edit_child_commits_on_a_remote_replica() {
    // Insert a child element and edit it in one atomic transaction; the edit
    // targets the child's stamp-derived id, which only the insert makes
    // reachable — the readiness gate must count it, or the group deadlocks.
    let mut src = Document::new(cid(1));
    let ops = src.atomic_transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        body.children()
            .insert_element(0, b"p")
            .attrs()
            .register(b"class", Scalar::Int(1));
    });

    let mut dst = Document::new(cid(2));
    for op in ops.iter().rev() {
        dst.apply(op);
    }
    assert_eq!(child_tag(&dst, b"doc", 0), Some(b"p".to_vec()));
}

#[test]
fn concurrent_inserts_at_the_same_gap_converge() {
    let mut a = Document::new(cid(1));
    let seed = a.transact(|tx| {
        tx.xml_element(b"doc", b"body");
    });
    let mut b = Document::new(cid(2));
    for op in &seed {
        b.apply(op);
    }

    let a_ops = a.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        body.children().insert_element(0, b"a1");
    });
    let b_ops = b.transact(|tx| {
        let mut body = tx.xml_element(b"doc", b"body");
        body.children().insert_element(0, b"b1");
    });
    for op in &b_ops {
        a.apply(op);
    }
    for op in &a_ops {
        b.apply(op);
    }
    assert_eq!(children_of(&a, b"doc"), children_of(&b, b"doc"));
    assert_eq!(children_of(&a, b"doc").len(), 2);
}
