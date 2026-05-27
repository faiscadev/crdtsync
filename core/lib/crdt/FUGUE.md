# Fugue: A Sequence CRDT — Implementation Guide

Notes for hand-implementing Fugue, the sequence CRDT we picked for `List`
(CORE-3) and `Text` (CORE-4). Pseudo-code is generic, not OCaml.

Reference: Weidner & Kleppmann, **"The Art of the Fugue: Minimizing Interleaving
in Collaborative Text Editing"**, PaPoC 2023.
<https://arxiv.org/abs/2305.00583>

---

## 1. The problem Fugue solves

Sequence CRDTs need to converge under concurrent edits while preserving user
intent. The classic anomaly:

```
Initial doc:  X

Alice (concurrent, after X):   a, b, c
Bob   (concurrent, after X):   x, y, z
```

Three candidate convergent orderings of the resulting sequence:

- `X a b c x y z`  ← Alice's run intact, then Bob's
- `X x y z a b c`  ← Bob's run intact, then Alice's
- `X a x b y c z`  ← **interleaved** (both runs mixed)

RGA and several earlier CRDTs can produce the interleaved result depending on
how unique-id tie-breaking interacts with the parent-pointer rule. Interleaving
is invisible in scalar lists but devastating in text: nobody types
`"abc"` and expects `"axbycz"`.

Fugue's contribution: a small change to RGA's parent-pointer rule that
**provably** avoids interleaving on concurrent insertions at the same point.

---

## 2. Data model

Each list entry is a node in an **implicit binary tree**. Every node has:

| Field | Meaning |
|---|---|
| `id` | Globally unique entry identifier. For us: `Element_id` derived from `(list_id, insert_op_id)`. |
| `value` | The payload at this slot (`Value.Scalar` or `Value.Element`). |
| `parent_id` | The id of the parent node in the tree (or "root" sentinel). |
| `side` | `Left` or `Right` — which side of the parent this node hangs off. |
| `tombstoned` | `true` after a `delete`; node record kept for position refs. |
| `insert_lamport` | Lamport timestamp of the insert op (for tie-breaks). |
| `insert_op_id` | Full op id, for tie-break on equal lamport. |

The "root" is a sentinel that conceptually parents the first inserted node.
You don't actually allocate it — just treat `parent_id = None` as "root".

### Conceptually, the in-order traversal of this tree gives the sequence.

```
          E              In-order traversal:
         / \              left subtree, self, right subtree
        B   F            visiting only non-tombstoned nodes
       / \   \
      A   C   G          gives: A B C D E F G H (if all live)
           \   \
            D   H
```

`A B C D E F G H` is the user-visible list.

---

## 3. Wire ops

```
Insert(entry_id, value, origin_left, origin_right)
Delete(target_entry_id)
Move(target_entry_id, new_origin_left, new_origin_right)
```

`origin_left` / `origin_right` are the entry_ids of the **live neighbors at
the moment the user clicked "insert here"**. Either or both may be `None`
(inserting at the head or tail). They are NOT the tree's `parent_id`+`side`
— Fugue derives those from `(origin_left, origin_right)` per the rule below.

---

## 4. Inserting: the Fugue rule

When applying `Insert(new_id, value, origin_left, origin_right)`:

```
function determine_parent_and_side(origin_left, origin_right):
    if origin_right is None
       OR origin_right has a left child already
       OR origin_right was inserted before origin_left:
        # attach as right child of origin_left
        parent = origin_left
        side   = Right
    else:
        # attach as left child of origin_right
        parent = origin_right
        side   = Left
```

The third clause — "origin_right was inserted before origin_left" — is the
**Fugue rule** that fixes RGA's interleaving. Compare insertion order using
the inserted nodes' `(lamport, op_id)`:

```
inserted_before(A, B) iff
    A.insert_lamport < B.insert_lamport
    OR (A.insert_lamport == B.insert_lamport
        AND Op_id.compare(A.insert_op_id, B.insert_op_id) < 0)
```

### Why this rule avoids interleaving — intuition

When Alice inserts `a b c` after `X` and Bob inserts `x y z` after `X`
concurrently:

- Alice's first insert: `origin_left = X`, `origin_right = whatever was X's right neighbor`. Becomes right child of X.
- Bob's first insert: same origins. Now there are two competing right children of X.
- Subsequent inserts in each run anchor on the previous one in that run.

The Fugue rule ensures that once Alice's `a` becomes a right child of X,
Bob's `x` (concurrent) cannot land "between" Alice's `a` and Alice's `b` —
the rule forces Bob's run to stay contiguous on one side or the other.
Both runs survive whole; one ends up before the other based on the
tie-break order.

### Tie-break for two nodes that pick the same parent + side

Both want to be the same child slot of the same parent. Sort the children
deterministically by `(insert_lamport, insert_op_id)` of each contender —
**higher** comes first in in-order position (i.e. the higher-id child is the
"closer to the parent" sibling). Both/all are retained, just ordered.

Equivalent formulation (more intuitive): siblings at the same parent+side
are kept in a list, ordered by decreasing `(lamport, op_id)`. The
in-order traversal visits them in that order.

---

## 5. Reading: in-order traversal

```
function to_list(tree):
    result = []
    visit(root, result)
    return result

function visit(node, result):
    if node is None: return
    for child in left_children(node) sorted by (lamport desc, op_id desc):
        visit(child, result)
    if not node.tombstoned:
        result.append(node.value)
    for child in right_children(node) sorted by (lamport desc, op_id desc):
        visit(child, result)
```

The "root" sentinel has no value to append (skip the middle step for it).

Naive implementation: O(n) traversal per read. Fine for v0.1.

Optimisations later: maintain a doubly-linked list of live nodes parallel to
the tree (Yjs does this); or a skip-list keyed by position for O(log n)
index lookups.

---

## 6. Deleting: tombstones

```
function apply_delete(target_id):
    node = tree[target_id]
    if node is None: return    # never inserted; could be out-of-order delivery
    if node.tombstoned: return # idempotent
    node.tombstoned = true
    if node.value is Element(child_id):
        return [child_id]      # released ref for orphan tracking
    return []
```

Tombstones are NEVER physically removed. Other entries may reference this
node as `origin_left` / `origin_right`, and cursors anchored on it
(via `entry_id`) must keep resolving to a valid (if dead) tree position.

GC of tombstones is a v0.5+ topic (snapshot compaction).

---

## 7. Move (our addition; not in the Fugue paper)

Fugue is a text CRDT; the paper doesn't define move. Our List adds a
`Move` op. The semantics are LWW per entry on the move op's
`(lamport, op_id)`:

```
function apply_move(target_id, new_origin_left, new_origin_right,
                   move_lamport, move_op_id):
    node = tree[target_id]
    if node is None: return []      # never inserted
    # LWW per entry: compare with the entry's "last position write"
    cur_lamport, cur_op_id = node.position_lamport, node.position_op_id
    if (move_lamport, move_op_id) <= (cur_lamport, cur_op_id):
        return []                   # stale move, ignore
    # rerun Fugue insert rule with new neighbors to get new parent+side
    new_parent, new_side = determine_parent_and_side(
        new_origin_left, new_origin_right)
    node.parent_id = new_parent
    node.side      = new_side
    node.position_lamport = move_lamport
    node.position_op_id   = move_op_id
    return []                       # move doesn't release any refs
```

`position_lamport` / `position_op_id` track the latest move winner per
entry. On insert, initialise to the insert's `(lamport, op_id)`.

No cycle detection needed — a list is linear, not a forest. Re-parenting
in the binary tree can't create cycles because the tree always remains
a tree (Fugue's parent is always an existing node, and we don't allow
parenting a node to itself or its descendant — but we don't have to check
that explicitly because the SDK only ever passes live neighbors that exist
elsewhere in the tree).

Edge case: if `target_id` is tombstoned, you can still apply the move
(updating its tree position). Cursors anchored on it stay valid. Reads
ignore tombstoned nodes anyway, so the move is invisible until/unless
the entry is "revived" — which doesn't happen in our model. Effectively a
no-op for the read view, but cheaper to just apply it than to special-case.

---

## 8. State machine summary

```
State per entry:
    id, value, parent_id, side,
    insert_lamport, insert_op_id,
    position_lamport, position_op_id,
    tombstoned

apply(Insert) :
    if entry already exists: idempotent no-op
    else: derive (parent, side) via Fugue rule from
          (origin_left, origin_right); allocate node;
          position_(lamport,op_id) = insert's (lamport, op_id)
    released = []

apply(Delete):
    if missing or already tombstoned: no-op
    else: tombstone; released = [child_id] if value was Element

apply(Move):
    if missing: no-op
    if (move_lamport, op_id) <= node's (position_lamport, position_op_id):
        no-op (LWW lost)
    else: rederive (parent, side); update position_(lamport,op_id)
    released = []
```

All three are idempotent on op identity in the obvious way:
- Re-applying same `Insert` re-derives same parent+side, same node.
- Re-applying same `Delete` finds node already tombstoned, no-op.
- Re-applying same `Move` finds `(position_lamport, op_id) ==` its own, doesn't strictly beat itself (uses `<=`), no-op.

---

## 9. Concrete walkthrough

Start empty. Alice and Bob both anchored on the empty doc.

### Step 1: Alice inserts "a" at head

```
Op:        Insert(id=A_a, value="a", origin_left=None, origin_right=None)
Rule:      origin_right is None -> right child of origin_left (None = root)
Tree:      root
              \
               A_a "a"
```

### Step 2: Alice inserts "b" after a (anchored on (A_a, None))

```
Op:        Insert(id=A_b, value="b", origin_left=A_a, origin_right=None)
Rule:      origin_right is None -> right child of A_a
Tree:      root
              \
               A_a "a"
                  \
                   A_b "b"
```

### Step 3: Concurrently (same lamport/region), Bob inserts "x" at head

```
Op:        Insert(id=B_x, value="x", origin_left=None, origin_right=A_a)
                                                       ^^^
                          Bob's "origin_right" is Alice's first insert
Rule:      origin_right = A_a; does A_a have a left child? No.
           Was A_a inserted before None? "None" treated as root with
           lamport = -infinity, so A_a IS inserted after it.
           Therefore: left child of A_a.
Tree:      root
              \
               A_a "a"
              /     \
            B_x "x"  A_b "b"
```

In-order traversal: `B_x A_a A_b` = `"x a b"`. Bob's `x` lands BEFORE
Alice's `a`. Good.

### Step 4: Bob inserts "y" after his "x"

```
Op:        Insert(id=B_y, value="y", origin_left=B_x, origin_right=A_a)
Rule:      origin_right = A_a; A_a has a left child already (B_x).
           Per the rule: attach as right child of origin_left (B_x).
Tree:      root
              \
               A_a "a"
              /     \
            B_x "x"  A_b "b"
                \
                 B_y "y"
```

In-order: `B_x B_y A_a A_b` = `"x y a b"`. Bob's run stays contiguous.
No interleaving.

### Step 5: Bob inserts "z" after his "y"

```
Op:        Insert(id=B_z, value="z", origin_left=B_y, origin_right=A_a)
Rule:      origin_right = A_a; A_a has a left child already (B_x subtree).
           Attach as right child of B_y.
Tree:      root
              \
               A_a "a"
              /     \
            B_x      A_b
              \
               B_y
                  \
                   B_z
```

In-order: `B_x B_y B_z A_a A_b` = `"x y z a b"`. Final convergent state.

The classic interleaving anomaly would have given `"a x b y c z"` etc.
Fugue prevents it.

---

## 10. Implementation checklist

Suggested build order:

- [ ] data structures: node record, tree (map from `entry_id` to node);
      root sentinel handling
- [ ] `empty ~element_id`: empty tree, root sentinel
- [ ] insertion order comparison: `(lamport, op_id)`-ordered total order
- [ ] `determine_parent_and_side` per the Fugue rule
- [ ] `apply Insert`: idempotent on entry_id; derive parent+side; insert
      into children list at correct position (decreasing-id order)
- [ ] traversal: in-order visit; respect children ordering
- [ ] `get index`, `to_list`, `entries`, `fold`: built on traversal
- [ ] `index_of`: linear scan through traversal until id matches
- [ ] `neighbors_at index`: walk traversal, return `(prev, next)` ids
      around position `index`
- [ ] `apply Delete`: idempotent; tombstone; release Element value
- [ ] `apply Move`: LWW per entry; re-derive parent+side
- [ ] `get_entry`: lookup by id; return value if live, None if tombstoned
- [ ] `length`: count live entries (or maintain a counter updated by apply)
- [ ] `equal`: compare element_id + entry maps (must equal_node, not
      structural — node records contain abstract types like Op_id /
      Lamport / Value)
- [ ] `pp`: dump entries in traversal order

Skip-list / linked-list parallel structures and run-length encoding are
v0.5+ optimisations; do not attempt them in the first pass.

---

## 11. Common pitfalls

1. **Forgetting to compare `(lamport, op_id)` instead of just `lamport`**
   when ordering siblings or evaluating the "inserted before" rule. Equal
   lamports happen at concurrent inserts and MUST tie-break by op_id.

2. **Mutating a node's `parent_id` without removing it from the old
   parent's children list and inserting into the new one.** On Move, you
   must update BOTH the moved node's `parent_id`/`side` AND fix the
   children lists of the old and new parents.

3. **Tombstoned nodes still participate in traversal structure.** Their
   value is skipped, but their children are visited. Do NOT prune them.

4. **The "root" sentinel.** Decide early whether it's a real node in your
   map keyed by some reserved id, or whether `parent_id = None` is the
   "root" signal. Either works; consistency is what matters.

5. **Idempotency on Insert.** Two clients can issue the same Insert (in
   replay or from a buffered tx). On re-apply, find existing node, no-op.
   Do NOT create a duplicate.

6. **`equal` cannot use structural `=`** if the node record contains
   abstract types whose internal representation may vary (Op_id, Lamport
   wrappers, Value via abstract Element_id). Compare via each type's
   `equal` function.

7. **`get_entry` returning None on tombstone** is by design (the entry is
   logically deleted) — but `index_of` should also return None for a
   tombstone, since tombstones have no live index. Tests rely on this.

---

## 12. Pointers to read offline

- Fugue paper (above) — sections 3 and 4 cover the algorithm; the proof
  is in section 5 and you can skip it for impl.
- Kleppmann's blog "Sequence CRDTs without interleaving" — short summary.
- `collabs` JS library's `CTotalOrder` / `CRichText` — TS reference impl
  by the paper authors. <https://github.com/composablesys/collabs>
