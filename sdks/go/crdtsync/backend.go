package crdtsync

// The storage/wire seam a Doc edits and reads through (§SDK-Ergonomic-Surface).
// A local Doc is backed directly by a *Document, which already exposes exactly
// this method set; a networked Doc is backed by one channel of a wire Client, so
// an edit is framed and outboxed for the wire while reads query that channel's
// replica — one replica per room, never two divergent copies. An edit returns
// the bytes it produced: raw ops for a document backend, a wire Ops frame for a
// client backend.

// Backend is the replica a Doc edits and reads through. *Document and
// *ClientBackend implement it; an application never implements it itself.
//
// A Backend is not safe for concurrent use — the Doc that owns it serializes
// every call under its own lock, and a networked Doc shares that lock with its
// provider. Reach for a Doc's Backend only from the goroutine holding it.
type Backend interface {
	// --- reads ---

	GetScalar(path [][]byte) ([]byte, bool)
	GetInt(path [][]byte) (int64, bool)
	GetCounter(path [][]byte) (int64, bool)
	GetBytes(path [][]byte) ([]byte, bool)
	GetBlob(path [][]byte) (BlobRef, bool)
	MapKeys(path [][]byte) ([][]byte, bool)
	ListLen(path [][]byte) (uint, bool)
	ListGet(path [][]byte, index uint) ([]byte, bool)
	TextLen(path [][]byte) (uint, bool)
	TextGet(path [][]byte) (string, bool)
	XmlTag(path [][]byte) ([]byte, bool)
	XmlChildrenLen(path [][]byte) (uint, bool)
	RelativePosition(path [][]byte, index uint, side Side) []byte
	ResolvePosition(path [][]byte, pos []byte) (uint, bool)
	MarksAt(seqPath [][]byte, index uint) []Mark

	// EncodeState serializes the replica to a canonical snapshot — the before/after
	// pair the Doc diffs to derive its change events.
	EncodeState() []byte

	// --- edits, each returning the bytes to transmit ---

	RegisterInt(path [][]byte, value int64) []byte
	Inc(path [][]byte, amount uint32) []byte
	Dec(path [][]byte, amount uint32) []byte
	SetScalar(path [][]byte, scalar []byte) []byte
	SetBytes(path [][]byte, value []byte) []byte
	Delete(path [][]byte) []byte
	ListInsert(path [][]byte, index uint, value []byte) []byte
	ListDelete(path [][]byte, index uint) []byte
	TextInsert(path [][]byte, index uint, text string) []byte
	TextDelete(path [][]byte, index, count uint) []byte
	SetBlob(path [][]byte, mime string, data []byte) ([]byte, bool)
	SetBlobRef(path [][]byte, id [16]byte, mime string, size uint64) []byte
	Mark(seqPath [][]byte, startIndex uint, startSide Side, endIndex uint, endSide Side, name []byte, value Scalar) (markID []byte, ops []byte)
	MarkSetValue(markID []byte, value Scalar) []byte
	MarkDelete(markID []byte) []byte
	XmlElement(path [][]byte, tag []byte) []byte
	XmlFragment(path [][]byte) []byte
	XmlInsertElement(path [][]byte, index uint, tag []byte) []byte
	XmlInsertText(path [][]byte, index uint, text string) []byte
	XmlChildDelete(path [][]byte, index uint) []byte
	XmlMove(parentPath [][]byte, childIndex uint, newParentPath [][]byte, destIndex uint) []byte

	// BeginAtomic opens an atomic group; edits accumulate until CommitAtomic
	// returns them as one batch.
	BeginAtomic()
	CommitAtomic() []byte

	// SetSchema binds a schema for repair observation and mark flavors, reporting
	// whether it bound; TakeRepairs drains the paths whose repaired reading newly
	// changed against it.
	SetSchema(schema []byte) bool
	TakeRepairs() [][]Step

	// Apply folds a peer's ops in, returning the count applied beside the count
	// no replica will ever hold. A networked backend syncs through its provider
	// instead and refuses with -1.
	Apply(ops []byte) (applied, refused int)

	// Close releases what the backend owns.
	Close()
}

// ClientBackend is a Backend over one channel of a wire Client: an edit is
// framed as a wire Ops frame and held in the channel's outbox until the server
// acknowledges it, and a read queries the channel's replica. A NetProvider binds
// its Doc to one of these, so the handle graph edits the same replica the wire
// session syncs — reads, change events, and all.
//
// A schema is the one thing it does not carry: binding one is replica-local
// runtime state with no per-channel seat, so a networked Doc reports no repairs
// and its marks take the default object flavor.
type ClientBackend struct {
	client  *Client
	channel uint32
}

// NewClientBackend binds a Backend to channel's room on client. The client
// outlives the backend; closing the backend does not close the client.
func NewClientBackend(client *Client, channel uint32) *ClientBackend {
	return &ClientBackend{client: client, channel: channel}
}

// Channel is the wire channel this backend edits and reads.
func (c *ClientBackend) Channel() uint32 { return c.channel }

func (c *ClientBackend) GetScalar(path [][]byte) ([]byte, bool) {
	return c.client.GetScalar(c.channel, path)
}

func (c *ClientBackend) GetInt(path [][]byte) (int64, bool) {
	return c.client.GetInt(c.channel, path)
}

func (c *ClientBackend) GetCounter(path [][]byte) (int64, bool) {
	return c.client.GetCounter(c.channel, path)
}

func (c *ClientBackend) GetBytes(path [][]byte) ([]byte, bool) {
	return c.client.GetBytes(c.channel, path)
}

func (c *ClientBackend) MapKeys(path [][]byte) ([][]byte, bool) {
	return c.client.MapKeys(c.channel, path)
}

func (c *ClientBackend) ListLen(path [][]byte) (uint, bool) {
	return c.client.ListLen(c.channel, path)
}

func (c *ClientBackend) ListGet(path [][]byte, index uint) ([]byte, bool) {
	return c.client.ListGet(c.channel, path, index)
}

func (c *ClientBackend) TextLen(path [][]byte) (uint, bool) {
	return c.client.TextLen(c.channel, path)
}

func (c *ClientBackend) TextGet(path [][]byte) (string, bool) {
	return c.client.TextGet(c.channel, path)
}

func (c *ClientBackend) SetScalar(path [][]byte, scalar []byte) []byte {
	return c.client.SetScalar(c.channel, path, scalar)
}

func (c *ClientBackend) RegisterInt(path [][]byte, value int64) []byte {
	return c.client.RegisterInt(c.channel, path, value)
}

func (c *ClientBackend) Inc(path [][]byte, amount uint32) []byte {
	return c.client.Inc(c.channel, path, amount)
}

func (c *ClientBackend) Dec(path [][]byte, amount uint32) []byte {
	return c.client.Dec(c.channel, path, amount)
}

func (c *ClientBackend) SetBytes(path [][]byte, value []byte) []byte {
	return c.client.SetBytes(c.channel, path, value)
}

func (c *ClientBackend) Delete(path [][]byte) []byte {
	return c.client.Delete(c.channel, path)
}

func (c *ClientBackend) ListInsert(path [][]byte, index uint, value []byte) []byte {
	return c.client.ListInsert(c.channel, path, index, value)
}

func (c *ClientBackend) ListDelete(path [][]byte, index uint) []byte {
	return c.client.ListDelete(c.channel, path, index)
}

func (c *ClientBackend) TextInsert(path [][]byte, index uint, text string) []byte {
	return c.client.TextInsert(c.channel, path, index, text)
}

func (c *ClientBackend) TextDelete(path [][]byte, index, count uint) []byte {
	return c.client.TextDelete(c.channel, path, index, count)
}

// SetBlob sets an inline blob, routed through the outbox. The bool reports
// whether the blob was inlined — bytes over the inline ceiling enqueue no op and
// are uploaded out of band with UploadBlob then set via SetBlobRef. The frame is
// the only signal the per-channel edit surface gives, and an atomic group holds
// every frame back until its commit, so a blob inlined inside a transaction
// reports false: the caller uploads it out of band needlessly, never loses it.
func (c *ClientBackend) SetBlob(path [][]byte, mime string, data []byte) ([]byte, bool) {
	frame := c.client.SetBlob(c.channel, path, mime, data)
	return frame, len(frame) > 0
}

func (c *ClientBackend) SetBlobRef(path [][]byte, id [16]byte, mime string, size uint64) []byte {
	return c.client.SetBlobRef(c.channel, path, id, mime, size)
}

func (c *ClientBackend) Mark(seqPath [][]byte, startIndex uint, startSide Side, endIndex uint, endSide Side, name []byte, value Scalar) ([]byte, []byte) {
	return c.client.Mark(c.channel, seqPath, startIndex, startSide, endIndex, endSide, name, value)
}

func (c *ClientBackend) MarkSetValue(markID []byte, value Scalar) []byte {
	return c.client.MarkSetValue(c.channel, markID, value)
}

func (c *ClientBackend) MarkDelete(markID []byte) []byte {
	return c.client.MarkDelete(c.channel, markID)
}

func (c *ClientBackend) XmlElement(path [][]byte, tag []byte) []byte {
	return c.client.XmlElement(c.channel, path, tag)
}

func (c *ClientBackend) XmlFragment(path [][]byte) []byte {
	return c.client.XmlFragment(c.channel, path)
}

func (c *ClientBackend) XmlInsertElement(path [][]byte, index uint, tag []byte) []byte {
	return c.client.XmlInsertElement(c.channel, path, index, tag)
}

func (c *ClientBackend) XmlInsertText(path [][]byte, index uint, text string) []byte {
	return c.client.XmlInsertText(c.channel, path, index, text)
}

func (c *ClientBackend) XmlChildDelete(path [][]byte, index uint) []byte {
	return c.client.XmlChildDelete(c.channel, path, index)
}

func (c *ClientBackend) XmlMove(parentPath [][]byte, childIndex uint, newParentPath [][]byte, destIndex uint) []byte {
	return c.client.XmlMove(c.channel, parentPath, childIndex, newParentPath, destIndex)
}

func (c *ClientBackend) BeginAtomic() { c.client.BeginAtomic(c.channel) }

func (c *ClientBackend) CommitAtomic() []byte { return c.client.CommitAtomic(c.channel) }

// Apply refuses: a networked replica folds a peer's work in through the frames
// its provider receives, never through a side channel that would bypass the
// outbox and the server sequence.
func (c *ClientBackend) Apply([]byte) (applied, refused int) { return -1, 0 }

// Close is a no-op — the provider owns the wire client and closes it.
func (c *ClientBackend) Close() {}

func (c *ClientBackend) EncodeState() []byte {
	state, _ := c.client.ChannelState(c.channel)
	return state
}

func (c *ClientBackend) GetBlob(path [][]byte) (BlobRef, bool) {
	return c.client.GetBlob(c.channel, path)
}

func (c *ClientBackend) XmlTag(path [][]byte) ([]byte, bool) {
	return c.client.XmlTag(c.channel, path)
}

func (c *ClientBackend) XmlChildrenLen(path [][]byte) (uint, bool) {
	return c.client.XmlChildrenLen(c.channel, path)
}

func (c *ClientBackend) RelativePosition(path [][]byte, index uint, side Side) []byte {
	return c.client.RelativePosition(c.channel, path, index, side)
}

func (c *ClientBackend) ResolvePosition(path [][]byte, pos []byte) (uint, bool) {
	return c.client.ResolvePosition(c.channel, path, pos)
}

func (c *ClientBackend) MarksAt(seqPath [][]byte, index uint) []Mark {
	return c.client.MarksAt(c.channel, seqPath, index)
}

// A schema binds to a replica as runtime state rather than as an op, and the
// per-channel seat carries no binding of its own, so a networked Doc has no
// schema and reports no repairs. Its marks take the default object flavor.
func (c *ClientBackend) SetSchema([]byte) bool { return false }

func (c *ClientBackend) TakeRepairs() [][]Step { return nil }
