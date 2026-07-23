// crdtsync — the ergonomic JavaScript/TypeScript SDK: a handle-graph over the
// CRDT core. Edit a document through live typed handles (`Doc.getMap`/`getList`/
// `getText`) with native-value marshaling; the byte-path core stays hidden.

export { Doc } from "./doc.js";
export type {
  Change,
  ChangeEvent,
  ChangeListener,
  DocOptions,
  UpdateEvent,
  UpdateListener,
} from "./doc.js";
export { CrdtMap, CrdtList, CrdtText } from "./handles.js";
export type { Value } from "./handles.js";
export type { Key } from "./path.js";
export type { ScalarValue } from "./marshal.js";
