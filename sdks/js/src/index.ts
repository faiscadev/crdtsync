// crdtsync — the ergonomic JavaScript/TypeScript SDK: a handle-graph over the
// CRDT core. Edit a document through live typed handles (`Doc.getMap`/`getList`/
// `getText`) with native-value marshaling; the byte-path core stays hidden.

export { Doc } from "./doc.js";
export type {
  Change,
  ChangeEvent,
  ChangeListener,
  DocOptions,
  RepairEvent,
  RepairListener,
  RepairStep,
  UpdateEvent,
  UpdateListener,
} from "./doc.js";
export { CrdtMap, CrdtList, CrdtText, CrdtXml } from "./handles.js";
export type {
  BlobRef,
  CursorSide,
  MarkId,
  MarkInfo,
  RelativePosition,
  Value,
} from "./handles.js";
export { actorKey, uploadBlob } from "./wasm/crdtsync_wasm.js";
export type { Key } from "./path.js";
export type { ScalarValue } from "./marshal.js";
export { connect, Provider } from "./provider.js";
export type { ConnectionState, ProviderOptions, StateListener } from "./provider.js";
export { Capability, DiffKind, Effect, SubjectKind } from "./operator.js";
export type { AclGrant, AclSubject, Branch } from "./operator.js";
