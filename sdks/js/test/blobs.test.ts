import { describe, expect, it } from "vitest";
import { type BlobRef, Doc } from "../src/index.js";

describe("blobs", () => {
  it("stores and reads an inline blob", () => {
    const doc = new Doc();
    const map = doc.getMap("root");
    const bytes = Uint8Array.of(1, 2, 3, 4);

    expect(map.setBlob("avatar", "image/png", bytes)).toBe(true);

    const ref = map.getBlob("avatar");
    expect(ref).toBeDefined();
    expect(ref?.mime).toBe("image/png");
    expect(ref?.size).toBe(4);
    expect(ref?.inline).toEqual(bytes);
    expect(ref?.id.length).toBe(16);
  });

  it("returns the blob ref from a generic get() without crashing", () => {
    const doc = new Doc();
    const map = doc.getMap("root");
    map.setBlob("file", "text/plain", Uint8Array.of(9));
    const value = map.get("file") as BlobRef;
    expect(value.mime).toBe("text/plain");
    expect(value.inline).toEqual(Uint8Array.of(9));
  });

  it("sets a store-backed blob ref by handle", () => {
    const doc = new Doc();
    const map = doc.getMap("root");
    const id = new Uint8Array(16).fill(7);
    map.setBlobRef("big", id, "video/mp4", 1_000_000);

    const ref = map.getBlob("big");
    expect(ref?.mime).toBe("video/mp4");
    expect(ref?.size).toBe(1_000_000);
    expect(ref?.inline).toBeNull(); // store-backed, not inline
    expect(ref?.id).toEqual(id);
  });

  it("converges a blob between two docs", () => {
    const a = new Doc();
    const b = new Doc();
    a.on("update", (e) => e.origin === "local" && b.applyUpdate(e.ops));

    a.getMap("root").setBlob("pic", "image/gif", Uint8Array.of(1, 2, 3));
    const ref = b.getMap("root").getBlob("pic");
    expect(ref?.inline).toEqual(Uint8Array.of(1, 2, 3));
  });
});
