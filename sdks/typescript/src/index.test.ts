import { describe, expect, it } from "vitest";
import { version } from "./index.js";

describe("crdtsync SDK smoke", () => {
  it("exports placeholder version", () => {
    expect(version).toBe("0.0.0");
  });
});
