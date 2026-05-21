import { describe, expect, it } from "vitest";
import { fmtPct, fmtPrice, fmtVolume } from "./format";

describe("fmtPrice", () => {
  it("formats with two decimal places by default", () => {
    expect(fmtPrice(1234.5)).toBe("1,234.50");
  });
  it("respects custom decimals", () => {
    expect(fmtPrice(0.12345, 4)).toBe("0.1235");
  });
});

describe("fmtVolume", () => {
  it("formats millions with M suffix", () => {
    expect(fmtVolume(2_500_000)).toBe("2.50M");
  });
  it("formats thousands with K suffix", () => {
    expect(fmtVolume(1500)).toBe("1.50K");
  });
  it("formats small values with four decimals", () => {
    expect(fmtVolume(0.5)).toBe("0.5000");
  });
});

describe("fmtPct", () => {
  it("adds + sign for positive", () => {
    expect(fmtPct(1.5)).toBe("+1.50%");
  });
  it("keeps - sign for negative", () => {
    expect(fmtPct(-0.75)).toBe("-0.75%");
  });
  it("shows + for zero", () => {
    expect(fmtPct(0)).toBe("+0.00%");
  });
});
