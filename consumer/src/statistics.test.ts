import { describe, expect, it } from "vitest";
import { extremeOutlierBounds, isExtremeOutlier, sampleStandardDeviation } from "./statistics";

describe("distribution statistics", () => {
  it("identifies only observations beyond the three-IQR extreme fence", () => {
    const values = [10, 11, 12, 13, 14, 200];
    const bounds = extremeOutlierBounds(values);

    expect(bounds.sufficientData).toBe(true);
    expect(values.filter((value) => isExtremeOutlier(value, bounds))).toEqual([200]);
    expect(values.filter((value) => !isExtremeOutlier(value, bounds))).toEqual([10, 11, 12, 13, 14]);
  });

  it("does not label outliers when there are too few observations", () => {
    const bounds = extremeOutlierBounds([10, 12, 100]);

    expect(bounds.sufficientData).toBe(false);
    expect(isExtremeOutlier(100, bounds)).toBe(false);
  });

  it("recalculates sample deviation after an extreme observation is removed", () => {
    const original = sampleStandardDeviation([10, 11, 12, 13, 14, 200]);
    const adjusted = sampleStandardDeviation([10, 11, 12, 13, 14]);

    expect(original).not.toBeNull();
    expect(adjusted).not.toBeNull();
    expect(adjusted!).toBeLessThan(original!);
  });
});
