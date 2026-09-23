export interface ExtremeOutlierBounds {
  lowerFence: number | null;
  upperFence: number | null;
  lowerQuartile: number | null;
  upperQuartile: number | null;
  interquartileRange: number | null;
  observations: number;
  sufficientData: boolean;
}

export function sampleStandardDeviation(values: number[]): number | null {
  const finite = values.filter(Number.isFinite);
  if (!finite.length) return null;
  if (finite.length === 1) return 0;
  const average = finite.reduce((total, value) => total + value, 0) / finite.length;
  return Math.sqrt(finite.reduce((total, value) => total + ((value - average) ** 2), 0) / (finite.length - 1));
}

export function arithmeticMean(values: number[]): number | null {
  const finite = values.filter(Number.isFinite);
  return finite.length ? finite.reduce((total, value) => total + value, 0) / finite.length : null;
}

export function percentile(values: number[], quantile: number): number | null {
  const sorted = values.filter(Number.isFinite).sort((left, right) => left - right);
  if (!sorted.length) return null;
  const position = (sorted.length - 1) * quantile;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + ((sorted[upper] - sorted[lower]) * (position - lower));
}

export function extremeOutlierBounds(values: number[]): ExtremeOutlierBounds {
  const finite = values.filter(Number.isFinite);
  if (finite.length < 4) {
    return {
      lowerFence: null,
      upperFence: null,
      lowerQuartile: null,
      upperQuartile: null,
      interquartileRange: null,
      observations: finite.length,
      sufficientData: false,
    };
  }
  const lowerQuartile = percentile(finite, 0.25)!;
  const upperQuartile = percentile(finite, 0.75)!;
  const interquartileRange = upperQuartile - lowerQuartile;
  return {
    lowerFence: lowerQuartile - (3 * interquartileRange),
    upperFence: upperQuartile + (3 * interquartileRange),
    lowerQuartile,
    upperQuartile,
    interquartileRange,
    observations: finite.length,
    sufficientData: true,
  };
}

export function isExtremeOutlier(value: number, bounds: ExtremeOutlierBounds): boolean {
  return bounds.sufficientData
    && bounds.lowerFence !== null
    && bounds.upperFence !== null
    && (value < bounds.lowerFence || value > bounds.upperFence);
}
