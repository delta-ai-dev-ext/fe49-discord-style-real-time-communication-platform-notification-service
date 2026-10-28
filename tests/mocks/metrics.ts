import type { Counter, Gauge } from "prom-client";

import { registry } from "../../src/metrics.ts";

type Labelled = Counter<string> | Gauge<string>;

/** Read a counter/gauge value for the given label set (0 when unset). */
export async function metricValue(name: string, labels: Record<string, string> = {}): Promise<number> {
  const metric = registry.getSingleMetric(name) as Labelled | undefined;
  if (!metric) throw new Error(`metric ${name} is not registered`);
  const data = await metric.get();
  const match = data.values.find((value) =>
    Object.entries(labels).every(([key, expected]) => value.labels[key] === expected),
  );
  return match?.value ?? 0;
}

export async function histogramCount(name: string): Promise<number> {
  const metric = registry.getSingleMetric(name) as unknown as {
    get(): Promise<{ values: { metricName?: string; labels: Record<string, string>; value: number }[] }>;
  };
  const data = await metric.get();
  const entry = data.values.find((value) => value.metricName === `${name}_count`);
  return entry?.value ?? 0;
}
