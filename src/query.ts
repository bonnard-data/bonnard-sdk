/**
 * Bonnard SDK — Query format conversion (zero IO)
 */

import type { QueryOptions } from './types.js';

/**
 * Convert SDK QueryOptions into a Cube-native query object.
 * All field names must be fully qualified (e.g. "orders.revenue").
 */
export function toCubeQuery(options: QueryOptions): Record<string, unknown> {
  const cubeQuery: Record<string, unknown> = {};

  if (options.measures) {
    cubeQuery.measures = options.measures;
  }

  if (options.dimensions) {
    cubeQuery.dimensions = options.dimensions;
  }

  if (options.filters) {
    cubeQuery.filters = options.filters.map(f => ({
      member: f.dimension,
      operator: f.operator,
      values: f.values,
    }));
  }

  if (options.timeDimension) {
    cubeQuery.timeDimensions = [{
      dimension: options.timeDimension.dimension,
      granularity: options.timeDimension.granularity,
      dateRange: options.timeDimension.dateRange,
    }];
  }

  if (options.orderBy) {
    cubeQuery.order = Object.entries(options.orderBy).map(([key, dir]) => [key, dir]);
  }

  if (options.limit) {
    cubeQuery.limit = options.limit;
  }

  return cubeQuery;
}
