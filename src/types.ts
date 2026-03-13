/**
 * Bonnard SDK — Shared types
 */

export interface BonnardConfig {
  /** Publishable API key (`bon_pk_…`). Mutually exclusive with `fetchToken`. */
  apiKey?: string;
  /**
   * Callback that returns a short-lived JWT from your server.
   *
   * The SDK **automatically caches** the token and re-calls this function
   * 60 seconds before the JWT's `exp` claim. Concurrent calls while a fetch
   * is in-flight are deduplicated — you do **not** need to cache the token
   * yourself.
   *
   * Mutually exclusive with `apiKey`.
   */
  fetchToken?: () => Promise<string>;
  /** API base URL. Defaults to `https://app.bonnard.dev`. */
  baseUrl?: string;
}

export interface QueryOptions {
  measures?: string[];
  dimensions?: string[];
  filters?: Filter[];
  timeDimension?: TimeDimension;
  orderBy?: Record<string, 'asc' | 'desc'>;
  limit?: number;
}

export interface CubeQuery {
  measures?: string[];
  dimensions?: string[];
  timeDimensions?: Array<{
    dimension: string;
    granularity?: 'day' | 'week' | 'month' | 'quarter' | 'year';
    dateRange?: string | [string, string];
  }>;
  filters?: Array<{
    member: string;
    operator: string;
    values?: (string | number)[];
  }>;
  segments?: string[];
  order?: Record<string, 'asc' | 'desc'> | Array<[string, 'asc' | 'desc']>;
  limit?: number;
  offset?: number;
}

export interface Filter {
  dimension: string;
  operator: 'equals' | 'notEquals' | 'contains' | 'gt' | 'gte' | 'lt' | 'lte';
  values: (string | number)[];
}

export interface TimeDimension {
  dimension: string;
  granularity?: 'day' | 'week' | 'month' | 'quarter' | 'year';
  dateRange?: string | [string, string];
}

export interface QueryResult<T = Record<string, unknown>> {
  data: T[];
  annotation?: {
    measures: Record<string, { title: string; type: string }>;
    dimensions: Record<string, { title: string; type: string }>;
  };
}

export interface SqlResult<T = Record<string, unknown>> {
  data: T[];
  schema?: Array<{ name: string; type: string }>;
}

export interface ExploreMeta {
  cubes: CubeMetaItem[];
}

export interface CubeMetaItem {
  name: string;
  title?: string;
  description?: string;
  type?: string;
  measures: CubeFieldMeta[];
  dimensions: CubeFieldMeta[];
  segments: CubeSegmentMeta[];
}

export interface CubeFieldMeta {
  name: string;
  title?: string;
  shortTitle?: string;
  description?: string;
  type: string;
  format?: string;
  meta?: Record<string, unknown>;
}

export interface CubeSegmentMeta {
  name: string;
  title?: string;
  shortTitle?: string;
  description?: string;
}

export interface ExploreOptions {
  viewsOnly?: boolean;
}

/** Type helper for defining cube schemas */
export type InferQueryResult<C extends string, M extends string[], D extends string[]> = {
  [K in M[number] | D[number]]: K extends M[number] ? number : string;
};

export interface DocsOptions {
  topic?: string;
  category?: string;
}

export interface DocsTopicSummary {
  id: string;
  title: string;
  description: string | null;
  category: string;
}

export interface DocsTopicListResult {
  topics: DocsTopicSummary[];
}

export interface DocsTopicResult {
  topic: { id: string; title: string; content: string };
}
