/**
 * Bonnard SDK — Client for querying semantic layer
 */

import type {
  BonnardConfig,
  QueryOptions,
  QueryResult,
  SqlResult,
  CubeQuery,
  ExploreMeta,
  ExploreOptions,
  DocsOptions,
  DocsTopicListResult,
  DocsTopicResult,
} from './types.js';
import { toCubeQuery } from './query.js';

/**
 * Parse JWT expiry from the payload (base64url-decoded middle segment).
 * Returns the `exp` claim as a millisecond timestamp, or 0 if unparseable.
 */
function parseJwtExpiry(token: string): number {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return 0;
    // base64url → base64 → decode
    const payload = parts[1]!.replace(/-/g, '+').replace(/_/g, '/');
    const json = JSON.parse(atob(payload));
    return typeof json.exp === 'number' ? json.exp * 1000 : 0;
  } catch {
    return 0;
  }
}

const REFRESH_BUFFER_MS = 60_000; // refresh 60s before expiry

/**
 * Create a Bonnard client.
 *
 * When `fetchToken` is provided, the returned token is cached automatically
 * and refreshed 60 seconds before its JWT `exp` claim. Concurrent calls are
 * deduplicated so your callback is never invoked more than once at a time.
 */
export function createClient(config: BonnardConfig) {
  const baseUrl = config.baseUrl || 'https://app.bonnard.dev';

  // Token cache for fetchToken mode
  let cachedToken: string | null = null;
  let cachedExpiry = 0;
  let pendingFetch: Promise<string> | null = null;

  async function getToken(): Promise<string> {
    // Static API key — return directly
    if (config.apiKey) {
      return config.apiKey;
    }

    // Token callback — cache, refresh, and deduplicate
    if (config.fetchToken) {
      const now = Date.now();
      if (cachedToken && cachedExpiry - REFRESH_BUFFER_MS > now) {
        return cachedToken;
      }

      // Deduplicate concurrent calls — share a single in-flight promise
      if (pendingFetch) return pendingFetch;

      pendingFetch = config.fetchToken()
        .then((token) => {
          cachedToken = token;
          cachedExpiry = parseJwtExpiry(token);
          return token;
        })
        .finally(() => {
          pendingFetch = null;
        });

      return pendingFetch;
    }

    throw new Error('BonnardConfig requires either apiKey or fetchToken');
  }

  async function request<T>(endpoint: string, body: unknown): Promise<T> {
    const token = await getToken();
    const res = await fetch(`${baseUrl}${endpoint}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const error = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(error.error || 'Query failed');
    }

    return res.json();
  }

  async function requestGet<T>(endpoint: string): Promise<T> {
    const token = await getToken();
    const res = await fetch(`${baseUrl}${endpoint}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    });

    if (!res.ok) {
      const error = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(error.error || 'Request failed');
    }

    return res.json();
  }

  return {
    /**
     * Execute a JSON query against the semantic layer.
     * All field names must be fully qualified (e.g. "orders.revenue").
     */
    async query<T = Record<string, unknown>>(options: QueryOptions): Promise<QueryResult<T>> {
      const cubeQuery = toCubeQuery(options);

      const result = await request<{ data: T[]; annotation?: QueryResult['annotation'] }>(
        '/api/cube/query',
        { query: cubeQuery }
      );

      return { data: result.data, annotation: result.annotation };
    },

    /**
     * Execute a raw Cube-native JSON query against the semantic layer.
     * Use this when you already have a Cube API query object.
     */
    async rawQuery<T = Record<string, unknown>>(cubeQuery: CubeQuery): Promise<QueryResult<T>> {
      const result = await request<{ data: T[]; annotation?: QueryResult['annotation'] }>(
        '/api/cube/query',
        { query: cubeQuery }
      );

      return { data: result.data, annotation: result.annotation };
    },

    /**
     * Execute a SQL query against the semantic layer
     */
    async sql<T = Record<string, unknown>>(query: string): Promise<SqlResult<T>> {
      return request<SqlResult<T>>('/api/cube/query', { sql: query });
    },

    /**
     * Discover available cubes, measures, dimensions, and segments.
     * By default returns only views (viewsOnly: true).
     */
    async explore(options?: ExploreOptions): Promise<ExploreMeta> {
      const meta = await requestGet<{ cubes: ExploreMeta['cubes'] }>('/api/cube/meta');
      const viewsOnly = options?.viewsOnly ?? true;

      if (viewsOnly) {
        return { cubes: meta.cubes.filter(c => c.type === 'view') };
      }

      return meta;
    },

    /**
     * Access Bonnard documentation.
     * - `docs()` — list all topics
     * - `docs({ category: 'dashboards' })` — list topics in a category
     * - `docs({ topic: 'dashboards.components' })` — get full markdown content
     */
    async docs(options?: DocsOptions): Promise<DocsTopicListResult | DocsTopicResult> {
      const params = new URLSearchParams();
      if (options?.topic) params.set('topic', options.topic);
      else if (options?.category) params.set('category', options.category);
      const qs = params.toString();
      return requestGet(`/api/docs${qs ? `?${qs}` : ''}`);
    },
  };
}
