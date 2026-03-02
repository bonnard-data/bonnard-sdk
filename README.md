# @bonnard/sdk

TypeScript SDK for querying the [Bonnard](https://www.bonnard.dev) semantic layer from any JavaScript or TypeScript application.

## Install

```bash
npm install @bonnard/sdk
```

## Quick Start

### With a publishable key

```typescript
import { createClient } from '@bonnard/sdk';

const bon = createClient({
  apiKey: 'bon_pk_...',
});

const { data } = await bon.query({
  measures: ['orders.revenue', 'orders.count'],
  dimensions: ['orders.status'],
});

console.log(data);
// [{ "orders.revenue": 45000, "orders.count": 120, "orders.status": "completed" }, ...]
```

### With token exchange (multi-tenant)

For B2B apps where each user sees their own data, use a server-side secret key to mint scoped tokens:

```typescript
// Server: exchange secret key for a scoped JWT
const res = await fetch('https://app.bonnard.dev/api/sdk/token', {
  method: 'POST',
  headers: {
    Authorization: 'Bearer bon_sk_...',
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    security_context: { tenant_id: 'acme-123' },
  }),
});
const { token } = await res.json();
```

```typescript
// Client: use the token callback
const bon = createClient({
  fetchToken: async () => {
    const res = await fetch('/api/analytics/token');
    const { token } = await res.json();
    return token;
  },
});

const { data } = await bon.query({
  measures: ['orders.revenue'],
  timeDimension: {
    dimension: 'orders.created_at',
    granularity: 'month',
    dateRange: ['2025-01-01', '2025-12-31'],
  },
});
```

Tokens are cached automatically and refreshed 60 seconds before expiry.

## API

### `createClient(config)`

| Option | Type | Description |
|--------|------|-------------|
| `apiKey` | `string` | Publishable key (`bon_pk_...`). Use one of `apiKey` or `fetchToken`. |
| `fetchToken` | `() => Promise<string>` | Async callback that returns a JWT. Use for multi-tenant setups. |
| `baseUrl` | `string` | API base URL (default: `https://app.bonnard.dev`) |

### `client.query(options)`

JSON query against the semantic layer.

```typescript
const { data } = await bon.query({
  measures: ['orders.revenue', 'orders.count'],
  dimensions: ['orders.product_category'],
  filters: [
    { dimension: 'orders.status', operator: 'equals', values: ['completed'] },
  ],
  timeDimension: {
    dimension: 'orders.created_at',
    granularity: 'month',
    dateRange: ['2025-01-01', '2025-12-31'],
  },
  orderBy: { 'orders.revenue': 'desc' },
  limit: 100,
});
```

### `client.sql(query)`

Raw SQL query using Cube SQL syntax.

```typescript
const { data } = await bon.sql(
  `SELECT product_category, MEASURE(revenue) FROM orders GROUP BY 1`
);
```

## Links

- [Bonnard Docs](https://docs.bonnard.dev)
- [Getting Started](https://docs.bonnard.dev/docs/getting-started)
- [Discord](https://discord.com/invite/RQuvjGRz)

## License

[MIT](./LICENSE)

