import { z } from "zod";
import type { BonnardClient, BonnardTool } from "./types.js";

// --- Shared helpers ---

const MAX_ROWS = 250;

/**
 * Validate that timeDimensions reference actual time-type fields.
 * Non-time fields (number, string) in timeDimensions cause Cube to silently
 * return unfiltered data — producing wrong results without any error.
 * Returns an error object if invalid, or null if OK.
 */
async function validateTimeDimensions(
  client: BonnardClient,
  timeDims: Array<{ dimension: string }> | undefined,
): Promise<{ error: string; hint: string } | null> {
  if (!timeDims || timeDims.length === 0) return null;

  const meta = await client.explore({ viewsOnly: false });

  for (const td of timeDims) {
    const dimName = td.dimension;
    const dotIdx = dimName.indexOf(".");
    if (dotIdx === -1) {
      return {
        error: `timeDimension "${dimName}" must be fully qualified (e.g. "orders.created_at").`,
        hint: `Use "view_name.field_name" format. Call explore_schema to see available time fields.`,
      };
    }

    const sourceName = dimName.substring(0, dotIdx);
    const cube = meta.cubes.find((c) => c.name === sourceName);
    if (!cube) continue;

    const field = cube.dimensions.find((d) => d.name === dimName);
    if (field && field.type !== "time") {
      return {
        error: `Cannot use "${dimName}" in timeDimensions — it is type "${field.type}", not "time". Using non-time fields in timeDimensions produces silently wrong results.`,
        hint: `Use "${dimName}" in filters instead: { member: "${dimName}", operator: "equals", values: ["value"] }. For numeric year/month columns, use operators like "gte" and "lte" for range filtering.`,
      };
    }
  }

  return null;
}

function normalizeValue(val: unknown): unknown {
  if (val === null || val === undefined) return null;
  if (typeof val === "number") {
    if (!Number.isInteger(val)) return Math.round(val * 100) / 100;
    return val;
  }
  return val;
}

function stripPrefixes(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (rows.length === 0) return rows;
  const keys = Object.keys(rows[0]);

  // Detect collisions — if two keys strip to the same short name, keep both fully qualified
  const shortKeys = keys.map((key) => key.split(".").pop() || key);
  const seen = new Set<string>();
  const collisions = new Set<string>();
  for (const sk of shortKeys) {
    if (seen.has(sk)) collisions.add(sk);
    seen.add(sk);
  }

  // Build key mapping: use short key unless it collides
  const keyMap = keys.map((key, idx) => [key, collisions.has(shortKeys[idx]) ? key : shortKeys[idx]] as const);

  return rows.map((row) => {
    const cleaned: Record<string, unknown> = {};
    for (const [origKey, mappedKey] of keyMap) {
      cleaned[mappedKey] = normalizeValue(row[origKey]);
    }
    return cleaned;
  });
}

function generateSqlErrorHints(error: string, sql: string): string {
  const hints: string[] = [];

  if (error.includes("Table or CTE with name") && error.includes("not found")) {
    hints.push("- Table not found: use explore_schema to list available views/cubes");
  }

  if (error.includes("Invalid identifier")) {
    hints.push("- Invalid column: check field names via explore_schema with `name` parameter");
    hints.push("- In SQL, use column names without view prefix (e.g. `revenue` not `view.revenue`)");
  }

  if (error.includes("could not be resolved")) {
    const hasMeasure = /MEASURE\s*\(/i.test(sql);
    const hasGroupBy = /GROUP\s+BY/i.test(sql);
    if (!hasMeasure) {
      hints.push("- Missing MEASURE(): wrap measure columns in MEASURE() function");
    }
    if (!hasGroupBy) {
      hints.push("- Missing GROUP BY: add GROUP BY for non-aggregated columns");
    }
  }

  if (error.includes("ParserError")) {
    hints.push("- SQL syntax error: check for typos or missing keywords");
    hints.push("- Use single quotes for string values: WHERE status = 'completed'");
  }

  if (/\bJOIN\b/i.test(sql)) {
    hints.push("- JOINs not supported: use UNION to combine results from different views");
  }

  if (hints.length === 0) {
    hints.push("- Verify table/view names with explore_schema");
    hints.push("- Use MEASURE() to aggregate measures");
    hints.push("- Include all non-aggregated columns in GROUP BY");
  }

  return hints.join("\n");
}

// --- Schemas ---

const exploreSchemaSchema = z.object({
  name: z.string().optional().describe("Source name to get full details (e.g. 'orders')"),
  search: z.string().optional().describe("Keyword to search across all field names and descriptions"),
});

const timeDimensionObject = z.object({
  dimension: z.string().min(1).describe('Time dimension (e.g. "orders.created_at")'),
  granularity: z.enum(["day", "week", "month", "quarter", "year"]).optional().describe("Time granularity for grouping"),
  dateRange: z.array(z.string()).min(2).max(2).optional().describe("Date range as [start, end] in YYYY-MM-DD format"),
});

const querySchema = z.object({
  measures: z.array(z.string()).optional().describe('Measures to query (e.g. ["orders.revenue", "orders.count"])'),
  dimensions: z.array(z.string()).optional().describe('Dimensions to group by (e.g. ["orders.status"])'),
  timeDimensions: z
    .array(timeDimensionObject)
    .optional()
    .describe("Time dimensions with date range and optional granularity"),
  timeDimension: timeDimensionObject.optional().describe("Alias for timeDimensions (single object)"),
  filters: z
    .array(
      z.object({
        member: z.string().optional().describe('Field to filter (e.g. "orders.status")'),
        dimension: z.string().optional().describe("Alias for member"),
        operator: z
          .enum([
            "equals",
            "notEquals",
            "contains",
            "notContains",
            "gt",
            "gte",
            "lt",
            "lte",
            "set",
            "notSet",
            "inDateRange",
            "notInDateRange",
            "beforeDate",
            "afterDate",
          ])
          .describe("Filter operator"),
        values: z.array(z.string()).optional().describe("Values to filter by (not needed for set/notSet operators)"),
      }),
    )
    .optional()
    .describe("Filters to apply"),
  segments: z.array(z.string()).optional().describe("Pre-defined filter segments"),
  order: z
    .array(
      z.object({
        field: z.string().min(1).describe('Field to sort by (e.g. "orders.revenue")'),
        direction: z.enum(["asc", "desc"]).describe("Sort direction"),
      }),
    )
    .optional()
    .describe("Sort order"),
  limit: z.number().optional().describe("Maximum rows to return (default: 250, max: 5000)"),
  offset: z.number().optional().describe("Number of rows to skip for pagination"),
});

const sqlQuerySchema = z.object({
  sql: z.string().min(1).describe("SQL query using Cube SQL syntax with MEASURE() for aggregations"),
});

const describeFieldSchema = z.object({
  field: z.string().min(1).describe('Fully qualified field name (e.g. "orders.revenue")'),
});

export function createTools(client: BonnardClient): BonnardTool[] {
  const exploreSchema: BonnardTool = {
    name: "explore_schema",
    description:
      "Discover available data sources (views), their measures, dimensions, and segments. " +
      "No arguments returns a summary of all sources. Use 'name' to get full field listings for one source. " +
      "Use 'search' to find fields by keyword across all sources.",
    schema: exploreSchemaSchema,
    execute: async (args) => {
      const meta = await client.explore({ viewsOnly: false });
      const cubes = meta.cubes;

      if (args.search) {
        const keyword = args.search.toLowerCase();
        const results: Array<{
          source: string;
          sourceType: string;
          field: string;
          kind: string;
          type: string;
          description?: string;
        }> = [];
        const MAX_SEARCH = 50;

        for (const cube of cubes) {
          if (results.length >= MAX_SEARCH) break;
          const sourceType = cube.type === "view" ? "view" : "cube";

          for (const m of cube.measures) {
            if (results.length >= MAX_SEARCH) break;
            if (
              m.name.toLowerCase().includes(keyword) ||
              m.description?.toLowerCase().includes(keyword) ||
              m.title?.toLowerCase().includes(keyword)
            ) {
              results.push({
                source: cube.name,
                sourceType,
                field: m.name,
                kind: "measure",
                type: m.type,
                description: m.description,
              });
            }
          }
          for (const d of cube.dimensions) {
            if (results.length >= MAX_SEARCH) break;
            if (
              d.name.toLowerCase().includes(keyword) ||
              d.description?.toLowerCase().includes(keyword) ||
              d.title?.toLowerCase().includes(keyword)
            ) {
              results.push({
                source: cube.name,
                sourceType,
                field: d.name,
                kind: "dimension",
                type: d.type,
                description: d.description,
              });
            }
          }
          for (const s of cube.segments) {
            if (results.length >= MAX_SEARCH) break;
            if (
              s.name.toLowerCase().includes(keyword) ||
              s.description?.toLowerCase().includes(keyword) ||
              s.title?.toLowerCase().includes(keyword)
            ) {
              results.push({
                source: cube.name,
                sourceType,
                field: s.name,
                kind: "segment",
                type: "segment",
                description: s.description,
              });
            }
          }
        }
        return results;
      }

      if (args.name) {
        const cube = cubes.find((c) => c.name === args.name);
        if (!cube) {
          return {
            error: `Source '${args.name}' not found. Available sources: ${cubes.map((c) => c.name).join(", ")}`,
          };
        }
        const dims = cube.dimensions.filter((d) => d.type !== "time");
        const timeDims = cube.dimensions.filter((d) => d.type === "time");
        return {
          name: cube.name,
          type: cube.type,
          description: cube.description,
          measures: cube.measures,
          dimensions: dims,
          timeDimensions: timeDims,
          segments: cube.segments,
        };
      }

      return cubes.map((c) => ({
        name: c.name,
        type: c.type,
        description: c.description,
        measures: c.measures.length,
        dimensions: c.dimensions.filter((d) => d.type !== "time").length,
        timeDimensions: c.dimensions.filter((d) => d.type === "time").length,
        segments: c.segments.length,
      }));
    },
  };

  const query: BonnardTool = {
    name: "query",
    description:
      "Query the semantic layer with measures, dimensions, filters, and time dimensions. " +
      'All field names must be fully qualified (e.g. "orders.revenue"). ' +
      "Use timeDimensions for date range constraints. Results are capped at 250 rows per response. " +
      'If data_completeness is "partial", use offset to fetch the next page.',
    schema: querySchema,
    execute: async (args) => {
      try {
        // Normalize singular timeDimension → timeDimensions array
        const timeDims = args.timeDimensions || (args.timeDimension ? [args.timeDimension] : undefined);

        // Validate timeDimensions reference actual time-type fields
        const timeDimError = await validateTimeDimensions(client, timeDims);
        if (timeDimError) return timeDimError;

        // Normalize filter dimension → member
        const filters = args.filters
          ?.map((f: { member?: string; dimension?: string; operator: string; values?: string[] }) => ({
            member: f.member || f.dimension,
            operator: f.operator,
            values: f.values,
          }))
          .filter((f: { member?: string }) => f.member);

        const userLimit = Math.min(args.limit || MAX_ROWS, 5000);

        const cubeQuery: Record<string, unknown> = {};
        if (args.measures && args.measures.length > 0) cubeQuery.measures = args.measures;
        if (args.dimensions) cubeQuery.dimensions = args.dimensions;
        if (timeDims) cubeQuery.timeDimensions = timeDims;
        if (filters && filters.length > 0) cubeQuery.filters = filters;
        if (args.segments) cubeQuery.segments = args.segments;
        cubeQuery.limit = userLimit + 1; // fetch one extra to detect if there's more data
        if (args.offset) cubeQuery.offset = args.offset;
        if (args.order) {
          cubeQuery.order = Object.fromEntries(
            args.order.map((o: { field: string; direction: string }) => [o.field, o.direction]),
          );
        }

        const result = await client.rawQuery(cubeQuery);
        const data = (result.data || []) as Record<string, unknown>[];

        if (data.length === 0) return { data_completeness: "complete", rows_shown: 0, results: [] };

        const isPartial = data.length > userLimit;
        const capped = data.slice(0, userLimit);
        const rows = stripPrefixes(capped);

        const response: Record<string, unknown> = {
          data_completeness: isPartial ? "partial" : "complete",
          rows_shown: rows.length,
          results: rows,
        };

        if (isPartial) {
          const nextOffset = (args.offset || 0) + rows.length;
          response.warning = `Partial results — do not sum or average these rows for totals. Use measures for accurate aggregations. To fetch more rows, use offset: ${nextOffset}.`;
        }

        response.hint =
          "To visualize these results, call visualize_read_me to learn chart options, then call visualize.";
        return response;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return {
          error: message,
          hint: "Check field names with explore_schema. Ensure all names are fully qualified (e.g. 'orders.revenue').",
        };
      }
    },
  };

  const sqlQuery: BonnardTool = {
    name: "sql_query",
    description:
      "Execute raw SQL against the semantic layer. Only use when the query tool cannot express what you need " +
      "(CTEs, UNIONs, custom arithmetic, CASE expressions). Use MEASURE() for aggregations.",
    schema: sqlQuerySchema,
    execute: async (args) => {
      try {
        const result = await client.sql(args.sql);
        const data = (result.data || []) as Record<string, unknown>[];

        if (data.length === 0) return { data_completeness: "complete", rows_shown: 0, results: [] };

        const capped = data.slice(0, MAX_ROWS);
        const isPartial = data.length > MAX_ROWS;
        const rows = stripPrefixes(capped);

        const response: Record<string, unknown> = {
          data_completeness: isPartial ? "partial" : "complete",
          rows_shown: rows.length,
          results: rows,
        };

        if (isPartial) {
          response.warning = `Partial results (${data.length} total). Do not sum or average these rows. Add LIMIT/OFFSET to your SQL to page through results.`;
        }

        return response;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const hint = generateSqlErrorHints(message, args.sql);
        return { error: message, hint };
      }
    },
  };

  const describeField: BonnardTool = {
    name: "describe_field",
    description:
      "Get detailed metadata for a specific field including its type, description, and which source it belongs to. " +
      'The field name must be fully qualified: "view_name.field_name" (e.g. "orders.revenue").',
    schema: describeFieldSchema,
    execute: async (args) => {
      const dotIndex = args.field.indexOf(".");
      if (dotIndex === -1) {
        return { error: 'Field must be fully qualified (e.g. "orders.revenue")' };
      }
      const sourceName = args.field.substring(0, dotIndex);
      const fieldName = args.field;

      const meta = await client.explore({ viewsOnly: false });
      const cube = meta.cubes.find((c) => c.name === sourceName);
      if (!cube) {
        return {
          error: `Source '${sourceName}' not found. Available sources: ${meta.cubes.map((c) => c.name).join(", ")}`,
        };
      }

      const sourceType = cube.type === "view" ? "view" : "cube";

      const measure = cube.measures.find((m) => m.name === fieldName);
      if (measure) {
        return {
          name: measure.name,
          kind: "measure",
          type: measure.type,
          description: measure.description,
          ...(measure.format && { format: measure.format }),
          ...(measure.meta && Object.keys(measure.meta).length > 0 && { meta: measure.meta }),
          source: cube.name,
          sourceType,
        };
      }

      const dimension = cube.dimensions.find((d) => d.name === fieldName);
      if (dimension) {
        return {
          name: dimension.name,
          kind: "dimension",
          type: dimension.type,
          description: dimension.description,
          ...(dimension.format && { format: dimension.format }),
          ...(dimension.meta && Object.keys(dimension.meta).length > 0 && { meta: dimension.meta }),
          source: cube.name,
          sourceType,
        };
      }

      const segment = cube.segments.find((s) => s.name === fieldName);
      if (segment) {
        return {
          name: segment.name,
          kind: "segment",
          type: "segment",
          description: segment.description,
          source: cube.name,
          sourceType,
        };
      }

      return { error: `Field '${fieldName}' not found in source '${sourceName}'` };
    },
  };

  const visualizeReadMe: BonnardTool = {
    name: "visualize_read_me",
    description:
      "Load visualization capabilities and chart options. Call this before using the visualize tool. " +
      "Returns available chart types, tool schema, and examples.",
    schema: z.object({}),
    execute: async () => {
      return {
        guide: `# Visualization Guide

## Chart Types
- **line** — time series, trends. Best with timeDimensions.
- **bar** — categorical comparisons. Auto-switches to horizontal when >8 categories.
- **area** — stacked composition over time. Use with stacking: "stacked".
- **pie** — part-of-whole. Best with ≤8 categories. Negative values are filtered out.
- **table** — tabular data. Shows all rows with pagination.

## visualize Tool Schema
\`\`\`
{
  chartType: "line" | "bar" | "area" | "pie" | "table"  (required)
  query: {                                                             (required)
    measures: ["view.measure_name"],
    dimensions: ["view.dimension_name"],        // optional
    timeDimensions: [{                          // optional
      dimension: "view.time_field",
      granularity: "day" | "week" | "month" | "quarter" | "year",
      dateRange: ["2025-01-01", "2025-12-31"]   // optional
    }],
    filters: [{ member: "view.field", operator: "equals", values: ["x"] }],
    order: { "view.field": "asc" | "desc" },
    limit: 250
  }
  title: "Chart Title"                          // optional — shown above chart
  stacking: "stacked" | "grouped" | "stacked100"  // optional — for bar/area
  horizontal: true | false                      // optional — for bar only
}
\`\`\`

## Examples

**Time series line chart:**
\`\`\`
visualize({
  chartType: "line",
  query: { measures: ["orders.revenue"], timeDimensions: [{ dimension: "orders.created_at", granularity: "month" }] },
  title: "Monthly Revenue"
})
\`\`\`

**Categorical bar chart:**
\`\`\`
visualize({
  chartType: "bar",
  query: { measures: ["orders.count"], dimensions: ["orders.status"] },
  title: "Orders by Status"
})
\`\`\`

**Multi-series (pivoted) area chart:**
\`\`\`
visualize({
  chartType: "area",
  query: { measures: ["orders.count"], dimensions: ["orders.category"], timeDimensions: [{ dimension: "orders.created_at", granularity: "month" }] },
  title: "Orders by Category Over Time",
  stacking: "stacked"
})
\`\`\`

## Filter Operators
- **equals** — match specific values: \`{ member: "field", operator: "equals", values: ["a", "b"] }\`
- **notEquals** — exclude specific values (same format)
- **contains** — case-insensitive substring search: \`{ member: "field", operator: "contains", values: ["search"] }\`
- **gt, gte, lt, lte** — numeric comparisons: \`{ member: "field", operator: "gte", values: ["100"] }\`
- **set / notSet** — null checks only, ignores values array: \`{ member: "field", operator: "set" }\` means "is not null"

**Important**: To filter to a list of specific items (e.g. top 10 partners), use \`equals\` with the values array — NOT \`set\`. The \`set\` operator only checks for non-null.

## Tips
- Always test your query with the query tool first to confirm it returns valid data.
- Use fully qualified field names: "view_name.field_name".
- For time series, use timeDimensions (not dimensions) for the time field.
- When charting by category with many values (>10), first identify the top N with the query tool, then pass those names as an \`equals\` filter in the visualize query.
- Currency and percentage formatting is auto-detected from field metadata.
- Null dimension values are labeled "(No value)" automatically.
- Missing time intervals are filled automatically.`,
        next_step: "Now call visualize() with chartType and a tested query.",
      };
    },
  };

  const visualizeSchema = z.object({
    chartType: z.enum(["line", "bar", "area", "pie", "table"]).describe("Chart type to render"),
    query: z
      .object({
        measures: z.array(z.string()).optional().describe('Measures (e.g. ["orders.revenue"])'),
        dimensions: z.array(z.string()).optional().describe("Dimensions to group by"),
        timeDimensions: z
          .array(
            z.object({
              dimension: z.string(),
              granularity: z.enum(["day", "week", "month", "quarter", "year"]).optional(),
              dateRange: z.tuple([z.string(), z.string()]).optional(),
            }),
          )
          .optional()
          .describe("Time dimensions"),
        filters: z
          .array(
            z.object({
              member: z.string(),
              operator: z.enum([
                "equals",
                "notEquals",
                "contains",
                "notContains",
                "gt",
                "gte",
                "lt",
                "lte",
                "set",
                "notSet",
                "inDateRange",
                "notInDateRange",
                "beforeDate",
                "afterDate",
              ]),
              values: z.array(z.string()).optional(),
            }),
          )
          .optional(),
        order: z.record(z.enum(["asc", "desc"])).optional(),
        limit: z.number().optional(),
      })
      .describe("Query to execute — same format as the query tool"),
    title: z.string().optional().describe("Chart title displayed above the chart"),
    stacking: z.enum(["stacked", "grouped", "stacked100"]).optional().describe("Stacking mode for bar/area charts"),
    horizontal: z.boolean().optional().describe("Horizontal bars (bar chart only)"),
  });

  const visualize: BonnardTool = {
    name: "visualize",
    description:
      "Render an interactive chart from a semantic layer query. " +
      "You must have called visualize_read_me earlier in this conversation to learn the options. " +
      "You should have tested the query via the query tool first to confirm it returns valid data. " +
      "Pass the same query that worked with the query tool.",
    schema: visualizeSchema,
    execute: async (args) => {
      try {
        // Normalize query (same as query tool)
        const queryArgs = args.query;
        const timeDims = queryArgs.timeDimensions;

        // Validate timeDimensions reference actual time-type fields
        const timeDimError = await validateTimeDimensions(client, timeDims);
        if (timeDimError) return { type: "visualization", ...timeDimError };

        const filters = queryArgs.filters?.map((f: { member: string; operator: string; values?: string[] }) => ({
          member: f.member,
          operator: f.operator,
          values: f.values,
        }));

        const cubeQuery: Record<string, unknown> = {};
        if (queryArgs.measures?.length) cubeQuery.measures = queryArgs.measures;
        if (queryArgs.dimensions) cubeQuery.dimensions = queryArgs.dimensions;
        if (timeDims) cubeQuery.timeDimensions = timeDims;
        if (filters?.length) cubeQuery.filters = filters;
        if (queryArgs.order) cubeQuery.order = queryArgs.order;
        cubeQuery.limit = Math.min(queryArgs.limit || MAX_ROWS, MAX_ROWS);

        // Execute query
        const result = await client.rawQuery(cubeQuery);
        const data = (result.data || []) as Record<string, unknown>[];

        if (data.length === 0) {
          return {
            type: "visualization",
            error: "Query returned no data. Check your filters or try a broader query.",
          };
        }

        // Fetch meta for the primary view
        const meta = await client.explore({ viewsOnly: false });
        const primaryMeasure = queryArgs.measures?.[0] || "";
        const viewName = primaryMeasure.includes(".") ? primaryMeasure.split(".")[0] : "";
        const viewMeta = meta.cubes.find((c) => c.name === viewName);

        if (!viewMeta) {
          return {
            type: "visualization",
            error: `View '${viewName}' not found in schema.`,
          };
        }

        // Return everything the client needs to call resolve() and render
        return {
          type: "visualization",
          chartType: args.chartType,
          title: args.title,
          stacking: args.stacking,
          horizontal: args.horizontal,
          data,
          meta: {
            name: viewMeta.name,
            measures: viewMeta.measures,
            dimensions: viewMeta.dimensions,
          },
          query: {
            measures: queryArgs.measures || [],
            dimensions: queryArgs.dimensions,
            timeDimensions: timeDims,
          },
          rows: data.length,
        };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return {
          type: "visualization",
          error: `Query failed: ${message}`,
          hint: "Verify field names with explore_schema. Test the query with the query tool first.",
        };
      }
    },
  };

  return [exploreSchema, query, sqlQuery, describeField, visualizeReadMe, visualize];
}
