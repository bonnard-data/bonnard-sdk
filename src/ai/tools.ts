import { z } from "zod";
import type { BonnardClient, BonnardTool } from "./types.js";

// --- Shared helpers ---

const MAX_ROWS = 250;

function normalizeValue(val: unknown): unknown {
  if (val === null || val === undefined) return null;
  if (typeof val === "number") {
    if (!Number.isInteger(val)) return Math.round(val * 100) / 100;
    return val;
  }
  if (typeof val === "string" && val !== "" && !isNaN(Number(val))) {
    const num = Number(val);
    if (!Number.isInteger(num)) return Math.round(num * 100) / 100;
    return num;
  }
  return val;
}

function stripPrefixes(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (rows.length === 0) return rows;
  const keys = Object.keys(rows[0]);
  return rows.map((row) => {
    const cleaned: Record<string, unknown> = {};
    for (const key of keys) {
      const shortKey = key.split(".").pop() || key;
      cleaned[shortKey] = normalizeValue(row[key]);
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
  dimension: z.string().describe("Time dimension (e.g. \"orders.created_at\")"),
  granularity: z.enum(["day", "week", "month", "quarter", "year"]).optional().describe("Time granularity for grouping"),
  dateRange: z.tuple([z.string(), z.string()]).optional().describe("Date range as [start, end] in YYYY-MM-DD format"),
});

const querySchema = z.object({
  measures: z.array(z.string()).optional().describe("Measures to query (e.g. [\"orders.revenue\", \"orders.count\"])"),
  dimensions: z.array(z.string()).optional().describe("Dimensions to group by (e.g. [\"orders.status\"])"),
  timeDimensions: z.array(timeDimensionObject).optional().describe("Time dimensions with date range and optional granularity"),
  timeDimension: timeDimensionObject.optional().describe("Alias for timeDimensions (single object)"),
  filters: z.array(z.object({
    member: z.string().optional().describe("Field to filter (e.g. \"orders.status\")"),
    dimension: z.string().optional().describe("Alias for member"),
    operator: z.enum(["equals", "notEquals", "contains", "notContains", "gt", "gte", "lt", "lte", "set", "notSet", "inDateRange", "notInDateRange", "beforeDate", "afterDate"]).describe("Filter operator"),
    values: z.array(z.string()).optional().describe("Values to filter by (not needed for set/notSet operators)"),
  })).optional().describe("Filters to apply"),
  segments: z.array(z.string()).optional().describe("Pre-defined filter segments"),
  order: z.record(z.enum(["asc", "desc"])).optional().describe("Sort order (e.g. {\"orders.revenue\": \"desc\"})"),
  limit: z.number().optional().describe("Maximum rows to return (default: 250, max: 5000)"),
  offset: z.number().optional().describe("Number of rows to skip for pagination"),
});

const sqlQuerySchema = z.object({
  sql: z.string().describe("SQL query using Cube SQL syntax with MEASURE() for aggregations"),
});

const describeFieldSchema = z.object({
  field: z.string().describe("Fully qualified field name (e.g. \"orders.revenue\")"),
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
        const results: Array<{ source: string; sourceType: string; field: string; kind: string; type: string; description?: string }> = [];
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
              results.push({ source: cube.name, sourceType, field: m.name, kind: "measure", type: m.type, description: m.description });
            }
          }
          for (const d of cube.dimensions) {
            if (results.length >= MAX_SEARCH) break;
            if (
              d.name.toLowerCase().includes(keyword) ||
              d.description?.toLowerCase().includes(keyword) ||
              d.title?.toLowerCase().includes(keyword)
            ) {
              results.push({ source: cube.name, sourceType, field: d.name, kind: "dimension", type: d.type, description: d.description });
            }
          }
          for (const s of cube.segments) {
            if (results.length >= MAX_SEARCH) break;
            if (
              s.name.toLowerCase().includes(keyword) ||
              s.description?.toLowerCase().includes(keyword) ||
              s.title?.toLowerCase().includes(keyword)
            ) {
              results.push({ source: cube.name, sourceType, field: s.name, kind: "segment", type: "segment", description: s.description });
            }
          }
        }
        return results;
      }

      if (args.name) {
        const cube = cubes.find((c) => c.name === args.name);
        if (!cube) {
          return { error: `Source '${args.name}' not found. Available sources: ${cubes.map((c) => c.name).join(", ")}` };
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
      "All field names must be fully qualified (e.g. \"orders.revenue\"). " +
      "Use timeDimensions for date range constraints. Results are capped at 250 rows per response. " +
      "If data_completeness is \"partial\", use offset to fetch the next page.",
    schema: querySchema,
    execute: async (args) => {
      // Normalize singular timeDimension → timeDimensions array
      const timeDims = args.timeDimensions
        || (args.timeDimension ? [args.timeDimension] : undefined);

      // Normalize filter dimension → member
      const filters = args.filters?.map((f: { member?: string; dimension?: string; operator: string; values?: string[] }) => ({
        member: f.member || f.dimension,
        operator: f.operator,
        values: f.values,
      })).filter((f: { member?: string }) => f.member);

      const requestLimit = Math.min(args.limit || MAX_ROWS, 5000);

      const cubeQuery: Record<string, unknown> = {};
      if (args.measures && args.measures.length > 0) cubeQuery.measures = args.measures;
      if (args.dimensions) cubeQuery.dimensions = args.dimensions;
      if (timeDims) cubeQuery.timeDimensions = timeDims;
      if (filters && filters.length > 0) cubeQuery.filters = filters;
      if (args.segments) cubeQuery.segments = args.segments;
      cubeQuery.limit = requestLimit;
      if (args.offset) cubeQuery.offset = args.offset;
      if (args.order) cubeQuery.order = args.order;

      const result = await client.rawQuery(cubeQuery);
      const data = (result.data || []) as Record<string, unknown>[];

      if (data.length === 0) return { data_completeness: "complete", rows_shown: 0, results: [] };

      const capped = data.slice(0, MAX_ROWS);
      const isPartial = data.length > MAX_ROWS || data.length >= requestLimit;
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

      return response;
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
        const hints = generateSqlErrorHints(message, args.sql);
        return { error: message, hints };
      }
    },
  };

  const describeField: BonnardTool = {
    name: "describe_field",
    description:
      "Get detailed metadata for a specific field including its type, description, and which source it belongs to. " +
      "The field name must be fully qualified: \"view_name.field_name\" (e.g. \"orders.revenue\").",
    schema: describeFieldSchema,
    execute: async (args) => {
      const dotIndex = args.field.indexOf(".");
      if (dotIndex === -1) {
        return { error: "Field must be fully qualified (e.g. \"orders.revenue\")" };
      }
      const sourceName = args.field.substring(0, dotIndex);
      const fieldName = args.field;

      const meta = await client.explore({ viewsOnly: false });
      const cube = meta.cubes.find((c) => c.name === sourceName);
      if (!cube) {
        return { error: `Source '${sourceName}' not found. Available sources: ${meta.cubes.map((c) => c.name).join(", ")}` };
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
        return { name: segment.name, kind: "segment", type: "segment", description: segment.description, source: cube.name, sourceType };
      }

      return { error: `Field '${fieldName}' not found in source '${sourceName}'` };
    },
  };

  return [exploreSchema, query, sqlQuery, describeField];
}
