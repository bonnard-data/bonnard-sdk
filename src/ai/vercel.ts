import { tool, type CoreTool } from "ai";
import { createTools as createBonnardTools } from "./tools.js";
import type { BonnardClient } from "./types.js";

/**
 * Create Bonnard tools for the Vercel AI SDK.
 *
 * Returns a `Record<string, CoreTool>` so tools can be spread directly:
 * ```ts
 * import { createTools } from "@bonnard/sdk/ai/vercel"
 * const tools = createTools(bonnardClient)
 * const result = await generateText({ model, tools: { ...tools, ...myOtherTools } })
 * ```
 */
export function createTools(client: BonnardClient): Record<string, CoreTool> {
  const bonnardTools = createBonnardTools(client);

  const tools: Record<string, CoreTool> = {};
  for (const t of bonnardTools) {
    tools[t.name] = tool({
      description: t.description,
      parameters: t.schema,
      execute: t.execute,
    });
  }

  return tools;
}
