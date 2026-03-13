import { DynamicStructuredTool } from "@langchain/core/tools";
import { createTools as createBonnardTools } from "./tools.js";
import type { BonnardClient } from "./types.js";

/**
 * Create Bonnard tools for LangChain / LangGraph.
 *
 * Returns a `DynamicStructuredTool[]` so tools can be spread directly:
 * ```ts
 * import { createTools } from "@bonnard/sdk/ai/langchain"
 * const tools = createTools(bonnardClient)
 * const agent = createReactAgent({ llm, tools: [...tools, ...myOtherTools] })
 * ```
 */
export function createTools(client: BonnardClient) {
  const bonnardTools = createBonnardTools(client);

  return bonnardTools.map(
    (t) =>
      new DynamicStructuredTool({
        name: t.name,
        description: t.description,
        schema: t.schema as any,
        func: async (args: any) => JSON.stringify(await t.execute(args)),
      })
  );
}
