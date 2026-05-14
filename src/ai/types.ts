import type { z } from "zod";
import type { createClient } from "../client.js";

export type BonnardClient = ReturnType<typeof createClient>;

export interface BonnardTool<T = any> {
  name: string;
  description: string;
  schema: z.ZodType<T>;
  execute: (args: T) => Promise<unknown>;
}
