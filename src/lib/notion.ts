import crypto from "node:crypto";
import { getLimiter, throttleOrWait } from "./ratelimit";

export const NOTION_API_KEY = process.env.NOTION_API_KEY;
export const NOTION_VERSION = process.env.NOTION_VERSION ?? "2025-09-03";

export function hmacSha256Hex(secret: string, payload: string): string {
  return crypto.createHmac("sha256", secret).update(payload).digest("hex");
}

export function timingSafeEqualHex(a: string, b: string): boolean {
  try {
    const ab = Buffer.from(a, "hex");
    const bb = Buffer.from(b, "hex");
    if (ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
  } catch {
    return false;
  }
}

export async function notionFetch(path: string, init: RequestInit) {
  if (!NOTION_API_KEY) throw new Error("Missing NOTION_API_KEY");

  // Global Notion API throttle (and can be keyed per workspace if needed later)
  const notionLimiter = getLimiter("notion");
  await throttleOrWait(notionLimiter, "global");

  const res = await fetch(`https://api.notion.com/v1${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${NOTION_API_KEY}`,
      "Notion-Version": NOTION_VERSION,
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
    cache: "no-store",
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Notion API error ${res.status}: ${text}`);
  }

  return res.json();
}

type NotionDatabaseGetResponse = {
  data_sources?: Array<{ id?: string }>;
};

export async function getDatabaseDataSourceId(databaseId: string): Promise<string> {
  const db = (await notionFetch(`/databases/${databaseId}`, {
    method: "GET",
  })) as NotionDatabaseGetResponse;

  const ds = db?.data_sources?.[0]?.id;
  if (!ds) throw new Error("Database has no data_sources[0].id");
  return ds;
}
