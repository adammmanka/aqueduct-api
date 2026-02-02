#!/usr/bin/env node

/**
 * Events Queue worker (MVP)
 *
 * - Pulls "New" rows from the Notion Events Queue DB
 * - Dedupe by Event ID (best-effort)
 * - Applies Upstash-based rate limiting before Notion calls
 * - Marks unknown events as Needs human review
 */

import { Redis } from "@upstash/redis";
import { Ratelimit } from "@upstash/ratelimit";

const NOTION_API_KEY = process.env.NOTION_API_KEY;
const NOTION_VERSION = process.env.NOTION_VERSION ?? "2025-09-03";
const DB_ID = process.env.NOTION_EVENTS_QUEUE_DB_ID;

if (!NOTION_API_KEY) throw new Error("Missing NOTION_API_KEY");
if (!DB_ID) throw new Error("Missing NOTION_EVENTS_QUEUE_DB_ID");

const url = process.env.UPSTASH_REDIS_REST_URL;
const token = process.env.UPSTASH_REDIS_REST_TOKEN;
const redis = url && token ? new Redis({ url, token }) : null;

const notionRps = Number(process.env.AQUEDUCT_NOTION_RPS ?? "3");
const limiter = redis
  ? new Ratelimit({
      redis,
      limiter: Ratelimit.fixedWindow(
        Number.isFinite(notionRps) && notionRps > 0 ? notionRps : 3,
        "1 s"
      ),
      prefix: "aqueduct:rl:notion",
    })
  : null;

async function throttle() {
  if (!limiter) return;
  while (true) {
    const res = await limiter.limit("global");
    if (res.success) return;
    const waitMs = Math.max(250, res.reset - Date.now());
    await new Promise((r) => setTimeout(r, waitMs));
  }
}

async function notionFetch(path, init) {
  await throttle();

  const res = await fetch(`https://api.notion.com/v1${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${NOTION_API_KEY}`,
      "Notion-Version": NOTION_VERSION,
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    cache: "no-store",
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Notion API error ${res.status}: ${text}`);
  }

  return res.json();
}

async function getDataSourceId(databaseId) {
  const db = await notionFetch(`/databases/${databaseId}`, { method: "GET" });
  const ds = db?.data_sources?.[0]?.id;
  if (!ds) throw new Error("Database has no data_sources[0].id");
  return ds;
}

async function queryNewEvents(dataSourceId, pageSize = 25) {
  // Assumes a select property "Status" exists with value "New".
  const body = {
    page_size: pageSize,
    filter: {
      property: "Status",
      select: { equals: "New" },
    },
    // Prefer deterministic order, but don't assume a custom "Created" property exists.
    sorts: [{ timestamp: "created_time", direction: "ascending" }],
  };

  return notionFetch(`/data_sources/${dataSourceId}/query`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

async function findByEventId(dataSourceId, eventId) {
  const body = {
    page_size: 10,
    filter: { property: "Event ID", rich_text: { equals: eventId } },
  };
  return notionFetch(`/data_sources/${dataSourceId}/query`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

async function updatePage(pageId, properties) {
  return notionFetch(`/pages/${pageId}`, {
    method: "PATCH",
    body: JSON.stringify({ properties }),
  });
}

function getRichText(page, propName) {
  const p = page?.properties?.[propName];
  const rt = p?.rich_text;
  if (!Array.isArray(rt) || rt.length === 0) return "";
  return rt.map((x) => x?.plain_text ?? "").join("").trim();
}

async function processEvent(page, dataSourceId) {
  const pageId = page.id;
  const eventId = getRichText(page, "Event ID");
  const type = page?.properties?.Type?.select?.name ?? "Other";

  // Mark in-progress first (best-effort)
  await updatePage(pageId, {
    Status: { select: { name: "In Progress" } },
  });

  // Dedupe: if multiple pages share same Event ID, keep the oldest (current one if it is oldest)
  if (eventId) {
    const q = await findByEventId(dataSourceId, eventId);
    const results = Array.isArray(q?.results) ? q.results : [];
    if (results.length > 1) {
      // Sort by created_time
      results.sort((a, b) =>
        String(a?.created_time ?? "").localeCompare(String(b?.created_time ?? ""))
      );
      const keeper = results[0]?.id;
      if (keeper && keeper !== pageId) {
        await updatePage(pageId, {
          Status: { select: { name: "Deduped" } },
          "Needs human review": { checkbox: false },
          "Scipio log": {
            rich_text: [
              {
                type: "text",
                text: {
                  content: `Worker: deduped (keeper=${keeper}) for eventId=${eventId}`,
                },
              },
            ],
          },
        });
        return;
      }
    }
  }

  // Dispatch (MVP): we don’t have automated handlers yet, so flag for triage.
  // Later: route by Type / Notion object type.
  await updatePage(pageId, {
    Status: { select: { name: "Needs human review" } },
    "Needs human review": { checkbox: true },
    "Scipio log": {
      rich_text: [
        {
          type: "text",
          text: {
            content: `Worker: no handler for type=${type}. Marked Needs human review.`,
          },
        },
      ],
    },
  });
}

async function main() {
  const dsId = await getDataSourceId(DB_ID);
  const q = await queryNewEvents(dsId, 25);
  const results = Array.isArray(q?.results) ? q.results : [];

  console.log(`Found ${results.length} new events`);

  for (const page of results) {
    try {
      await processEvent(page, dsId);
      console.log(`Processed ${page.id}`);
    } catch (err) {
      console.error(`Error processing ${page?.id}:`, err);
      // best-effort: mark failed
      try {
        await updatePage(page.id, {
          Status: { select: { name: "Error" } },
          "Needs human review": { checkbox: true },
          "Scipio log": {
            rich_text: [
              {
                type: "text",
                text: { content: `Worker: error: ${String(err).slice(0, 500)}` },
              },
            ],
          },
        });
      } catch {
        // ignore
      }
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
