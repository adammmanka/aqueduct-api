import {
  getDatabaseDataSourceId,
  hmacSha256Hex,
  notionFetch,
  timingSafeEqualHex,
} from "@/lib/notion";

const EVENTS_QUEUE_DB_ID = process.env.NOTION_EVENTS_QUEUE_DB_ID;

// This is Notion's subscription verification token.
// Notion uses it to sign webhook payloads (HMAC-SHA256) into X-Notion-Signature.
const WEBHOOK_VERIFICATION_TOKEN =
  process.env.NOTION_WEBHOOK_VERIFICATION_TOKEN ??
  process.env.NOTION_WEBHOOK_SECRET;

async function eventExists(dataSourceId: string, eventId: string): Promise<boolean> {
  // Filter on the "Event ID" rich_text property.
  const body = {
    page_size: 1,
    filter: {
      property: "Event ID",
      rich_text: { equals: eventId },
    },
  };

  const q = await notionFetch(`/data_sources/${dataSourceId}/query`, {
    method: "POST",
    body: JSON.stringify(body),
  });

  return Array.isArray(q?.results) && q.results.length > 0;
}

async function writeEventToQueue(params: {
  databaseId: string;
  type: string;
  eventId: string;
  objectType?: string;
  objectId?: string;
  sourceUrl?: string;
  payloadJson: string;
}) {
  const now = new Date().toISOString();

  const name = `${params.type} • ${params.objectType ?? "object"}:${params.objectId ?? ""}`.trim();

  // Note: property names must match your Events Queue DB schema.
  const page = {
    parent: { database_id: params.databaseId },
    properties: {
      Name: { title: [{ type: "text", text: { content: name.slice(0, 200) } }] },
      "Event ID": {
        rich_text: [{ type: "text", text: { content: params.eventId } }],
      },
      Type: { select: { name: params.type } },
      "Notion object type": params.objectType
        ? { select: { name: params.objectType } }
        : undefined,
      "Notion object id": params.objectId
        ? { rich_text: [{ type: "text", text: { content: params.objectId } }] }
        : undefined,
      "Source URL": params.sourceUrl ? { url: params.sourceUrl } : undefined,
      Status: { select: { name: "New" } },
      "Needs human review": { checkbox: false },
      "Payload (json)": {
        rich_text: [{ type: "text", text: { content: params.payloadJson.slice(0, 1900) } }],
      },
      Created: { date: { start: now } },
      "Scipio log": {
        rich_text: [
          {
            type: "text",
            text: {
              content: `Aqueduct: queued event ${params.eventId} (${params.type}) at ${now}`,
            },
          },
        ],
      },
    },
  };

  // Strip undefined properties (Notion rejects them).
  const props = page.properties as Record<string, unknown>;
  for (const [k, v] of Object.entries(props)) {
    if (v === undefined) delete props[k];
  }

  await notionFetch(`/pages`, { method: "POST", body: JSON.stringify(page) });
}

export async function POST(req: Request) {
  const rawBody = await req.text();
  const signature = req.headers.get("x-notion-signature") ?? "";

  // 1) Subscription verification (one-time)
  // Notion sends {"verification_token":"..."} to the webhook URL.
  try {
    const parsed = JSON.parse(rawBody);

    if (parsed?.verification_token) {
      // Notion subscription verification bootstrap.
      // We log a redacted token so you can confirm receipt in Vercel logs.
      const tok = String(parsed.verification_token);
      const redacted = tok.length <= 12 ? tok : `${tok.slice(0, 8)}…${tok.slice(-4)}`;
      console.log(`[notion] verification_token received: ${redacted}`);
      return Response.json({ verification_token: parsed.verification_token });
    }
  } catch {
    // ignore
  }

  // 2) Validate signature (recommended)
  if (!WEBHOOK_VERIFICATION_TOKEN) {
    return new Response("Missing NOTION_WEBHOOK_VERIFICATION_TOKEN", { status: 500 });
  }

  const computed = hmacSha256Hex(WEBHOOK_VERIFICATION_TOKEN, rawBody);
  if (!signature || !timingSafeEqualHex(signature, computed)) {
    return new Response("Invalid signature", { status: 401 });
  }

  // 3) Parse event payload
  let payload: unknown;
  try {
    payload = JSON.parse(rawBody) as unknown;
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }

  if (!EVENTS_QUEUE_DB_ID) {
    return new Response("Missing NOTION_EVENTS_QUEUE_DB_ID", { status: 500 });
  }

  // Notion may send a single event object.
  // (If they ever send arrays/aggregates, we can extend later.)
  const evt = payload as {
    id?: string;
    type?: string;
    entity?: { type?: string; id?: string };
  };

  const eventId: string | undefined = evt.id;
  const type: string = evt.type ?? "Other";
  const entityType: string | undefined = evt.entity?.type;
  const entityId: string | undefined = evt.entity?.id;

  if (!eventId) {
    return new Response("Missing event id", { status: 400 });
  }

  // 4) Deduplicate
  const dsId = await getDatabaseDataSourceId(EVENTS_QUEUE_DB_ID);
  const exists = await eventExists(dsId, eventId);
  if (exists) {
    return Response.json({ ok: true, deduped: true });
  }

  // 5) Write to Events Queue
  await writeEventToQueue({
    databaseId: EVENTS_QUEUE_DB_ID,
    type,
    eventId,
    objectType: entityType,
    objectId: entityId,
    sourceUrl: undefined,
    payloadJson: JSON.stringify(payload),
  });

  return Response.json({ ok: true });
}
