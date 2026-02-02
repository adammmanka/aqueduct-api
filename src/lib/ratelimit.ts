import { Redis } from "@upstash/redis";
import { Ratelimit } from "@upstash/ratelimit";

function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

export type LimiterName = "notion" | "external";

export function getRedis(): Redis | null {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (url && token) return new Redis({ url, token });

  // (Optional) support REDIS_URL in the future if we add a different client.
  return null;
}

export function getLimiter(name: LimiterName): {
  limit: (key: string) => Promise<{ ok: boolean; resetMs?: number; remaining?: number }>;
} {
  const redis = getRedis();

  // No Redis configured: do nothing (developer-friendly), but keep the interface.
  if (!redis) {
    return {
      limit: async () => ({ ok: true }),
    };
  }

  if (name === "notion") {
    const rps = intEnv("AQUEDUCT_NOTION_RPS", 3);
    const rl = new Ratelimit({
      redis,
      limiter: Ratelimit.fixedWindow(rps, "1 s"),
      prefix: "aqueduct:rl:notion",
    });
    return {
      limit: async (key: string) => {
        const res = await rl.limit(key);
        return { ok: res.success, resetMs: res.reset, remaining: res.remaining };
      },
    };
  }

  // external
  const rpm = intEnv("AQUEDUCT_EXTERNAL_RPM", 60);
  const rl = new Ratelimit({
    redis,
    limiter: Ratelimit.fixedWindow(rpm, "60 s"),
    prefix: "aqueduct:rl:external",
  });
  return {
    limit: async (key: string) => {
      const res = await rl.limit(key);
      return { ok: res.success, resetMs: res.reset, remaining: res.remaining };
    },
  };
}

export async function throttleOrWait(limiter: ReturnType<typeof getLimiter>, key: string) {
  // Simple: if limited, sleep until reset.
  // This keeps the worker single-threaded and prevents bursts.
  while (true) {
    const res = await limiter.limit(key);
    if (res.ok) return;
    const now = Date.now();
    const waitMs = Math.max(250, (res.resetMs ?? now + 1000) - now);
    await new Promise((r) => setTimeout(r, waitMs));
  }
}
