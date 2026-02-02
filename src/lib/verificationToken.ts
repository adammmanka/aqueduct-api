import { Redis } from "@upstash/redis";

const KEY = "aqueduct:notion:verification_token";
const USED_KEY = "aqueduct:notion:verification_token_used";

function getRedis(): Redis | null {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  return new Redis({ url, token });
}

export async function storeVerificationToken(token: string) {
  const redis = getRedis();
  if (!redis) return; // no-op if not configured

  // Store for a short window; one-time retrieval.
  await redis.set(KEY, token, { ex: 10 * 60 }); // 10 minutes
  await redis.del(USED_KEY);
}

export async function consumeVerificationToken(): Promise<string | null> {
  const redis = getRedis();
  if (!redis) return null;

  const alreadyUsed = await redis.get<string>(USED_KEY);
  if (alreadyUsed) return null;

  const tok = await redis.get<string>(KEY);
  if (!tok) return null;

  // one-time: mark used and delete token
  await redis.set(USED_KEY, "1", { ex: 10 * 60 });
  await redis.del(KEY);
  return tok;
}
