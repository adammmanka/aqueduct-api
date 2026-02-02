import { consumeVerificationToken } from "@/lib/verificationToken";

export async function GET(req: Request) {
  const expected = process.env.AQUEDUCT_ADMIN_SECRET;
  if (!expected) {
    return new Response("Missing AQUEDUCT_ADMIN_SECRET", { status: 500 });
  }

  const provided = req.headers.get("x-aqueduct-admin-secret") ?? "";
  if (provided !== expected) {
    return new Response("Unauthorized", { status: 401 });
  }

  const tok = await consumeVerificationToken();
  if (!tok) {
    return new Response("No token available", { status: 404 });
  }

  return Response.json({ verification_token: tok });
}
