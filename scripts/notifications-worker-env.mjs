function stripWrappingQuotes(value) {
  let next = String(value || "").trim();
  for (let index = 0; index < 3; index += 1) {
    if (
      (next.startsWith('"') && next.endsWith('"'))
      || (next.startsWith("'") && next.endsWith("'"))
      || (next.startsWith("`") && next.endsWith("`"))
    ) {
      next = next.slice(1, -1).trim();
      continue;
    }
    break;
  }
  return next;
}

function stripEnvAssignment(key, value) {
  const assignment = new RegExp(`^(?:export\\s+)?${key}\\s*=\\s*([\\s\\S]*)$`);
  const match = String(value || "").trim().match(assignment);
  return match ? match[1] : value;
}

export function normalizeSecretEnv(key) {
  const raw = process.env[key];
  if (!raw) return raw;
  const normalized = stripWrappingQuotes(stripEnvAssignment(key, raw));
  process.env[key] = normalized;
  return normalized;
}

export function normalizeDatabaseUrlEnv() {
  const raw = process.env.DATABASE_URL;
  if (!raw) return raw;
  let normalized = stripWrappingQuotes(stripEnvAssignment("DATABASE_URL", raw));
  const embeddedUrl = normalized.match(/postgres(?:ql)?:\/\/[^\s"'`]+/i)?.[0];
  if (embeddedUrl) normalized = stripWrappingQuotes(embeddedUrl);
  process.env.DATABASE_URL = normalized;
  return normalized;
}

export function normalizeWorkerEnv() {
  normalizeDatabaseUrlEnv();
  normalizeSecretEnv("WHATSAPP_AUTH_ENCRYPTION_KEY");
  normalizeSecretEnv("NOTIFICATION_AUTH_ENCRYPTION_KEY");
  normalizeSecretEnv("CARD_EXPORT_WORKER_SECRET");
}
