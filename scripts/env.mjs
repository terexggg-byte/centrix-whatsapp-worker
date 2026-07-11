function stripWrappingQuotes(value) {
  let next = String(value || "").trim();
  for (let i = 0; i < 3; i += 1) {
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
  if (!raw) {
    return raw;
  }

  const normalized = stripWrappingQuotes(stripEnvAssignment(key, raw));
  process.env[key] = normalized;
  return normalized;
}

export function normalizeDatabaseUrlEnv() {
  const raw = process.env.DATABASE_URL;
  if (!raw) {
    return raw;
  }

  let normalized = stripWrappingQuotes(stripEnvAssignment("DATABASE_URL", raw));
  const embeddedUrl = normalized.match(/postgres(?:ql)?:\/\/[^\s"'`]+/i)?.[0];
  if (embeddedUrl) {
    normalized = stripWrappingQuotes(embeddedUrl);
  }

  if (!/^postgres(?:ql)?:\/\//i.test(normalized)) {
    throw new Error(
      "DATABASE_URL must be the raw production Postgres URL starting with postgresql:// or postgres://. "
      + "If you copied from a .env file, paste only the value after DATABASE_URL= into the GitHub Secret."
    );
  }

  process.env.DATABASE_URL = normalized;
  return normalized;
}

export function normalizeWorkerEnv() {
  normalizeDatabaseUrlEnv();
  normalizeSecretEnv("WHATSAPP_AUTH_ENCRYPTION_KEY");
  normalizeSecretEnv("NOTIFICATION_AUTH_ENCRYPTION_KEY");
}
