export function isValidWhatsAppVersion(value) {
  return Array.isArray(value)
    && value.length === 3
    && value.every((part) => Number.isSafeInteger(part) && part >= 0);
}

export function sameWhatsAppVersion(left, right) {
  return isValidWhatsAppVersion(left)
    && isValidWhatsAppVersion(right)
    && left.every((part, index) => part === right[index]);
}

export async function resolveWhatsAppVersion({
  fetchBaileysVersion,
  fetchWaWebVersion,
  fallbackVersion
}) {
  const attempts = [
    ["baileys-master", fetchBaileysVersion],
    ["whatsapp-web", fetchWaWebVersion]
  ];
  let usableFallback = isValidWhatsAppVersion(fallbackVersion)
    ? [...fallbackVersion]
    : null;
  const errors = [];

  for (const [source, fetchVersion] of attempts) {
    if (typeof fetchVersion !== "function") continue;
    try {
      const result = await fetchVersion();
      if (isValidWhatsAppVersion(result?.version) && result?.isLatest) {
        return {
          version: [...result.version],
          source,
          verified: true,
          errors
        };
      }
      if (!usableFallback && isValidWhatsAppVersion(result?.version)) {
        usableFallback = [...result.version];
      }
      if (result?.error) errors.push(result.error);
    } catch (error) {
      errors.push(error);
    }
  }

  if (usableFallback) {
    return {
      version: usableFallback,
      source: "cached-or-package-default",
      verified: false,
      errors
    };
  }

  const error = new Error("Unable to resolve a usable WhatsApp Web protocol version.");
  error.code = "WHATSAPP_VERSION_UNAVAILABLE";
  throw error;
}
