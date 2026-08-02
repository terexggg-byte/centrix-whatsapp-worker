import test from "node:test";
import assert from "node:assert/strict";
import {
  resolveWhatsAppVersion,
  sameWhatsAppVersion
} from "../scripts/whatsapp-version.mjs";

test("uses the maintained Baileys protocol version instead of a stale package default", async () => {
  const resolved = await resolveWhatsAppVersion({
    fetchBaileysVersion: async () => ({ version: [2, 3000, 1043857760], isLatest: true }),
    fetchWaWebVersion: async () => ({ version: [2, 3000, 1044300879], isLatest: true }),
    fallbackVersion: [2, 3000, 1035194821]
  });

  assert.deepEqual(resolved.version, [2, 3000, 1043857760]);
  assert.equal(resolved.source, "baileys-master");
  assert.equal(resolved.verified, true);
});

test("falls back to WhatsApp Web and then to the last usable version", async () => {
  const webVersion = await resolveWhatsAppVersion({
    fetchBaileysVersion: async () => ({ version: [2, 3000, 1035194821], isLatest: false }),
    fetchWaWebVersion: async () => ({ version: [2, 3000, 1044300879], isLatest: true })
  });
  assert.deepEqual(webVersion.version, [2, 3000, 1044300879]);

  const cachedVersion = await resolveWhatsAppVersion({
    fetchBaileysVersion: async () => {
      throw new Error("offline");
    },
    fetchWaWebVersion: async () => ({ version: null, isLatest: false }),
    fallbackVersion: webVersion.version
  });
  assert.equal(cachedVersion.verified, false);
  assert.equal(sameWhatsAppVersion(cachedVersion.version, webVersion.version), true);
});
