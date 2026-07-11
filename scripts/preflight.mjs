import { normalizeWorkerEnv } from "./env.mjs";

const requiredEnv = ["DATABASE_URL", "WHATSAPP_AUTH_ENCRYPTION_KEY"];
for (const key of requiredEnv) {
  if (!process.env[key]) {
    throw new Error(`Missing required env: ${key}`);
  }
}

normalizeWorkerEnv();

const { PrismaClient } = await import("@prisma/client");
const prisma = new PrismaClient();

try {
  const lease = await prisma.notificationWorkerLease.findUnique({
    where: { id: process.env.NOTIFICATIONS_WORKER_LEASE_ID || "notifications-worker" }
  });
  const now = Date.now();
  console.log(JSON.stringify({
    ok: true,
    check: "notification-worker-preflight",
    lease: lease ? {
      ownerLabel: lease.ownerLabel,
      active: lease.expiresAt.getTime() > now,
      heartbeatAt: lease.heartbeatAt.toISOString(),
      expiresAt: lease.expiresAt.toISOString()
    } : null
  }));
} finally {
  await prisma.$disconnect();
}
