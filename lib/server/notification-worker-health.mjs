function toTime(value) {
  if (value instanceof Date) return value.getTime();
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

export function buildNotificationWorkerHealth({
  lease,
  staleProcessingCount = 0,
  now = new Date(),
  leaseTtlMs = 45_000
}) {
  const nowMs = toTime(now);
  const heartbeatAtMs = toTime(lease?.heartbeatAt);
  const expiresAtMs = toTime(lease?.expiresAt);
  const heartbeatAgeMs = Number.isFinite(heartbeatAtMs)
    ? Math.max(0, nowMs - heartbeatAtMs)
    : null;
  const leaseExpiresInMs = Number.isFinite(expiresAtMs)
    ? expiresAtMs - nowMs
    : null;
  const leaderActive = Number.isFinite(nowMs)
    && Number.isFinite(heartbeatAtMs)
    && Number.isFinite(expiresAtMs)
    && expiresAtMs > nowMs
    && heartbeatAgeMs <= leaseTtlMs;
  const normalizedStaleCount = Number.isSafeInteger(staleProcessingCount)
    ? Math.max(0, staleProcessingCount)
    : 0;
  const staleProcessingDetected = normalizedStaleCount > 0;

  return {
    ok: leaderActive && !staleProcessingDetected,
    state: !leaderActive
      ? "leader-missing"
      : staleProcessingDetected
        ? "stale-processing"
        : "leader-active",
    leaderActive,
    heartbeatAgeMs,
    leaseExpiresInMs,
    staleProcessingDetected,
    staleProcessingCount: normalizedStaleCount
  };
}
