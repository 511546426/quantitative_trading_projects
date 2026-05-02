import { useEffect, useState } from "react";
import { fetchHealth } from "../api/client";

interface HealthState {
  ok: boolean | null;
  serverTime: string | null;
  polledAt: string | null;
}

export function useHealth(intervalMs = 30_000): HealthState {
  const [state, setState] = useState<HealthState>({
    ok: null,
    serverTime: null,
    polledAt: null,
  });

  useEffect(() => {
    let cancelled = false;
    const poll = () => {
      fetchHealth()
        .then((h) => {
          if (cancelled) return;
          setState({ ok: !!h.ok, serverTime: h.server_time_utc ?? null, polledAt: new Date().toISOString() });
        })
        .catch(() => {
          if (cancelled) return;
          setState({ ok: false, serverTime: null, polledAt: new Date().toISOString() });
        });
    };
    poll();
    const id = window.setInterval(poll, intervalMs);
    return () => { cancelled = true; window.clearInterval(id); };
  }, [intervalMs]);

  return state;
}
