import { useCallback, useEffect, useRef, useState } from "react";

/**
 * Polls `fn` immediately and then every `intervalMs` milliseconds.
 *
 * Pass a `key` that changes whenever the query changes (e.g. `searchParams.toString()`).
 * When `key` changes the effect re-runs: an immediate fetch fires and the
 * interval timer resets — no waiting for the next tick.
 */
export function usePolling<T>(
  fn: () => Promise<T>,
  intervalMs: number,
  enabled = true,
  key?: string,
) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const fnRef = useRef(fn);
  fnRef.current = fn;

  const refresh = useCallback(async () => {
    try {
      const result = await fnRef.current();
      setData(result);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!enabled) return;
    setLoading(true);
    refresh();
    if (intervalMs <= 0) return;
    const id = setInterval(refresh, intervalMs);
    return () => clearInterval(id);
    // key is intentionally included: when the query changes we want an
    // immediate re-fetch and a fresh interval, not just the next tick.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, enabled, refresh, key]);

  return { data, error, loading, refresh };
}
