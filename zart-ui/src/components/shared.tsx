export function statusBadge(status: string) {
  const cls = `badge badge-${status}`;
  return <span className={cls}>{status}</span>;
}

export function formatTs(ts: string | null | undefined) {
  if (!ts) return "-";
  return new Date(ts).toLocaleString();
}
