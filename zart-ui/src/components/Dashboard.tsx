import { getStats, listExecutions, listPauseRules } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { statusBadge } from "./shared";

export function Dashboard() {
  const { data: stats } = usePolling(getStats, 15000);
  const { data: pauseRules } = usePolling(listPauseRules, 15000);
  const { data: recent } = usePolling(
    () => listExecutions({ limit: 10, sortOrder: "desc" }),
    15000,
  );

  return (
    <>
      <div className="page-header">
        <h2>Dashboard</h2>
        <p>Overview of your Zart durable execution instance</p>
      </div>

      <div className="stats-grid">
        <StatCard label="Scheduled" value={stats?.scheduled ?? "-"} status="scheduled" />
        <StatCard label="Running" value={stats?.running ?? "-"} status="running" />
        <StatCard label="Completed" value={stats?.completed ?? "-"} status="completed" />
        <StatCard label="Failed" value={stats?.failed ?? "-"} status="failed" />
        <StatCard label="Cancelled" value={stats?.cancelled ?? "-"} status="cancelled" />
        <StatCard label="Active Pause Rules" value={pauseRules?.length ?? "-"} status="paused" />
      </div>

      <div className="page-header">
        <h2>Recent Executions</h2>
      </div>

      <ExecTable executions={recent ?? []} />
    </>
  );
}

function StatCard({ label, value, status }: { label: string; value: string | number; status: string }) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value" style={{ color: `var(--status-${status})` }}>
        {value}
      </div>
    </div>
  );
}

function ExecTable({ executions }: { executions: import("../api/types").ExecutionResponse[] }) {
  if (executions.length === 0) {
    return (
      <div className="empty-state">
        <p>No executions found</p>
      </div>
    );
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Task</th>
            <th>Status</th>
            <th>Scheduled</th>
          </tr>
        </thead>
        <tbody>
          {executions.map((e) => (
            <tr key={e.durableExecutionId}>
              <td>
                <a href={`/executions/${e.durableExecutionId}`} className="mono">
                  {e.durableExecutionId.length > 24
                    ? `${e.durableExecutionId.slice(0, 24)}...`
                    : e.durableExecutionId}
                </a>
              </td>
              <td>{e.name}</td>
              <td>{statusBadge(e.status)}</td>
              <td className="mono">{new Date(e.scheduledAt).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
