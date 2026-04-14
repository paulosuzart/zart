import { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { listExecutions } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { statusBadge } from "./shared";

export function ExecutionList() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [status, setStatus] = useState(searchParams.get("status") ?? "");
  const [search, setSearch] = useState(searchParams.get("search") ?? "");
  const [taskName, setTaskName] = useState(searchParams.get("taskName") ?? "");
  const page = parseInt(searchParams.get("page") ?? "1", 10);
  const limit = 25;

  const { data: executions, loading } = usePolling(
    () =>
      listExecutions({
        status: status || undefined,
        taskName: taskName || undefined,
        search: search || undefined,
        limit,
        offset: (page - 1) * limit,
        sortOrder: "desc",
      }),
    15000,
  );

  function apply(newParams: Record<string, string>) {
    const next = new URLSearchParams();
    for (const [k, v] of Object.entries(newParams)) {
      if (v) next.set(k, v);
    }
    setSearchParams(next);
  }

  return (
    <>
      <div className="page-header">
        <h2>Executions</h2>
        <p>Filterable list of all durable executions</p>
      </div>

      <div className="filters">
        <select
          value={status}
          onChange={(e) => {
            setStatus(e.target.value);
            apply({ status: e.target.value, search, taskName, page: "1" });
          }}
        >
          <option value="">All statuses</option>
          <option value="scheduled">Scheduled</option>
          <option value="running">Running</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
          <option value="cancelled">Cancelled</option>
        </select>
        <input
          placeholder="Task name"
          value={taskName}
          onChange={(e) => setTaskName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") apply({ status, search, taskName, page: "1" });
          }}
        />
        <input
          placeholder="Search ID..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") apply({ status, search, taskName, page: "1" });
          }}
        />
        <button
          className="btn btn-sm"
          onClick={() => apply({ status, search, taskName, page: "1" })}
        >
          Filter
        </button>
      </div>

      {loading && !executions ? (
        <div className="empty-state">
          <p>Loading...</p>
        </div>
      ) : executions && executions.length === 0 ? (
        <div className="empty-state">
          <p>No executions match your filters</p>
        </div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Task</th>
                <th>Status</th>
                <th>Scheduled</th>
                <th>Completed</th>
              </tr>
            </thead>
            <tbody>
              {executions?.map((e) => (
                <tr key={e.durableExecutionId}>
                  <td>
                    <a href={`/executions/${e.durableExecutionId}`} className="mono">
                      {e.durableExecutionId.length > 32
                        ? `${e.durableExecutionId.slice(0, 32)}...`
                        : e.durableExecutionId}
                    </a>
                  </td>
                  <td>{e.name}</td>
                  <td>{statusBadge(e.status)}</td>
                  <td className="mono">{new Date(e.scheduledAt).toLocaleString()}</td>
                  <td className="mono">
                    {e.completedAt ? new Date(e.completedAt).toLocaleString() : "-"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="pagination">
            <span>
              Page {page} ({limit} per page)
            </span>
            <div style={{ display: "flex", gap: 8 }}>
              <button
                className="btn btn-sm"
                disabled={page <= 1}
                onClick={() => apply({ status, search, taskName, page: String(page - 1) })}
              >
                Prev
              </button>
              <button
                className="btn btn-sm"
                disabled={(executions?.length ?? 0) < limit}
                onClick={() => apply({ status, search, taskName, page: String(page + 1) })}
              >
                Next
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
