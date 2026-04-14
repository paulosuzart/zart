import { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { listExecutions } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { statusBadge } from "./shared";
import { StartExecutionDialog } from "./StartExecutionDialog";

// ── Execution list ────────────────────────────────────────────────────────────

export function ExecutionList() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [status, setStatus] = useState(searchParams.get("status") ?? "");
  const [search, setSearch] = useState(searchParams.get("search") ?? "");
  const [taskName, setTaskName] = useState(searchParams.get("taskName") ?? "");
  const [sortBy, setSortBy] = useState(searchParams.get("sortBy") ?? "scheduled_at");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">(
    (searchParams.get("sortOrder") as "asc" | "desc") ?? "desc",
  );
  const [showStart, setShowStart] = useState(false);
  const page = parseInt(searchParams.get("page") ?? "1", 10);
  const limit = 25;

  const { data: executions, loading, refresh } = usePolling(
    () =>
      listExecutions({
        status: status || undefined,
        taskName: taskName || undefined,
        search: search || undefined,
        limit,
        offset: (page - 1) * limit,
        sortBy,
        sortOrder,
      }),
    15000,
    true,
    searchParams.toString(),
  );

  function apply(newParams: Record<string, string>) {
    const next = new URLSearchParams();
    for (const [k, v] of Object.entries(newParams)) {
      if (v) next.set(k, v);
    }
    setSearchParams(next);
  }

  function applySort(newSortBy: string, newSortOrder: "asc" | "desc") {
    setSortBy(newSortBy);
    setSortOrder(newSortOrder);
    apply({ status, search, taskName, sortBy: newSortBy, sortOrder: newSortOrder, page: "1" });
  }

  return (
    <>
      <div className="page-header">
        <h2>Executions</h2>
        <p>Filterable list of all durable executions</p>
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16, flexWrap: "wrap", gap: 12 }}>
        <div className="filters" style={{ marginBottom: 0 }}>
          <select
            value={status}
            onChange={(e) => {
              setStatus(e.target.value);
              apply({ status: e.target.value, search, taskName, sortBy, sortOrder, page: "1" });
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
              if (e.key === "Enter") apply({ status, search, taskName, sortBy, sortOrder, page: "1" });
            }}
          />
          <input
            placeholder="Search ID…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") apply({ status, search, taskName, sortBy, sortOrder, page: "1" });
            }}
          />
          <button
            className="btn btn-sm"
            onClick={() => apply({ status, search, taskName, sortBy, sortOrder, page: "1" })}
          >
            Filter
          </button>

          <div style={{ width: 1, background: "var(--border)", alignSelf: "stretch" }} />

          <select
            value={sortBy}
            onChange={(e) => applySort(e.target.value, sortOrder)}
          >
            <option value="scheduled_at">Scheduled</option>
            <option value="status">Status</option>
            <option value="task_name">Task name</option>
          </select>
          <button
            className="btn btn-sm"
            title={sortOrder === "desc" ? "Descending — click for ascending" : "Ascending — click for descending"}
            onClick={() => applySort(sortBy, sortOrder === "desc" ? "asc" : "desc")}
          >
            {sortOrder === "desc" ? "↓" : "↑"}
          </button>
        </div>

        <button className="btn btn-primary" onClick={() => setShowStart(true)}>
          + Start execution
        </button>
      </div>

      {loading && !executions ? (
        <div className="empty-state"><p>Loading executions...</p></div>
      ) : executions && executions.length === 0 ? (
        <div className="empty-state">
          <p>No executions match your filters</p>
          <p className="empty-state-hint">Try adjusting your search or status filter</p>
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
                        ? `${e.durableExecutionId.slice(0, 32)}…`
                        : e.durableExecutionId}
                    </a>
                  </td>
                  <td>{e.name}</td>
                  <td>{statusBadge(e.status)}</td>
                  <td className="mono">{new Date(e.scheduledAt).toLocaleString()}</td>
                  <td className="mono">
                    {e.completedAt ? new Date(e.completedAt).toLocaleString() : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="pagination">
            <span>Page {page} ({limit} per page)</span>
            <div style={{ display: "flex", gap: 8 }}>
              <button
                className="btn btn-sm"
                disabled={page <= 1}
                onClick={() => apply({ status, search, taskName, sortBy, sortOrder, page: String(page - 1) })}
              >
                Prev
              </button>
              <button
                className="btn btn-sm"
                disabled={(executions?.length ?? 0) < limit}
                onClick={() => apply({ status, search, taskName, sortBy, sortOrder, page: String(page + 1) })}
              >
                Next
              </button>
            </div>
          </div>
        </div>
      )}

      {showStart && (
        <StartExecutionDialog
          onClose={() => { setShowStart(false); refresh(); }}
        />
      )}
    </>
  );
}
