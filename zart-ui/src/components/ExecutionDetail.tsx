import { useState } from "react";
import { useParams } from "react-router-dom";
import {
  getExecutionDetail,
  retryStep,
  restartExecution,
  cancelExecution,
} from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { statusBadge, formatTs } from "./shared";

export function ExecutionDetail() {
  const { id } = useParams<{ id: string }>();
  const [confirm, setConfirm] = useState<{
    title: string;
    message: string;
    action: () => Promise<void>;
  } | null>(null);
  const [acting, setActing] = useState(false);

  const { data: detail, loading, refresh } = usePolling(
    () => getExecutionDetail(id!),
    5000,
    !!id,
  );

  async function doAction(action: () => Promise<void>) {
    setActing(true);
    try {
      await action();
      setConfirm(null);
      await refresh();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    } finally {
      setActing(false);
    }
  }

  if (loading && !detail) {
    return (
      <div className="empty-state">
        <p>Loading...</p>
      </div>
    );
  }

  if (!detail) {
    return (
      <div className="empty-state">
        <p>Execution not found</p>
      </div>
    );
  }

  const { execution, runs, steps } = detail;
  const isTerminal = ["completed", "failed", "cancelled"].includes(execution.status);
  const hasDeadSteps = steps.some((s) => s.retryable);

  return (
    <>
      <div className="page-header">
        <h2 style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <a href="/executions" style={{ color: "var(--text-muted)", fontSize: 16 }}>
            &larr;
          </a>
          {execution.durableExecutionId}
          {statusBadge(execution.status)}
        </h2>
        <p>
          {execution.name} &middot; scheduled {formatTs(execution.scheduledAt)}
        </p>
      </div>

      <div className="action-bar">
        {hasDeadSteps && (
          <button
            className="btn btn-primary"
            disabled={acting}
            onClick={() =>
              setConfirm({
                title: "Retry failed steps",
                message: `Retry all dead steps in execution ${execution.durableExecutionId}?`,
                action: async () => {
                  for (const s of steps.filter((s) => s.retryable)) {
                    await retryStep(execution.durableExecutionId, s.name);
                  }
                },
              })
            }
          >
            Retry dead steps
          </button>
        )}
        {isTerminal && (
          <button
            className="btn"
            disabled={acting}
            onClick={() =>
              setConfirm({
                title: "Restart execution",
                message: `Restart ${execution.durableExecutionId} from scratch?`,
                action: () => doAction(async () => {
                  await restartExecution(execution.durableExecutionId);
                }),
              })
            }
          >
            Restart
          </button>
        )}
        {!isTerminal && (
          <button
            className="btn btn-danger"
            disabled={acting}
            onClick={() =>
              setConfirm({
                title: "Cancel execution",
                message: `Cancel ${execution.durableExecutionId}? This cannot be undone.`,
                action: () => doAction(async () => {
                  await cancelExecution(execution.durableExecutionId);
                }),
              })
            }
          >
            Cancel
          </button>
        )}
      </div>

      <div className="detail-grid">
        <div className="detail-section">
          <h3>Payload</h3>
          <div className="json-block">{JSON.stringify(execution.payload, null, 2)}</div>
        </div>
        <div className="detail-section">
          <h3>Result</h3>
          <div className="json-block">
            {execution.result ? JSON.stringify(execution.result, null, 2) : "No result yet"}
          </div>
        </div>
      </div>

      <div className="page-header">
        <h2>Steps</h2>
      </div>

      {steps.length === 0 ? (
        <div className="empty-state">
          <p>No steps recorded yet</p>
        </div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Kind</th>
                <th>Status</th>
                <th>Retry #</th>
                <th>Error</th>
                <th>Scheduled</th>
                <th>Completed</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {steps.map((s) => (
                <tr key={s.stepId}>
                  <td className="mono">{s.name}</td>
                  <td>{s.kind}</td>
                  <td>{statusBadge(s.status)}</td>
                  <td>{s.retryAttempt}</td>
                  <td className="error-msg">{s.lastError ?? "-"}</td>
                  <td className="mono">{formatTs(s.scheduledAt)}</td>
                  <td className="mono">{formatTs(s.completedAt)}</td>
                  <td>
                    {s.retryable && (
                      <button
                        className="btn btn-sm btn-primary"
                        disabled={acting}
                        onClick={() =>
                          setConfirm({
                            title: `Retry step "${s.name}"`,
                            message: `Retry dead step ${s.name}?`,
                            action: () => doAction(async () => {
                              await retryStep(execution.durableExecutionId, s.name);
                            }),
                          })
                        }
                      >
                        Retry
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {runs.length > 1 && (
        <>
          <div className="page-header" style={{ marginTop: 32 }}>
            <h2>Run History</h2>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Trigger</th>
                  <th>Status</th>
                  <th>Started</th>
                  <th>Completed</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((r) => (
                  <tr key={r.runId}>
                    <td className="mono">#{r.runIndex}</td>
                    <td>{r.trigger}</td>
                    <td>{statusBadge(r.status)}</td>
                    <td className="mono">{formatTs(r.startedAt)}</td>
                    <td className="mono">{formatTs(r.completedAt)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {confirm && (
        <div className="overlay" onClick={() => setConfirm(null)}>
          <div className="dialog" onClick={(e) => e.stopPropagation()}>
            <h3>{confirm.title}</h3>
            <p>{confirm.message}</p>
            <div className="dialog-actions">
              <button className="btn" onClick={() => setConfirm(null)}>
                Cancel
              </button>
              <button
                className="btn btn-primary"
                disabled={acting}
                onClick={() => doAction(confirm.action)}
              >
                Confirm
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
