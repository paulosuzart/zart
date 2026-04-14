import { useState } from "react";
import { useParams } from "react-router-dom";
import {
  getExecutionDetail,
  retryStep,
  restartExecution,
  rerunSteps,
  cancelExecution,
  offerEvent,
} from "../api/client";
import type { StepDetailResponse } from "../api/types";
import { usePolling } from "../hooks/usePolling";
import { StepGraph } from "./StepGraph";
import { statusBadge, formatTs } from "./shared";

// ── Confirm dialog ────────────────────────────────────────────────────────────

type ConfirmState = {
  title: string;
  body: React.ReactNode;
  action: () => Promise<void>;
};

// ── Selective rerun dialog ────────────────────────────────────────────────────

function RerunDialog({
  steps,
  onConfirm,
  onClose,
}: {
  steps: StepDetailResponse[];
  onConfirm: (rerun: string[], preserve: string[]) => Promise<void>;
  onClose: () => void;
}) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [acting, setActing] = useState(false);

  function toggle(name: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
  }

  async function submit() {
    const rerun = Array.from(selected);
    const preserve = steps.map((s) => s.name).filter((n) => !selected.has(n));
    setActing(true);
    try {
      await onConfirm(rerun, preserve);
      onClose();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    } finally {
      setActing(false);
    }
  }

  return (
    <div className="overlay" onClick={onClose}>
      <div className="dialog" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 520 }}>
        <h3>Selective rerun</h3>
        <p>Check the steps you want to rerun. Unchecked steps will be preserved.</p>
        <div className="rerun-steps">
          {steps.map((s) => (
            <label key={s.stepId} className="rerun-step-row">
              <input
                type="checkbox"
                checked={selected.has(s.name)}
                onChange={() => toggle(s.name)}
              />
              <span style={{ fontFamily: "var(--font-mono)", fontSize: 12 }}>{s.name}</span>
              {statusBadge(s.status)}
            </label>
          ))}
        </div>
        <div className="dialog-actions">
          <button className="btn" onClick={onClose}>Cancel</button>
          <button
            className="btn btn-primary"
            disabled={acting || selected.size === 0}
            onClick={submit}
          >
            Rerun {selected.size > 0 ? `(${selected.size})` : ""}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Deliver event dialog ──────────────────────────────────────────────────────

function EventDialog({
  executionId,
  onClose,
}: {
  executionId: string;
  onClose: () => void;
}) {
  const [eventName, setEventName] = useState("");
  const [payload, setPayload] = useState("{}");
  const [acting, setActing] = useState(false);
  const [payloadError, setPayloadError] = useState("");

  async function submit() {
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
      setPayloadError("");
    } catch {
      setPayloadError("Invalid JSON");
      return;
    }
    setActing(true);
    try {
      await offerEvent(executionId, eventName, parsed);
      onClose();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    } finally {
      setActing(false);
    }
  }

  return (
    <div className="overlay" onClick={onClose}>
      <div className="dialog" onClick={(e) => e.stopPropagation()}>
        <h3>Deliver event</h3>
        <p>Send an external event to this execution.</p>
        <label>Event name</label>
        <input
          type="text"
          value={eventName}
          onChange={(e) => setEventName(e.target.value)}
          placeholder="e.g. payment-confirmed"
        />
        <label>Payload (JSON)</label>
        <textarea
          value={payload}
          onChange={(e) => setPayload(e.target.value)}
          spellCheck={false}
        />
        {payloadError && <div className="error-msg" style={{ marginBottom: 8 }}>{payloadError}</div>}
        <div className="dialog-actions">
          <button className="btn" onClick={onClose}>Cancel</button>
          <button
            className="btn btn-primary"
            disabled={acting || !eventName.trim()}
            onClick={submit}
          >
            Deliver
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Steps table ───────────────────────────────────────────────────────────────

function StepsTable({
  steps,
  acting,
  onRetry,
}: {
  steps: StepDetailResponse[];
  acting: boolean;
  onRetry: (step: StepDetailResponse) => void;
}) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  function toggleExpand(id: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  }

  if (steps.length === 0) {
    return <div className="empty-state"><p>No steps recorded yet</p></div>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Kind</th>
            <th>Status</th>
            <th>Attempts</th>
            <th>Scheduled</th>
            <th>Completed</th>
            <th>Output / Error</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {steps.map((s) => {
            const isExpanded = expanded.has(s.stepId);
            const hasDetail =
              s.result !== null && s.result !== undefined || s.lastError || s.attempts.length > 1;
            return (
              <>
                <tr key={s.stepId}>
                  <td className="mono">{s.name}</td>
                  <td>{s.kind}</td>
                  <td>{statusBadge(s.status)}</td>
                  <td>{s.retryAttempt + 1}</td>
                  <td className="mono">{formatTs(s.scheduledAt)}</td>
                  <td className="mono">{formatTs(s.completedAt)}</td>
                  <td>
                    {s.lastError ? (
                      <span className="error-msg" title={s.lastError}>
                        {s.lastError.length > 40 ? s.lastError.slice(0, 40) + "…" : s.lastError}
                      </span>
                    ) : s.result !== null && s.result !== undefined ? (
                      <span className="mono" style={{ color: "var(--status-completed)", fontSize: 11 }}>
                        {JSON.stringify(s.result).slice(0, 40)}
                      </span>
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>—</span>
                    )}
                  </td>
                  <td style={{ whiteSpace: "nowrap" }}>
                    {hasDetail && (
                      <button
                        className="btn btn-sm"
                        style={{ marginRight: 6 }}
                        onClick={() => toggleExpand(s.stepId)}
                      >
                        {isExpanded ? "▲" : "▼"}
                      </button>
                    )}
                    {s.retryable && (
                      <button
                        className="btn btn-sm btn-primary"
                        disabled={acting}
                        onClick={() => onRetry(s)}
                      >
                        Retry
                      </button>
                    )}
                  </td>
                </tr>
                {isExpanded && (
                  <tr key={`${s.stepId}-detail`}>
                    <td colSpan={8} style={{ padding: "0 14px 14px" }}>
                      {s.result !== null && s.result !== undefined && (
                        <>
                          <div style={{ fontSize: 11, color: "var(--text-muted)", marginBottom: 4, marginTop: 8 }}>
                            OUTPUT
                          </div>
                          <div className="json-block">{JSON.stringify(s.result, null, 2)}</div>
                        </>
                      )}
                      {s.lastError && (
                        <>
                          <div style={{ fontSize: 11, color: "var(--text-muted)", marginBottom: 4, marginTop: 8 }}>
                            ERROR
                          </div>
                          <div className="json-block" style={{ color: "var(--status-failed)" }}>
                            {s.lastError}
                          </div>
                        </>
                      )}
                      {s.attempts.length > 1 && (
                        <>
                          <div style={{ fontSize: 11, color: "var(--text-muted)", marginBottom: 6, marginTop: 12 }}>
                            ATTEMPTS ({s.attempts.length})
                          </div>
                          <table style={{ fontSize: 12, width: "auto" }}>
                            <thead>
                              <tr>
                                <th>#</th>
                                <th>Status</th>
                                <th>Started</th>
                                <th>Completed</th>
                                <th>Error</th>
                              </tr>
                            </thead>
                            <tbody>
                              {s.attempts.map((a) => (
                                <tr key={a.attemptNumber}>
                                  <td className="mono">{a.attemptNumber}</td>
                                  <td>{statusBadge(a.status)}</td>
                                  <td className="mono">{formatTs(a.startedAt)}</td>
                                  <td className="mono">{formatTs(a.completedAt)}</td>
                                  <td className="error-msg">{a.error ?? "—"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </>
                      )}
                    </td>
                  </tr>
                )}
              </>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

type Tab = "graph" | "steps";

export function ExecutionDetail() {
  const { id } = useParams<{ id: string }>();
  const [tab, setTab] = useState<Tab>("graph");
  const [selectedRunId, setSelectedRunId] = useState<string | undefined>(undefined);
  const [confirm, setConfirm] = useState<ConfirmState | null>(null);
  const [acting, setActing] = useState(false);
  const [showRerun, setShowRerun] = useState(false);
  const [showEvent, setShowEvent] = useState(false);

  const { data: detail, loading, refresh } = usePolling(
    () => getExecutionDetail(id!, selectedRunId),
    5000,
    !!id,
    // Re-fetch immediately when the selected run changes
    selectedRunId ?? "__current__",
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
    return <div className="empty-state"><p>Loading…</p></div>;
  }

  if (!detail) {
    return <div className="empty-state"><p>Execution not found</p></div>;
  }

  const { execution, runs, steps } = detail;
  const isTerminal = ["completed", "failed", "cancelled"].includes(execution.status);
  const hasDeadSteps = steps.some((s) => s.retryable);

  return (
    <>
      <div className="page-header">
        <h2 style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <a href="/executions" style={{ color: "var(--text-muted)", fontSize: 16 }}>&larr;</a>
          {execution.durableExecutionId}
          {statusBadge(execution.status)}
        </h2>
        <p>
          {execution.name} &middot; scheduled {formatTs(execution.scheduledAt)}
        </p>
      </div>

      {/* Action bar */}
      <div className="action-bar">
        {hasDeadSteps && (
          <button
            className="btn btn-primary"
            disabled={acting}
            onClick={() =>
              setConfirm({
                title: "Retry dead steps",
                body: `Retry all dead steps in execution ${execution.durableExecutionId}?`,
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

        <button
          className="btn"
          disabled={acting}
          onClick={() => setShowRerun(true)}
        >
          Selective rerun
        </button>

        {!isTerminal && (
          <button
            className="btn"
            disabled={acting}
            onClick={() => setShowEvent(true)}
          >
            Deliver event
          </button>
        )}

        {isTerminal && (
          <button
            className="btn"
            disabled={acting}
            onClick={() =>
              setConfirm({
                title: "Restart execution",
                body: `Restart ${execution.durableExecutionId} from scratch?`,
                action: async () => { await restartExecution(execution.durableExecutionId); },
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
                body: `Cancel ${execution.durableExecutionId}? This cannot be undone.`,
                action: () => cancelExecution(execution.durableExecutionId),
              })
            }
          >
            Cancel
          </button>
        )}
      </div>

      {/* Payload + Result */}
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

      {/* Run selector + view tabs in one bar */}
      <div className="run-bar">
        <div className="tabs" style={{ borderBottom: "none", marginBottom: 0 }}>
          <button className={tab === "graph" ? "active" : ""} onClick={() => setTab("graph")}>
            Graph
          </button>
          <button className={tab === "steps" ? "active" : ""} onClick={() => setTab("steps")}>
            Steps {steps.length > 0 && `(${steps.length})`}
          </button>
        </div>

        {runs.length > 0 && (
          <div className="run-selector">
            <label>Run</label>
            <select
              value={selectedRunId ?? ""}
              onChange={(e) => setSelectedRunId(e.target.value || undefined)}
            >
              <option value="">Latest (#{runs[runs.length - 1]?.runIndex ?? 0})</option>
              {[...runs].reverse().map((r) => (
                <option key={r.runId} value={r.runId}>
                  #{r.runIndex} — {r.trigger} — {statusBadge(r.status) ? r.status : r.status}
                  {r.completedAt ? ` (${formatTs(r.completedAt)})` : " (running)"}
                </option>
              ))}
            </select>
          </div>
        )}
      </div>
      <div style={{ borderBottom: "1px solid var(--border)", marginBottom: 20 }} />

      {tab === "graph" && <StepGraph steps={steps} />}

      {tab === "steps" && (
        <StepsTable
          steps={steps}
          acting={acting}
          onRetry={(s) =>
            setConfirm({
              title: `Retry step "${s.name}"`,
              body: `Retry dead step "${s.name}"?`,
              action: async () => { await retryStep(execution.durableExecutionId, s.name); },
            })
          }
        />
      )}

      {/* Selective rerun dialog */}
      {showRerun && (
        <RerunDialog
          steps={steps}
          onClose={() => setShowRerun(false)}
          onConfirm={async (rerun, preserve) => {
            await rerunSteps(execution.durableExecutionId, rerun, preserve);
            await refresh();
          }}
        />
      )}

      {/* Deliver event dialog */}
      {showEvent && (
        <EventDialog
          executionId={execution.durableExecutionId}
          onClose={() => { setShowEvent(false); refresh(); }}
        />
      )}

      {/* Generic confirm dialog */}
      {confirm && (
        <div className="overlay" onClick={() => setConfirm(null)}>
          <div className="dialog" onClick={(e) => e.stopPropagation()}>
            <h3>{confirm.title}</h3>
            <p>{confirm.body}</p>
            <div className="dialog-actions">
              <button className="btn" onClick={() => setConfirm(null)}>Cancel</button>
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
