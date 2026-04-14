import { useState } from "react";
import type { StepDetailResponse } from "../api/types";

// ── Helpers ───────────────────────────────────────────────────────────────────

function elapsed(step: StepDetailResponse): string {
  if (!step.scheduledAt || !step.completedAt) return "";
  const ms =
    new Date(step.completedAt).getTime() -
    new Date(step.scheduledAt).getTime();
  return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`;
}

function statusIcon(status: string): string {
  switch (status) {
    case "completed":  return "✓";
    case "failed":
    case "dead":       return "✗";
    case "running":    return "●";
    case "cancelled":  return "–";
    default:           return "○";
  }
}

function fmt(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString();
}

// ── Side panel ────────────────────────────────────────────────────────────────

function StepPanel({
  step,
  onClose,
}: {
  step: StepDetailResponse;
  onClose: () => void;
}) {
  return (
    <div className="step-panel">
      <div className="step-panel__header">
        <span className="step-panel__name">{step.name}</span>
        <button className="step-panel__close" onClick={onClose}>✕</button>
      </div>

      <div className="step-panel__row">
        <span className="step-panel__label">Status</span>
        <span className={`badge badge-${step.status}`}>
          {statusIcon(step.status)} {step.status}
        </span>
      </div>

      <div className="step-panel__row">
        <span className="step-panel__label">Kind</span>
        <span>{step.kind}</span>
      </div>

      <div className="step-panel__row">
        <span className="step-panel__label">Scheduled</span>
        <span className="mono">{fmt(step.scheduledAt)}</span>
      </div>

      <div className="step-panel__row">
        <span className="step-panel__label">Completed</span>
        <span className="mono">{fmt(step.completedAt)}</span>
      </div>

      {elapsed(step) && (
        <div className="step-panel__row">
          <span className="step-panel__label">Duration</span>
          <span className="mono">{elapsed(step)}</span>
        </div>
      )}

      <div className="step-panel__row">
        <span className="step-panel__label">Attempts</span>
        <span>{step.attempts.length}</span>
      </div>

      {step.lastError && (
        <div className="step-panel__section">
          <div className="step-panel__section-title">Error</div>
          <div className="step-panel__code step-panel__code--error">
            {step.lastError}
          </div>
        </div>
      )}

      {step.result !== null && step.result !== undefined && (
        <div className="step-panel__section">
          <div className="step-panel__section-title">Output</div>
          <pre className="step-panel__code">
            {JSON.stringify(step.result, null, 2)}
          </pre>
        </div>
      )}

      {step.attempts.length > 1 && (
        <div className="step-panel__section">
          <div className="step-panel__section-title">
            Attempts ({step.attempts.length})
          </div>
          {step.attempts.map((a) => (
            <div key={a.attemptNumber} className="step-panel__attempt">
              <div className="step-panel__row">
                <span className="step-panel__label">#{a.attemptNumber}</span>
                <span className={`badge badge-${a.status}`}>{a.status}</span>
              </div>
              <div className="step-panel__row">
                <span className="step-panel__label">Started</span>
                <span className="mono">{fmt(a.startedAt)}</span>
              </div>
              <div className="step-panel__row">
                <span className="step-panel__label">Completed</span>
                <span className="mono">{fmt(a.completedAt)}</span>
              </div>
              {a.error && (
                <div className="step-panel__code step-panel__code--error" style={{ marginTop: 4 }}>
                  {a.error}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Graph ─────────────────────────────────────────────────────────────────────

export function StepGraph({ steps }: { steps: StepDetailResponse[] }) {
  const [selected, setSelected] = useState<StepDetailResponse | null>(null);

  if (steps.length === 0) {
    return (
      <div className="empty-state">
        <p>No steps recorded yet</p>
        <p className="empty-state-hint">The graph will render as steps execute</p>
      </div>
    );
  }

  // Build columns: one column per unique name, preserving first-appearance order.
  // Loops produce multiple nodes in the same column.
  const seenNames: string[] = [];
  for (const s of steps) {
    if (!seenNames.includes(s.name)) seenNames.push(s.name);
  }
  const columns: StepDetailResponse[][] = seenNames.map((name) =>
    steps.filter((s) => s.name === name),
  );

  return (
    <div className="step-graph-wrap">
      <div className="step-graph">
        {columns.map((col, ci) => (
          <div key={col[0].name} className="step-graph-item">
            {/* Column — all nodes with this step name stacked vertically */}
            <div className="step-column">
              {col.map((step) => (
                <button
                  key={step.stepId}
                  className={`step-node step-node--${step.status}${selected?.stepId === step.stepId ? " step-node--selected" : ""}`}
                  onClick={() =>
                    setSelected((prev) =>
                      prev?.stepId === step.stepId ? null : step,
                    )
                  }
                >
                  <div className="step-node__name">{step.name}</div>
                  <div className="step-node__status">
                    <span className={`badge badge-${step.status}`}>
                      {statusIcon(step.status)} {step.status}
                    </span>
                  </div>
                  {elapsed(step) && (
                    <div className="step-node__meta">{elapsed(step)}</div>
                  )}
                  {step.attempts.length > 1 && (
                    <div className="step-node__meta">
                      {step.attempts.length} attempts
                    </div>
                  )}
                  {step.lastError && (
                    <div className="step-node__error" title={step.lastError}>
                      {step.lastError.length > 50
                        ? step.lastError.slice(0, 50) + "…"
                        : step.lastError}
                    </div>
                  )}
                </button>
              ))}
            </div>

            {/* Connector arrow to the next column */}
            {ci < columns.length - 1 && (
              <div className="step-connector">
                <div className="step-connector__line" />
                <div className="step-connector__arrow" />
              </div>
            )}
          </div>
        ))}
      </div>

      {selected && (
        <StepPanel step={selected} onClose={() => setSelected(null)} />
      )}
    </div>
  );
}
