import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { startExecution } from "../api/client";

interface Props {
  onClose: () => void;
  /** Pre-fill values when launching from an existing execution. */
  initial?: {
    taskName: string;
    executionId: string;
    payload: unknown;
  };
}

export function StartExecutionDialog({ onClose, initial }: Props) {
  const navigate = useNavigate();
  const [taskName, setTaskName] = useState(initial?.taskName ?? "");
  const [execId, setExecId] = useState(initial?.executionId ?? "");
  const [payload, setPayload] = useState(
    initial?.payload !== undefined
      ? JSON.stringify(initial.payload, null, 2)
      : "{}",
  );
  const [payloadError, setPayloadError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
      setPayloadError("");
    } catch {
      setPayloadError("Invalid JSON");
      return;
    }
    setSubmitting(true);
    try {
      const result = await startExecution({
        executionId: execId.trim() || undefined,
        taskName: taskName.trim(),
        payload: parsed,
      });
      onClose();
      navigate(`/executions/${result.durableExecutionId}`);
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="overlay" onClick={onClose}>
      <div
        className="dialog"
        onClick={(e) => e.stopPropagation()}
        style={{ maxWidth: 720, width: "90vw" }}
      >
        <h3>Start new execution</h3>
        <p>Trigger a durable execution for any registered task.</p>
        <form onSubmit={handleSubmit}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 16px" }}>
            <div>
              <label>Task name *</label>
              <input
                type="text"
                value={taskName}
                onChange={(e) => setTaskName(e.target.value)}
                placeholder="e.g. ui_demo::OrderProcessingTask"
                required
              />
            </div>
            <div>
              <label>Execution ID (optional — generated if blank)</label>
              <input
                type="text"
                value={execId}
                onChange={(e) => setExecId(e.target.value)}
                placeholder="my-execution-id"
              />
            </div>
          </div>
          <label>Payload (JSON)</label>
          <textarea
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
            spellCheck={false}
            style={{ minHeight: 240 }}
          />
          {payloadError && (
            <div className="error-msg" style={{ marginBottom: 8 }}>
              {payloadError}
            </div>
          )}
          <div className="dialog-actions">
            <button type="button" className="btn" onClick={onClose}>
              Cancel
            </button>
            <button
              type="submit"
              className="btn btn-primary"
              disabled={submitting || !taskName.trim()}
            >
              Start
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
