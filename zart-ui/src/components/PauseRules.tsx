import { useState } from "react";
import { listPauseRules, createPauseRule, deletePauseRule } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { formatTs } from "./shared";

export function PauseRules() {
  const { data: rules, refresh } = usePolling(listPauseRules, 15000);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({
    executionId: "",
    taskName: "",
    stepPattern: "",
    expiresAt: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    try {
      await createPauseRule({
        executionId: form.executionId || undefined,
        taskName: form.taskName || undefined,
        stepPattern: form.stepPattern || undefined,
        expiresAt: form.expiresAt || undefined,
      });
      setForm({ executionId: "", taskName: "", stepPattern: "", expiresAt: "" });
      setShowForm(false);
      await refresh();
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete(ruleId: string) {
    try {
      await deletePauseRule(ruleId);
      setConfirmDelete(null);
      await refresh();
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    }
  }

  const activeRules = rules?.filter((r) => !r.deletedAt) ?? [];

  return (
    <>
      <div className="page-header">
        <h2>Pause Rules</h2>
        <p>Manage pause/resume rules for execution steps</p>
      </div>

      <button
        className="btn btn-primary"
        style={{ marginBottom: 20 }}
        onClick={() => setShowForm(!showForm)}
      >
        {showForm ? "Cancel" : "New Rule"}
      </button>

      {showForm && (
        <form onSubmit={handleCreate} style={{ marginBottom: 24 }}>
          <div className="filters">
            <input
              placeholder="Execution ID (optional)"
              value={form.executionId}
              onChange={(e) => setForm({ ...form, executionId: e.target.value })}
            />
            <input
              placeholder="Task name (optional)"
              value={form.taskName}
              onChange={(e) => setForm({ ...form, taskName: e.target.value })}
            />
            <input
              placeholder="Step pattern (optional glob)"
              value={form.stepPattern}
              onChange={(e) => setForm({ ...form, stepPattern: e.target.value })}
            />
            <input
              type="datetime-local"
              value={form.expiresAt}
              onChange={(e) => setForm({ ...form, expiresAt: e.target.value })}
              title="Expires at"
            />
            <button className="btn btn-primary" type="submit" disabled={submitting}>
              Create
            </button>
          </div>
        </form>
      )}

      {activeRules.length === 0 ? (
        <div className="empty-state">
          <p>No active pause rules</p>
        </div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Rule ID</th>
                <th>Execution</th>
                <th>Task</th>
                <th>Step Pattern</th>
                <th>Created</th>
                <th>Expires</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {activeRules.map((r) => (
                <tr key={r.ruleId}>
                  <td className="mono">{r.ruleId.slice(0, 20)}...</td>
                  <td className="mono">{r.executionId ?? "*"}</td>
                  <td>{r.taskName ?? "*"}</td>
                  <td className="mono">{r.stepPattern ?? "*"}</td>
                  <td className="mono">{formatTs(r.createdAt)}</td>
                  <td className="mono">{formatTs(r.expiresAt)}</td>
                  <td>
                    <button
                      className="btn btn-sm btn-danger"
                      onClick={() => setConfirmDelete(r.ruleId)}
                    >
                      Resume
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {confirmDelete && (
        <div className="overlay" onClick={() => setConfirmDelete(null)}>
          <div className="dialog" onClick={(e) => e.stopPropagation()}>
            <h3>Resume (delete rule)</h3>
            <p>Remove this pause rule? Steps will resume scheduling.</p>
            <div className="dialog-actions">
              <button className="btn" onClick={() => setConfirmDelete(null)}>
                Cancel
              </button>
              <button className="btn btn-danger" onClick={() => handleDelete(confirmDelete)}>
                Delete
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
