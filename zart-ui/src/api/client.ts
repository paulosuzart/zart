import type {
  ExecutionDetailResponse,
  ExecutionResponse,
  ListParams,
  PauseRuleResponse,
  StatsResponse,
} from "./types";

const STORAGE_KEY = "zart_api_base_url";

function getBaseUrl(): string {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored) return stored;
  return import.meta.env.VITE_API_BASE_URL ?? window.location.origin;
}

export function getCurrentBaseUrl(): string {
  return getBaseUrl();
}

export function setBaseUrl(url: string) {
  localStorage.setItem(STORAGE_KEY, url);
}

function qs(params: Record<string, unknown>): string {
  const parts: string[] = [];
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") {
      parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
    }
  }
  return parts.length ? `?${parts.join("&")}` : "";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${getBaseUrl()}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

export function getStats() {
  return request<StatsResponse>("/api/v1/stats");
}

export function listExecutions(params: ListParams = {}) {
  return request<ExecutionResponse[]>(
    `/api/v1/executions${qs(params as Record<string, unknown>)}`,
  );
}

export function getExecution(id: string) {
  return request<ExecutionResponse>(`/api/v1/executions/${encodeURIComponent(id)}`);
}

export function cancelExecution(id: string) {
  return request<void>(`/api/v1/executions/${encodeURIComponent(id)}/cancel`, { method: "POST" });
}

export function getExecutionDetail(id: string, runId?: string) {
  const q = runId ? `?runId=${encodeURIComponent(runId)}` : "";
  return request<ExecutionDetailResponse>(
    `/admin/v1/executions/${encodeURIComponent(id)}/detail${q}`,
  );
}

export function retryStep(id: string, stepName: string, triggeredBy?: string) {
  return request<{ newTaskId: string }>(
    `/admin/v1/executions/${encodeURIComponent(id)}/retry-step`,
    { method: "POST", body: JSON.stringify({ stepName, triggeredBy }) },
  );
}

export function restartExecution(id: string, payload?: unknown, triggeredBy?: string) {
  return request<{ newRunId: string }>(
    `/admin/v1/executions/${encodeURIComponent(id)}/restart`,
    { method: "POST", body: JSON.stringify({ payload, triggeredBy }) },
  );
}

export function rerunSteps(
  id: string,
  rerunSteps: string[],
  preserveSteps: string[],
  triggeredBy?: string,
) {
  return request<{ newRunNumber: number; effectiveRerun: string[] }>(
    `/admin/v1/executions/${encodeURIComponent(id)}/rerun`,
    { method: "POST", body: JSON.stringify({ rerunSteps, preserveSteps, triggeredBy }) },
  );
}

export function offerEvent(executionId: string, eventName: string, payload: unknown) {
  return request<void>(
    `/api/v1/events/${encodeURIComponent(executionId)}/${encodeURIComponent(eventName)}`,
    { method: "POST", body: JSON.stringify(payload) },
  );
}

export function startExecution(body: {
  executionId?: string;
  taskName: string;
  payload: unknown;
}) {
  return request<ExecutionResponse>("/api/v1/executions", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function listPauseRules() {
  return request<PauseRuleResponse[]>("/admin/v1/pause");
}

export function createPauseRule(rule: {
  executionId?: string;
  taskName?: string;
  stepPattern?: string;
  expiresAt?: string;
  triggeredBy?: string;
}) {
  return request<PauseRuleResponse>("/admin/v1/pause", {
    method: "POST",
    body: JSON.stringify(rule),
  });
}

export function deletePauseRule(ruleId: string) {
  return request<void>(`/admin/v1/pause/${encodeURIComponent(ruleId)}`, { method: "DELETE" });
}
