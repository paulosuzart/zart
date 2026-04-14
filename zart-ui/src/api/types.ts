export interface ExecutionResponse {
  name: string;
  durableExecutionId: string;
  payload: unknown;
  status: "scheduled" | "running" | "completed" | "failed" | "cancelled";
  scheduledAt: string;
  completedAt: string | null;
  version: number;
  result: unknown;
}

export interface StatsResponse {
  scheduled: number;
  running: number;
  completed: number;
  failed: number;
  cancelled: number;
}

export interface RunRecordResponse {
  runId: string;
  executionId: string;
  runIndex: number;
  payload: unknown;
  status: string;
  result: unknown;
  startedAt: string;
  completedAt: string | null;
  trigger: string;
}

export interface StepAttemptResponse {
  attemptNumber: number;
  status: string;
  result: unknown;
  error: string | null;
  startedAt: string;
  completedAt: string | null;
}

export interface StepDetailResponse {
  stepId: string;
  name: string;
  kind: string;
  status: string;
  retryAttempt: number;
  result: unknown;
  lastError: string | null;
  retryable: boolean;
  scheduledAt: string;
  completedAt: string | null;
  attempts: StepAttemptResponse[];
}

export interface ExecutionDetailResponse {
  execution: ExecutionResponse;
  runs: RunRecordResponse[];
  steps: StepDetailResponse[];
}

export interface PauseRuleResponse {
  ruleId: string;
  executionId: string | null;
  taskName: string | null;
  stepPattern: string | null;
  createdAt: string;
  expiresAt: string | null;
  createdBy: string | null;
  deletedAt: string | null;
}

export interface ListParams {
  status?: string;
  taskName?: string;
  from?: string;
  to?: string;
  search?: string;
  sortBy?: string;
  sortOrder?: "asc" | "desc";
  limit?: number;
  offset?: number;
}
