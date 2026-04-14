import { beforeEach, describe, expect, it, vi } from "vitest";
import { cancelExecution, getExecution, getStats, listExecutions, startExecution } from "./client";

const mockFetch = vi.fn();
global.fetch = mockFetch;

function okJson(body: unknown, status = 200) {
  return Promise.resolve({
    ok: true,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  } as Response);
}

beforeEach(() => {
  mockFetch.mockReset();
  localStorage.getItem?.mockReturnValue(null);
});

describe("getStats", () => {
  it("calls GET /api/v1/stats", async () => {
    mockFetch.mockReturnValueOnce(okJson({ scheduled: 0, running: 1, completed: 5, failed: 0, cancelled: 0 }));
    await getStats();
    expect(mockFetch).toHaveBeenCalledOnce();
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("/api/v1/stats");
  });
});

describe("listExecutions", () => {
  it("calls GET /api/v1/executions with no query string when no params", async () => {
    mockFetch.mockReturnValueOnce(okJson([]));
    await listExecutions();
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("/api/v1/executions");
    expect(url).not.toContain("?");
  });

  it("appends status param", async () => {
    mockFetch.mockReturnValueOnce(okJson([]));
    await listExecutions({ status: "failed" });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("status=failed");
  });

  it("appends taskName param", async () => {
    mockFetch.mockReturnValueOnce(okJson([]));
    await listExecutions({ taskName: "OrderTask" });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("taskName=OrderTask");
  });

  it("appends limit and offset", async () => {
    mockFetch.mockReturnValueOnce(okJson([]));
    await listExecutions({ limit: 25, offset: 50 });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("limit=25");
    expect(url).toContain("offset=50");
  });

  it("omits undefined params from the query string", async () => {
    mockFetch.mockReturnValueOnce(okJson([]));
    await listExecutions({ status: undefined, limit: 10 });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).not.toContain("status=");
    expect(url).toContain("limit=10");
  });

  it("appends sortBy and sortOrder", async () => {
    mockFetch.mockReturnValueOnce(okJson([]));
    await listExecutions({ sortBy: "status", sortOrder: "asc" });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("sortBy=status");
    expect(url).toContain("sortOrder=asc");
  });
});

describe("getExecution", () => {
  it("calls GET /api/v1/executions/:id with encoded id", async () => {
    mockFetch.mockReturnValueOnce(okJson({ durableExecutionId: "abc/123" }));
    await getExecution("abc/123");
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("/api/v1/executions/abc%2F123");
  });
});

describe("cancelExecution", () => {
  it("calls POST /api/v1/executions/:id/cancel", async () => {
    mockFetch.mockReturnValueOnce(Promise.resolve({ ok: true, status: 204, json: vi.fn(), text: vi.fn() } as unknown as Response));
    await cancelExecution("exec-1");
    const [url, init] = mockFetch.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/executions/exec-1/cancel");
    expect(init.method).toBe("POST");
  });
});

describe("startExecution", () => {
  it("calls POST /api/v1/executions with the correct body", async () => {
    mockFetch.mockReturnValueOnce(okJson({ durableExecutionId: "new-id" }, 201));
    await startExecution({ taskName: "MyTask", payload: { foo: "bar" } });
    const [url, init] = mockFetch.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/executions");
    expect(init.method).toBe("POST");
    const body = JSON.parse(init.body as string);
    expect(body.taskName).toBe("MyTask");
    expect(body.payload).toEqual({ foo: "bar" });
  });
});
