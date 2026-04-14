import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Dashboard } from "./Dashboard";

// Mock the API module so no real HTTP calls are made
vi.mock("../api/client", () => ({
  getStats: vi.fn().mockResolvedValue({
    scheduled: 2,
    running: 1,
    completed: 10,
    failed: 3,
    cancelled: 0,
  }),
  listPauseRules: vi.fn().mockResolvedValue([]),
  listExecutions: vi.fn().mockResolvedValue([]),
}));

// MemoryRouter is not needed here because Dashboard has no Links/NavLinks,
// but BrowserRouter from react-router-dom IS used transitively through shared
// components. Wrap when necessary.

describe("Dashboard", () => {
  it("renders all stat card labels", () => {
    render(<Dashboard />);
    expect(screen.getByText("Scheduled")).toBeInTheDocument();
    expect(screen.getByText("Running")).toBeInTheDocument();
    expect(screen.getByText("Completed")).toBeInTheDocument();
    expect(screen.getByText("Failed")).toBeInTheDocument();
    expect(screen.getByText("Cancelled")).toBeInTheDocument();
    expect(screen.getByText("Active Pause Rules")).toBeInTheDocument();
  });

  it("shows stats returned by the API", async () => {
    render(<Dashboard />);
    // After the polling hook resolves, the counts appear
    await waitFor(() => {
      expect(screen.getByText("1")).toBeInTheDocument(); // running
      expect(screen.getByText("10")).toBeInTheDocument(); // completed
      expect(screen.getByText("3")).toBeInTheDocument(); // failed
    });
  });

  it("shows the recent executions section heading", () => {
    render(<Dashboard />);
    expect(screen.getByText("Recent Executions")).toBeInTheDocument();
  });

  it("shows empty-state message when there are no recent executions", async () => {
    render(<Dashboard />);
    await waitFor(() => {
      expect(screen.getByText("No executions yet")).toBeInTheDocument();
    });
  });
});
