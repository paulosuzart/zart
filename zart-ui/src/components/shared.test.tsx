import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { formatTs, statusBadge } from "./shared";

describe("formatTs", () => {
  it("returns '-' for null", () => {
    expect(formatTs(null)).toBe("-");
  });

  it("returns '-' for undefined", () => {
    expect(formatTs(undefined)).toBe("-");
  });

  it("returns a non-empty string for a valid ISO timestamp", () => {
    const result = formatTs("2024-06-15T10:30:00.000Z");
    expect(result).toBeTruthy();
    expect(result).not.toBe("-");
  });
});

describe("statusBadge", () => {
  it("renders a span with the correct status class", () => {
    const { container } = render(<>{statusBadge("completed")}</>);
    const span = container.querySelector("span");
    expect(span).toBeInTheDocument();
    expect(span).toHaveClass("badge-completed");
    expect(span).toHaveTextContent("completed");
  });

  it("renders the correct class for 'failed' status", () => {
    const { container } = render(<>{statusBadge("failed")}</>);
    expect(container.querySelector("span")).toHaveClass("badge-failed");
  });

  it("renders the correct class for 'running' status", () => {
    const { container } = render(<>{statusBadge("running")}</>);
    expect(container.querySelector("span")).toHaveClass("badge-running");
  });
});
