import { cleanup } from "@testing-library/react";
import * as matchers from "@testing-library/jest-dom/matchers";
import { afterEach, expect } from "vitest";

expect.extend(matchers);

// Mock localStorage for API client tests (jsdom doesn't have it)
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
  key: vi.fn(),
  length: 0,
};
Object.defineProperty(global, "localStorage", { value: localStorageMock });

afterEach(() => {
  vi.clearAllMocks();
  cleanup();
});
