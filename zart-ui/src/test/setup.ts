import { cleanup } from "@testing-library/react";
import * as matchers from "@testing-library/jest-dom/matchers";
import { afterEach, expect } from "vitest";

expect.extend(matchers);

const localStorageMock = {
  getItem: vi.fn().mockReturnValue(null),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
  key: vi.fn(),
  length: 0,
} as Storage;
(globalThis as typeof globalThis & { localStorage: typeof localStorageMock }).localStorage = localStorageMock;

afterEach(() => {
  vi.clearAllMocks();
  cleanup();
});
