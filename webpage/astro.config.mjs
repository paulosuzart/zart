import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import mermaid from "astro-mermaid";

export default defineConfig({
  redirects: {
    "/docs": "/docs/getting-started",
  },
  integrations: [
    mermaid(),
    starlight({
      components: {
        Banner: "./src/components/DevBanner.astro",
      },
      head: [
        {
          tag: "link",
          attrs: {
            rel: "icon",
            type: "image/svg+xml",
            href: "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32' width='32' height='32'%3E%3Crect width='32' height='32' rx='6' fill='%234f46e5'/%3E%3Ctext x='16' y='23' font-family='Georgia,serif' font-size='20' font-weight='700' text-anchor='middle' fill='white'%3EZ%3C/text%3E%3C/svg%3E",
          },
        },
      ],
      title: "Zart",
      description:
        "Durable Execution for Rust — workflows that survive failure",
      logo: {
        light: "./src/assets/logo-light.svg",
        dark: "./src/assets/logo-dark.svg",
        replacesTitle: false,
      },
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/paulosuzart/zart",
        },
        {
          icon: "twitter",
          label: "X",
          href: "https://x.com/RunZart",
        },
      ],
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        // ── Learn ──────────────────────────────────────────────
        {
          label: "Learn",
          items: [
            { label: "About", link: "/docs/about" },
            { label: "Getting Started", link: "/docs/getting-started" },
          ],
        },

        // ── Concepts ───────────────────────────────────────────
        {
          label: "Concepts",
          items: [
            { label: "Durable Execution", link: "/docs/concepts/durable-execution" },
            { label: "Steps", link: "/docs/concepts/steps" },
            { label: "Flow Control", link: "/docs/concepts/flow-control" },
            { label: "Error Handling", link: "/docs/concepts/error-handling" },
            { label: "Timeouts & Cancellation", link: "/docs/concepts/timeouts" },
            { label: "Transactions", link: "/docs/concepts/transactions" },
            { label: "Recurring Durable Executions", link: "/docs/concepts/recurring-durable" },
          ],
        },

        // ── API Reference ──────────────────────────────────────
        {
          label: "API Reference",
          items: [
            { label: "Free Functions", link: "/docs/api/free-functions" },
            { label: "Macros", link: "/docs/api/macros" },
            {
              label: "Execution Management",
              link: "/docs/api/execution-management",
            },
            { label: "Error Types", link: "/docs/api/error-types" },
          ],
        },

        // ── Operations ─────────────────────────────────────────
        {
          label: "Operations",
          autogenerate: { directory: "docs/admin" },
        },

        // ── Examples ───────────────────────────────────────────
        {
          label: "Examples",
          items: [
            { label: "Brewery Finder", link: "/docs/examples/brewery-finder" },
            { label: "Approval Workflow", link: "/docs/examples/approval-workflow" },
            { label: "Parallel Steps", link: "/docs/examples/parallel-steps" },
            { label: "Error Handling", link: "/docs/examples/error-handling" },
            { label: "Durable Loops", link: "/docs/examples/durable-loops" },
            { label: "Sleep", link: "/docs/examples/sleep" },
            { label: "Retry Simulation", link: "/docs/examples/retry-simulation" },
            { label: "Radkit LLM Agent", link: "/docs/examples/radkit-agent" },
          ],
        },

        // ── Deployment ─────────────────────────────────────────
        {
          label: "Deployment",
          items: [
            { label: "Options", link: "/docs/deployment/options" },
            { label: "Observability", link: "/docs/deployment/observability" },
          ],
        },

        // ── Coming Soon sections hidden for now ──
        // Content preserved in: /java-sdk/, /http-api/
      ],
    }),
  ],
});
