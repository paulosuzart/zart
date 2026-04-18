import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import mermaid from "astro-mermaid";

export default defineConfig({
  integrations: [
    mermaid(),
    starlight({
      components: {
        Banner: "./src/components/DevBanner.astro",
        Head: "./src/components/CustomHead.astro",
      },
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
            { label: "About", link: "/about" },
            { label: "Getting Started", link: "/getting-started" },
          ],
        },

        // ── Concepts ───────────────────────────────────────────
        {
          label: "Concepts",
          items: [
            { label: "Durable Execution", link: "/concepts/durable-execution" },
            { label: "Steps", link: "/concepts/steps" },
            { label: "Flow Control", link: "/concepts/flow-control" },
            { label: "Error Handling", link: "/concepts/error-handling" },
            { label: "Timeouts & Cancellation", link: "/concepts/timeouts" },
            { label: "Transactions", link: "/concepts/transactions" },
          ],
        },

        // ── API Reference ──────────────────────────────────────
        {
          label: "API Reference",
          items: [
            { label: "Free Functions", link: "/api/free-functions" },
            { label: "Macros", link: "/api/macros" },
            {
              label: "Execution Management",
              link: "/api/execution-management",
            },
            { label: "Error Types", link: "/api/error-types" },
          ],
        },

        // ── Operations ─────────────────────────────────────────
        {
          label: "Operations",
          autogenerate: { directory: "admin" },
        },

        // ── Examples ───────────────────────────────────────────
        {
          label: "Examples",
          items: [
            { label: "Brewery Finder", link: "/examples/brewery-finder" },
            { label: "Approval Workflow", link: "/examples/approval-workflow" },
            { label: "Parallel Steps", link: "/examples/parallel-steps" },
            { label: "Error Handling", link: "/examples/error-handling" },
            { label: "Durable Loops", link: "/examples/durable-loops" },
            { label: "Sleep", link: "/examples/sleep" },
            { label: "Retry Simulation", link: "/examples/retry-simulation" },
            { label: "Radkit LLM Agent", link: "/examples/radkit-agent" },
          ],
        },

        // ── Deployment ─────────────────────────────────────────
        {
          label: "Deployment",
          items: [
            { label: "Options", link: "/deployment/options" },
            { label: "Observability", link: "/deployment/observability" },
          ],
        },

        // ── Coming Soon sections hidden for now ──
        // Content preserved in: /java-sdk/, /http-api/
      ],
    }),
  ],
});
