import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import mermaid from "astro-mermaid";

export default defineConfig({
  integrations: [
    mermaid(),
    starlight({
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
          href: "https://github.com/your-org/zart",
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
            { label: "Features", link: "/features" },
          ],
        },

        // ── Reference ──────────────────────────────────────────
        {
          label: "Rust API",
          items: [
            { label: "Overview", link: "/rust-api/overview" },
            { label: "DurableExecution Trait", link: "/rust-api/task-handler" },
            { label: "ZartStep Trait", link: "/rust-api/zart-step" },
            { label: "Macros", link: "/rust-api/macros" },
            { label: "Durable Loops", link: "/rust-api/loops" },
            {
              label: "Parallel Steps (wait_all)",
              link: "/rust-api/parallel-steps",
            },
            { label: "Wait for Event", link: "/rust-api/wait-for-event" },
          ],
        },

        // ── Examples ───────────────────────────────────────────
        {
          label: "Examples",
          items: [
            { label: "Radkit LLM Agent SDK", link: "/examples/radkit-agent" },
            { label: "Brewery Finder", link: "/examples/brewery-finder" },
            { label: "Approval Workflow", link: "/examples/approval-workflow" },
            { label: "Parallel Steps", link: "/examples/parallel-steps" },
          ],
        },

        // ── Deploy & Integrate and Coming Soon sections hidden for now ──
        // Content preserved in: /deployment/, /java-sdk/, /http-api/
      ],
    }),
  ],
});
