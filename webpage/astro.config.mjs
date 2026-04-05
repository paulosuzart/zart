import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  integrations: [
    starlight({
      title: 'Zart',
      description: 'Durable Execution for Rust — workflows that survive failure',
      logo: {
        light: './src/assets/logo-light.svg',
        dark:  './src/assets/logo-dark.svg',
        replacesTitle: false,
      },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/your-org/zart' },
      ],
      customCss: ['./src/styles/custom.css'],
      sidebar: [
        // ── Learn ──────────────────────────────────────────────
        {
          label: 'Learn',
          items: [
            { label: 'Getting Started', link: '/getting-started' },
            { label: 'Features',        link: '/features' },
          ],
        },

        // ── Reference ──────────────────────────────────────────
        {
          label: 'Rust API',
          items: [
            { label: 'Overview',                  link: '/rust-api/overview' },
            { label: 'TaskHandler Trait',          link: '/rust-api/task-handler' },
            { label: 'Macros',                     link: '/rust-api/macros' },
            { label: 'Durable Loops',              link: '/rust-api/loops' },
            { label: 'Parallel Steps (wait_all)',  link: '/rust-api/parallel-steps' },
            { label: 'Wait for Event',             link: '/rust-api/wait-for-event' },
          ],
        },

        // ── Examples ───────────────────────────────────────────
        {
          label: 'Examples',
          items: [
            { label: 'Brewery Finder',    link: '/examples/brewery-finder' },
            { label: 'Approval Workflow', link: '/examples/approval-workflow' },
            { label: 'Parallel Steps',    link: '/examples/parallel-steps' },
          ],
        },

        // ── Deploy & Integrate and Coming Soon sections hidden for now ──
        // Content preserved in: /deployment/, /llm-agents/, /cli/, /java-sdk/, /http-api/
      ],
    }),
  ],
});
