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

        // ── Deploy & Integrate ──────────────────────────────────
        {
          label: 'Deploy & Integrate',
          items: [
            { label: 'Deployment Options',         link: '/deployment/options' },
            { label: 'Using Zart with AI Agents',  link: '/llm-agents/overview' },
          ],
        },

        // ── Coming Soon (collapsed by default) ─────────────────
        {
          label: 'Coming Soon',
          collapsed: true,
          items: [
            {
              label: 'CLI',
              badge: { text: 'Soon', variant: 'caution' },
              link: '/cli/overview',
            },
            {
              label: 'Java SDK',
              badge: { text: 'Planned', variant: 'note' },
              link: '/java-sdk/overview',
            },
            {
              label: 'HTTP API Reference',
              badge: { text: 'Soon', variant: 'caution' },
              link: '/http-api/endpoints',
            },
          ],
        },
      ],
    }),
  ],
});
