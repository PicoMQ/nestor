import { defineConfig } from 'vitepress';

const docsSidebar = [
  {
    text: 'Getting started',
    collapsed: false,
    items: [
      { text: 'Introduction', link: '/docs/' },
      { text: 'Quick start', link: '/docs/quick-start' },
    ],
  },
  {
    text: 'Design',
    collapsed: false,
    items: [
      { text: 'Overview', link: '/docs/design/overview' },
      { text: 'Blocks & reads', link: '/docs/design/reads' },
      { text: 'Origin fetches', link: '/docs/design/fetches' },
      { text: 'Consistency', link: '/docs/design/consistency' },
      { text: 'Cache tiers', link: '/docs/design/tiers' },
      { text: 'S3 endpoint', link: '/docs/design/endpoint' },
      { text: 'Cluster', link: '/docs/design/cluster' },
    ],
  },
  {
    text: 'Operations',
    collapsed: true,
    items: [
      { text: 'Configuration', link: '/docs/operations/configuration' },
      { text: 'Deployment', link: '/docs/operations/deployment' },
      { text: 'Metrics', link: '/docs/operations/metrics' },
      { text: 'Tuning', link: '/docs/operations/tuning' },
    ],
  },
  {
    text: 'Library',
    collapsed: true,
    items: [
      { text: 'nestor', link: '/docs/library/nestor' },
      { text: 'nestor-store', link: '/docs/library/store' },
      { text: 'nestor-client', link: '/docs/library/client' },
    ],
  },
  {
    text: 'Community',
    collapsed: true,
    items: [
      { text: 'Contribute', link: '/docs/contribute' },
      { text: 'Discord', link: 'https://discord.gg/qsMy5sSpYX' },
    ],
  },
];

export default defineConfig({
  title: 'Nestor',
  description:
    'Nestor is a read-through block cache for S3-compatible object storage, in RAM and on local disk.',
  base: process.env.BASE_PATH ?? '/',
  cleanUrls: true,
  appearance: false,
  srcDir: 'pages',
  vite: {
    publicDir: 'assets',
    server: {
      fs: {
        allow: ['..'],
      },
    },
  },
  markdown: {
    theme: 'github-light',
  },
  head: [
    ['link', { rel: 'icon', href: '/images/favicon.ico', sizes: 'any' }],
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/images/logo.svg' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    [
      'link',
      { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
    ],
    [
      'link',
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&family=Marcellus&family=Playfair+Display:wght@400;500;600&display=swap',
      },
    ],
  ],
  themeConfig: {
    siteTitle: false,
    nav: [
      { text: 'Docs', link: '/docs' },
      { text: 'Contribute', link: '/docs/contribute' },
      { text: 'Discord', link: 'https://discord.gg/qsMy5sSpYX' },
      { text: 'GitHub', link: 'https://github.com/picomq/nestor' },
    ],
    sidebar: docsSidebar,
    search: {
      provider: 'local',
    },
    outline: {
      level: [1, 3],
      label: 'On this page',
    },
    footer: {
      copyright: '© 2026 Nestor. Apache 2.0.',
    },
  },
});
