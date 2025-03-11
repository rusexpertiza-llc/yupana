import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import smartyPants from 'remark-smartypants';
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';

const config: Config = {
  title: 'Yupana', // Title for your website.
  tagline: 'Yupana — система анализа розничных продаж',
  url: 'https://docs.yupana.org', // Your website URL
  baseUrl: '/', // Base URL for your project */
  favicon: 'img/favicon.ico',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  customFields: {
    repoUrl: 'https://github.com/rusexpertiza-llc/yupana',
    cname: 'docs.yupana.org',
  },

  i18n: {
    defaultLocale: 'ru',
    locales: ['ru'],
  },

  staticDirectories: ['static'],

  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          path: '../website/target/docs',
          /* editUrl: 'https://github.com/rusexpertiza-llc/yupana/edit/master/docs/', */
          sidebarPath: require.resolve('./sidebars.json'),
          remarkPlugins: [remarkMath],
          rehypePlugins: [rehypeKatex]
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/customTheme.css')
        }
      }
    ]
  ],

  stylesheets: [
    {
      href: 'https://cdn.jsdelivr.net/npm/katex@0.13.24/dist/katex.min.css',
      type: 'text/css',
      integrity: 'sha384-odtC+0UGzzFL/6PNoE8rX/SPcQDXBJ+uRepguP4QkPCm2LBxH3FA3y+fKSiJ+AmM',
      crossorigin: 'anonymous',
    }
  ],

  organizationName: 'rusexpertiza-llc',
  projectName: 'yupana-docs',

  themeConfig: {
    navbar: {
      title: 'Yupana',
      logo: {
        alt: 'Yupana logo',
        src: 'img/yupana-logo.png'
      },
      items: [
        {
          type: 'doc',
          docId: 'architecture',
          position: 'right',
          label: 'Документация'
        },
        {
          href: 'https://github.com/rusexpertiza-llc/yupana',
          label: 'GitHub',
          position: 'right'
        },
        {
          href: 'pathname:///api/org/yupana/index.html',
          label: 'API',
          position: 'right'
        }
      ]
    },

    footer: {
      style: 'dark',
      copyright: `Copyright © Yupana, Первый ОФД ${new Date().getFullYear()}`
    },

    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
  future: {
    experimental_faster: true
  }
};

export default config;
