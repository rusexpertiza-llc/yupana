const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

const siteConfig = {
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
    locales: [ 'ru' ],
  },

  staticDirectories: [ 'static' ],

  presets: [
    [
      'classic',
      ({
        docs: {
          path: '../website/target/docs',
          /* editUrl: 'https://github.com/rusexpertiza-llc/yupana/edit/master/docs/', */
          sidebarPath: require.resolve('./sidebars.json')
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/customTheme.css')
        }
      })
    ]
  ],

  organizationName: 'rusexpertiza-llc',
  projectName: 'yupana-docs',

  themeConfig:
    ({
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
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),

}

module.exports = siteConfig;
