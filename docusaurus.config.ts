import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'NutsDB Documents',
  tagline: 'NutsDB are cool',
  favicon: 'img/icon.png',

  // Set the production url of your site here
  url: 'https://nutsdb.github.io/',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/nutsdb/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'NutsDB', // Usually your GitHub org/user name.
  projectName: 'NutsDB', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'zh',
    locales: ['zh', 'en'],
    path: 'i18n',
    localeConfigs: {
      zh: {
        label: 'Chinese',
        direction: 'ltr',
        htmlLang: 'en-US',
        calendar: 'gregory',
        path: 'zh',
      },
      en: {
        label: 'English',
        direction: 'ltr',
        htmlLang: 'en-US',
        calendar: 'gregory',
        path: 'en',
      }
    },

  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      require.resolve("@cmfcmf/docusaurus-search-local"),
      {
        language: ['zh', 'en']
      }
    ]
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/nutsdb.png',
    navbar: {
      title: 'NutsDB Documents',
      logo: {
        alt: 'NutsDB Logo',
        src: 'img/nutsdb_light.png',
        srcDark: 'img/nutsdb_dark.png',
        style: {
          "border-radius": "50%",
          border: 'solid 3px #f0b843'
        }
      },
      items: [
        {
          type: 'dropdown',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Tutorial',
          to: '/docs/overview',
          items: [
            {
              to: '/docs/overview',
              label: 'Overview',
            },
            {
              to: '/docs/quick_start',
              label: 'Quick Start',
            },
            {
              to: '/docs/tutorial/overview',
              label: 'Tutorials',
            },
          ]
        },
        {
            to: '/blog',
            label: 'Blog', 
            position: 'left'
        },
        {
          to: '/about',
          label: 'About',
          position: 'left'
        },
        {
          to: '/community',
          label: 'Community',
          position: 'left'
        },
        {
          to: '/solutions',
          label: 'Solutions',
          position: 'left'
        },
        {
          type: 'localeDropdown',
          position: 'right',
        },
        {
          href: 'https://github.com/nutsdb/nutsdb',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Tutorial',
              to: '/docs/quick_start',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Add NutsDB WeChat Group',
              href: 'https://github.com/nutsdb/nutsdb/issues/116',
            }
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: '/blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/facebook/docusaurus',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} NutsDB. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
