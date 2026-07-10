import { fileURLToPath, URL } from 'node:url';
import { defineConfig } from 'vitepress';

const repository = 'andrewmunsell/tinybase-supabase';

export default defineConfig({
	base: process.env.GITHUB_ACTIONS ? '/tinybase-supabase/' : '/',
	cleanUrls: true,
	description: 'Browser-only offline persistence and Supabase synchronization for TinyBase.',
	head: [
		['meta', { content: '#0f172a', name: 'theme-color' }],
		['link', { href: 'https://github.com/andrewmunsell/tinybase-supabase', rel: 'icon' }],
	],
	lastUpdated: true,
	themeConfig: {
		docFooter: { next: 'Next page', prev: 'Previous page' },
		editLink: {
			pattern: `https://github.com/${repository}/edit/main/:path`,
			text: 'Edit this page on GitHub',
		},
		nav: [
			{ link: '/guide/getting-started', text: 'Guide' },
			{ link: '/examples', text: 'Example' },
			{ link: `https://github.com/${repository}`, text: 'GitHub' },
		],
		sidebar: [
			{
				items: [
					{ link: '/guide/getting-started', text: 'Getting started' },
					{ link: '/guide/configuration', text: 'Configuration' },
					{ link: '/guide/collaborative-crdts', text: 'Collaborative CRDT cells' },
					{ link: '/guide/offline-and-conflicts', text: 'Offline and conflicts' },
					{ link: '/guide/supabase-and-rls', text: 'Supabase and RLS' },
					{ link: '/guide/realtime', text: 'Realtime' },
					{ link: '/guide/testing', text: 'Testing locally' },
				],
				text: 'Guide',
			},
			{
				items: [{ link: '/examples', text: 'Interactive todo' }],
				text: 'Examples',
			},
		],
		socialLinks: [{ icon: 'github', link: `https://github.com/${repository}` }],
	},
	title: 'tinybase-supabase',
	vite: {
		resolve: {
			alias: {
				'tinybase-supabase': fileURLToPath(new URL('../../src/index.ts', import.meta.url)),
			},
		},
	},
});
