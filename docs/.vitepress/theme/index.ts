import DefaultTheme from 'vitepress/theme';
import type { Theme } from 'vitepress';
import TodoDemo from './components/TodoDemo.vue';

export default {
	extends: DefaultTheme,
	enhanceApp({ app }) {
		app.component('TodoDemo', TodoDemo);
	},
} satisfies Theme;
