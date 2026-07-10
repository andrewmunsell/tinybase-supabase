<script setup lang="ts">
import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { createStore, type Store } from 'tinybase';
import { onUnmounted, ref } from 'vue';
import { createSupabasePersister, type SupabasePersister } from 'tinybase-supabase';

type Todo = {
	completed: boolean;
	id: string;
	title: string;
};

const apiUrl = ref('http://127.0.0.1:55421');
const publishableKey = ref('');
const email = ref('');
const password = ref('');
const title = ref('');
const message = ref('');
const todos = ref<Todo[]>([]);
const isConnected = ref(false);
const isBusy = ref(false);

let client: SupabaseClient | undefined;
let persister: SupabasePersister | undefined;
let store: Store | undefined;
let userId: string | undefined;
let listenerId: string | undefined;

const refreshTodos = (): void => {
	if (!store) {
		todos.value = [];
		return;
	}

	todos.value = Object.entries(store.getTable('todos')).map(([id, row]) => ({
		completed: row.completed === true,
		id,
		title: String(row.title ?? ''),
	}));
};

const disconnect = async (): Promise<void> => {
	if (listenerId && store) {
		store.delListener(listenerId);
	}
	listenerId = undefined;
	await persister?.destroy();
	await client?.auth.signOut();
	client = undefined;
	persister = undefined;
	store = undefined;
	userId = undefined;
	isConnected.value = false;
	refreshTodos();
};

// biome-ignore lint/correctness/noUnusedVariables: Vue template event handler.
const connect = async (): Promise<void> => {
	isBusy.value = true;
	message.value = '';
	try {
		await disconnect();
		client = createClient(apiUrl.value, publishableKey.value, {
			auth: { autoRefreshToken: false, persistSession: true },
		});
		const response = await client.auth.signInWithPassword({
			email: email.value,
			password: password.value,
		});
		if (response.error || !response.data.user) {
			throw response.error ?? new Error('Supabase did not return a user');
		}

		userId = response.data.user.id;
		store = createStore();
		persister = await createSupabasePersister(store, {
			databaseName: 'tinybase-supabase-docs-example',
			scopeKey: userId,
			supabase: client,
			tables: {
				projects: { table: 'projects' },
				todos: { dependsOn: ['projects'], realtime: true, table: 'todos' },
			},
		});
		await persister.startAutoPersisting();
		listenerId = store.addTableListener('todos', refreshTodos);
		refreshTodos();
		isConnected.value = true;
		message.value = 'Connected. New edits are durable offline and will sync when available.';
	} catch (error) {
		message.value = error instanceof Error ? error.message : 'Could not connect';
	} finally {
		isBusy.value = false;
	}
};

// biome-ignore lint/correctness/noUnusedVariables: Vue template event handler.
const addTodo = async (): Promise<void> => {
	if (!store || !persister || !userId || !title.value.trim()) {
		return;
	}
	const todoId = crypto.randomUUID();
	const projectId = 'docs-example-project';
	store.transaction(() => {
		store?.setRow('projects', projectId, {
			active: true,
			owner_id: userId,
			settings: { source: 'docs-example' },
			title: 'Documentation example',
		});
		store?.setRow('todos', todoId, {
			completed: false,
			metadata: { source: 'docs-example' },
			owner_id: userId,
			priority: 0,
			project_id: projectId,
			tags: ['docs'],
			title: title.value.trim(),
		});
	});
	await persister.save();
	title.value = '';
	refreshTodos();
};

// biome-ignore lint/correctness/noUnusedVariables: Vue template event handler.
const toggleTodo = async (todo: Todo): Promise<void> => {
	if (!store || !persister) {
		return;
	}
	store.setCell('todos', todo.id, 'completed', !todo.completed);
	await persister.save();
	refreshTodos();
};

onUnmounted(() => {
	void disconnect();
});
</script>

<template>
	<div class="demo">
		<p class="notice">
			Use a disposable local or demo Supabase project and a publishable key. Never enter a
			service-role key here.
		</p>
		<div v-if="!isConnected" class="fields">
			<label>
				Supabase URL
				<input v-model="apiUrl" type="url" />
			</label>
			<label>
				Publishable key
				<input v-model="publishableKey" type="password" />
			</label>
			<label>
				Email
				<input v-model="email" type="email" />
			</label>
			<label>
				Password
				<input v-model="password" type="password" />
			</label>
			<button :disabled="isBusy || !publishableKey || !email || !password" @click="connect">
				{{ isBusy ? 'Connecting…' : 'Connect' }}
			</button>
		</div>
		<div v-else>
			<form class="add-todo" @submit.prevent="addTodo">
				<input v-model="title" placeholder="Write a todo" />
				<button :disabled="!title.trim()" type="submit">Add todo</button>
			</form>
			<ul>
				<li v-for="todo in todos" :key="todo.id">
					<label>
						<input :checked="todo.completed" type="checkbox" @change="toggleTodo(todo)" />
						<span :class="{ completed: todo.completed }">{{ todo.title }}</span>
					</label>
				</li>
			</ul>
			<button class="secondary" @click="disconnect">Disconnect</button>
		</div>
		<p v-if="message" class="message">{{ message }}</p>
	</div>
</template>

<style scoped>
.demo {
	border: 1px solid var(--vp-c-divider);
	border-radius: 12px;
	margin: 24px 0;
	padding: 20px;
}

.fields,
.add-todo {
	display: grid;
	gap: 12px;
	grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
}

label {
	display: grid;
	font-size: 0.9rem;
	gap: 4px;
}

input {
	border: 1px solid var(--vp-c-divider);
	border-radius: 6px;
	padding: 8px;
}

button {
	align-self: end;
	background: var(--vp-c-brand-1);
	border: 0;
	border-radius: 6px;
	color: white;
	cursor: pointer;
	padding: 8px 12px;
}

button.secondary {
	background: var(--vp-c-default-2);
	margin-top: 12px;
}

.notice,
.message {
	color: var(--vp-c-text-2);
}

.completed {
	text-decoration: line-through;
}
</style>
