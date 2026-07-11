# tinybase-supabase

`tinybase-supabase` is a browser-only, offline-first persister for regular
TinyBase `Store` instances. It stores data and an outbox in IndexedDB, writes
mapped rows through the Supabase JS client, and reconciles remote state after
reconnecting.

It supports both ESM and CommonJS consumers, direct Supabase CRUD, RLS, soft
deletes, optional Realtime wake-and-reconcile subscriptions, and local-only or
read-only TinyBase tables.

The same `createSupabasePersister` factory optionally supports collaborative
`text`, `map`, and `array` cells. When configured, it stores append-only Yjs
updates in a child table while keeping IDs, ownership, relationships, and other
RLS fields on the ordinary parent row. Configurations with no CRDT cells use
only the ordinary whole-row implementation and require no updates table. See the
[collaborative CRDT guide](docs/guide/collaborative-crdts.md) for the required
schema and many-to-one and many-to-many policy examples.

## Documentation

The versioned source for the documentation and interactive Todo example lives in
[`docs/`](docs/). GitHub Actions builds it on every pull request and publishes
the `main` branch to [GitHub Pages](https://andrewmunsell.github.io/tinybase-supabase/).

## Installation

```sh
npm install tinybase tinybase-supabase @supabase/supabase-js
```

```ts
import {createStore} from 'tinybase';
import {createClient} from '@supabase/supabase-js';
import {createSupabasePersister} from 'tinybase-supabase';

const store = createStore();
const supabase = createClient(import.meta.env.VITE_SUPABASE_URL, import.meta.env.VITE_SUPABASE_KEY);
const persister = await createSupabasePersister(store, {
	crdtUpdateBufferMs: 500,
	databaseName: 'my-app',
	scopeKey: 'current-user-id',
	supabase,
	tables: {
		projects: {table: 'projects'},
		todos: {
			dependsOn: ['projects'],
			realtime: true,
			table: 'todos',
		},
		public_templates: {mode: 'read-only', table: 'public_templates'},
	},
});

await persister.startAutoPersisting();
```

CommonJS projects can use the same public API:

```js
const {createSupabasePersister} = require('tinybase-supabase');
```

## Configuration reference

`createSupabasePersister(store, config)` accepts the following top-level
options:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `supabase` | Supabase client | Required | A browser client created with a publishable or anon key. Do not use a service-role client. |
| `databaseName` | `string` | Required | Stable IndexedDB database name for this application. |
| `scopeKey` | `string` | Required | Stable user or tenant identifier. It isolates one authenticated scope from another in IndexedDB. |
| `tables` | `Record<string, SupabaseTableConfig>` | Required | Mapped tables, keyed by TinyBase table ID. TinyBase tables omitted from this object remain local-only. |
| `pollIntervalMs` | `number` | `60000` | Safety-pull interval in milliseconds. Set to `0` to disable interval pulls. |
| `pageSize` | `number` | `500` | Maximum number of parent rows or CRDT update rows fetched per Supabase page. |
| `crdtUpdateBufferMs` | `number` | `500` | Time to buffer and merge local Yjs updates per row before upload. Set to `0` for immediate upload; local durability is not delayed. |
| `retryBaseDelayMs` | `number` | `1000` | Initial delay for exponential retry after a transient synchronization failure. |
| `retryMaxDelayMs` | `number` | `30000` | Maximum delay between retries after repeated transient failures. |
| `onError` | `(error: Error) => void` | None | Receives persistence, Realtime, and synchronization errors. |

Each entry in `tables` supports these options:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `table` | `string` | Required | Remote Supabase table name. The containing object key is the TinyBase table ID. |
| `mode` | `'read-write' \| 'read-only'` | `'read-write'` | Read-only mappings pull remote rows but never enqueue local writes. |
| `idColumn` | `string` | `'id'` | Existing remote primary-key column. IDs must be created by the application and are always added to outgoing rows. |
| `deletedAtColumn` | `string` | `'deleted_at'` | Existing nullable timestamp column used for soft-delete tombstones. |
| `dependsOn` | `readonly string[]` | `[]` | TinyBase table IDs that must be uploaded before this table. Tombstones are sent in the reverse dependency order. |
| `select` | `string` | `'*'` | Columns passed to Supabase `select()` during parent-row reconciliation. Include the configured ID and soft-delete columns. |
| `toRemote` | `(rowId: string, row: Row) => SupabaseRow` | Identity mapping | Encodes a TinyBase row for Supabase. The configured ID column is added after this function returns. |
| `fromRemote` | `(row: SupabaseRow) => [string, Row]` | Default column mapping | Decodes a remote row. By default, the ID becomes the TinyBase row ID and the ID and soft-delete columns are omitted from its cells. |
| `realtime` | `boolean \| RealtimeTableConfig` | `false` | Enables Postgres Changes as a debounced pull wake-up. Use `true` for the defaults or an object for the options below. |
| `crdtCells` | `Record<string, CrdtCellConfig>` | `{}` | Collaborative cells keyed by TinyBase cell ID. Each value is `{type: 'text'}`, `{type: 'map'}`, or `{type: 'array'}`. |
| `crdtUpdatesTable` | `string` | None | Append-only Yjs updates table. Required when `crdtCells` is non-empty. |
| `crdtRowIdColumn` | `string` | `'row_id'` | Foreign-key column in `crdtUpdatesTable` that refers to the parent row. |

`realtime: true` uses the defaults below. Pass an object to override either
option:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `channelName` | `string` | `'tinybase-supabase:<scopeKey>:<tableId>'` | Supabase Realtime channel name for parent-row changes. CRDT update channels use their own per-row names. |
| `debounceMs` | `number` | `200` | Delay used to coalesce Realtime notifications before pulling. It applies to both parent-row and CRDT update notifications. |

For example, a configuration using every option can look like this:

```ts
const persister = await createSupabasePersister(store, {
	crdtUpdateBufferMs: 250,
	databaseName: 'my-app',
	onError: (error) => console.error(error),
	pageSize: 250,
	pollIntervalMs: 30_000,
	retryBaseDelayMs: 500,
	retryMaxDelayMs: 15_000,
	scopeKey: user.id,
	supabase,
	tables: {
		documents: {
			crdtCells: {
				body: {type: 'text'},
				metadata: {type: 'map'},
				tags: {type: 'array'},
			},
			crdtRowIdColumn: 'document_id',
			crdtUpdatesTable: 'document_yjs_updates',
			deletedAtColumn: 'deleted_at',
			dependsOn: ['projects'],
			fromRemote: (row) => [String(row.id), {title: String(row.title)}],
			idColumn: 'id',
			mode: 'read-write',
			realtime: {
				channelName: `documents:${user.id}`,
				debounceMs: 100,
			},
			select: 'id,title,deleted_at',
			table: 'documents',
			toRemote: (_rowId, row) => ({title: row.title}),
		},
		projects: {table: 'projects'},
		publicTemplates: {
			mode: 'read-only',
			table: 'public_templates',
		},
	},
});
```

`crdtCells` cannot be combined with `mode: 'read-only'`. If any collaborative
cells are configured, provide the update table and policies described in the
[collaborative CRDT guide](docs/guide/collaborative-crdts.md).

## Database contract

Every read-write mapping needs an application-created string or UUID primary
key (default column: `id`) and a nullable soft-delete timestamp (default:
`deleted_at`). The client never uses a service-role key. Enable RLS and grant
only the operations your authenticated users need.

Each TinyBase row maps to a whole remote row. Use `toRemote` and `fromRemote`
for renamed columns or domain codecs. JSON values, arrays, nullable values,
dates encoded as strings, and storage-path references are supported by normal
Supabase column mappings.

`mode: 'read-only'` pulls a table but never queues writes. This is suitable for
public reference data and data available through read-only RLS policies.

## Synchronization behavior

Writes are locally durable once the IndexedDB transaction completes. While
offline, edits remain optimistic in TinyBase and are queued. Permanent Supabase
errors such as RLS rejection are retained as rejected operations; inspect them
with `getRejectedOperations`, then retry or discard them. Discarding stops
retries but deliberately does not alter the local optimistic row.

Transient failures retry with capped exponential backoff. Sync also runs on
browser reconnect and when a hidden tab becomes visible, in addition to the
optional periodic safety pull.

Ordinary cells use server-arrival, full-row last-write-wins. They do not promise
cross-table transactions or exactly-once delivery. Cells explicitly configured
as Yjs types use append-only CRDT updates and merge concurrent changes within
their values.

Local Yjs updates are durable immediately, then buffered for 500 ms by default
and merged per row before upload. Set `crdtUpdateBufferMs` to tune the
collaboration-latency/row-count tradeoff, or call `syncNow()` to flush the
current buffer immediately.

With `realtime: true`, Postgres Changes only wakes a debounced authenticated
pull. CRUD remains the write path, and startup, reconnect, manual, and safety
pulls remain authoritative. Add opted-in tables to the `supabase_realtime`
publication in your own schema.

## Local development and tests

```sh
pnpm install
supabase start
supabase status -o env
```

Set `SUPABASE_TEST_URL` from `API_URL` and `SUPABASE_TEST_ANON_KEY` from
`ANON_KEY`, then run:

```sh
pnpm check
pnpm test:unit
pnpm test:integration
pnpm test:e2e
pnpm build
```

The local fixture covers private authenticated todo data, invalid RLS writes,
anonymous public data with and without RLS, Realtime and non-Realtime tables,
and Chromium browser scenarios for offline reload, reconnect, Realtime,
two-client server-arrival conflict resolution, durable CRDT rehydration, and
concurrent Y.Text convergence.

Formatting is enforced by Biome with four-width tabs, semicolons, single
quotes, trailing commas, and no internal barrel files.
