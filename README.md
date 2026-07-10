# tinybase-supabase

`tinybase-supabase` is a browser-only, offline-first persister for regular
TinyBase `Store` instances. It stores data and an outbox in IndexedDB, writes
mapped rows through the Supabase JS client, and reconciles remote state after
reconnecting.

It supports both ESM and CommonJS consumers, direct Supabase CRUD, RLS, soft
deletes, optional Realtime wake-and-reconcile subscriptions, and local-only or
read-only TinyBase tables.

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

Direct CRUD uses server-arrival, full-row last-write-wins. It does not promise
cross-table transactions, exactly-once delivery, or CRDT merge semantics.
Use an RPC-backed design when those guarantees are required.

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
and a Chromium browser scenario for offline reload, reconnect, Realtime, and
two-client server-arrival conflict resolution.

Formatting is enforced by Biome with four-width tabs, semicolons, single
quotes, trailing commas, and no internal barrel files.
