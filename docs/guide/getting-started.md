# Getting started

Install TinyBase, the Supabase browser client, and this package:

```sh
pnpm add tinybase @supabase/supabase-js tinybase-supabase
```

Create your Supabase client with a publishable key, then create a regular
TinyBase `Store` and start automatic persistence.

```ts
import {createClient} from '@supabase/supabase-js';
import {createStore} from 'tinybase';
import {createSupabasePersister} from 'tinybase-supabase';

const store = createStore();
const supabase = createClient(url, publishableKey);
const persister = await createSupabasePersister(store, {
	databaseName: 'my-app',
	scopeKey: user.id,
	supabase,
	tables: {
		projects: {table: 'projects', updatedAtColumn: 'updated_at'},
		todos: {
			dependsOn: ['projects'],
			realtime: true,
			table: 'todos',
			updatedAtColumn: 'updated_at',
		},
	},
});

await persister.startAutoPersisting();
```

`scopeKey` must change when the signed-in account, tenant, or authorization
version changes. It keeps each visible row set and outbox separate.

`updatedAtColumn` is optional for backward compatibility. We strongly recommend
the shown server-managed timestamp configuration; omitting it makes every
reconciliation a paginated full authoritative pull.
