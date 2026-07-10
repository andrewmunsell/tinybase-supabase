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
		projects: {table: 'projects'},
		todos: {dependsOn: ['projects'], realtime: true, table: 'todos'},
	},
});

await persister.startAutoPersisting();
```

`scopeKey` must change when the signed-in account or tenant changes. It keeps
their browser persistence and outbox separate.
