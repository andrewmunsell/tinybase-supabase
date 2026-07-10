# Configuration

Each `tables` key is a TinyBase table Id. Its `table` value is the remote
Supabase table. The default primary key is `id`, and the default tombstone
column is `deleted_at`.

```ts
tables: {
	projects: {
		table: 'projects',
	},
	todos: {
		dependsOn: ['projects'],
		idColumn: 'id',
		deletedAtColumn: 'deleted_at',
		table: 'todos',
	},
	publicTemplates: {
		mode: 'read-only',
		table: 'public_templates',
	},
}
```

Use `toRemote` and `fromRemote` when cell and column names differ or when the
application needs a domain codec. The package automatically adds the configured
primary key to every outgoing row.

`read-only` mappings are pulled normally but never create an outbox operation.
