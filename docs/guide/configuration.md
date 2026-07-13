# Configuration

Each `tables` key is a TinyBase table Id. Its `table` value is the remote
Supabase table. The default primary key is `id`, and the default tombstone
column is `deleted_at`.

Collaborative cells are configured on the same table mapping with `crdtCells`,
`crdtUpdatesTable`, and optionally `crdtRowIdColumn`. Omit `crdtCells` for the
ordinary whole-row path. See [Collaborative CRDT cells](./collaborative-crdts)
for the complete update-table and RLS contract.

`crdtUpdateBufferMs` configures how long local Yjs updates are collected and
merged per row before upload. It defaults to `500`; set it to `0` for immediate
upload. Local durability is not delayed.

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

`read-only` mappings are pulled normally but never create or upload an ordinary
or CRDT outbox operation. CRDT rows can still be opened to hydrate, follow, and
project their remote content; local mutations through their Yjs handles throw.

For mixed-access tables, keep the mapping read-write, expose editing controls
for writable rows, and enforce row permissions with Supabase RLS. Permanent
CRDT write failures are quarantined per row so dependent local updates cannot
upload out of order.
