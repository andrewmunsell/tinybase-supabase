# Realtime

Set `realtime: true` on a mapped table after adding that table to the
`supabase_realtime` publication.

```ts
todos: {
	realtime: {debounceMs: 200},
	table: 'todos',
	updatedAtColumn: 'updated_at',
}
```

Realtime is a **wake-up signal**, not the write channel or durable source of
truth. Ordinary parent-table events and CRDT updates-table events share one
debouncer and one reconciliation path. When several events arrive together,
the earliest configured deadline wins and one pass flushes pending writes,
pulls ordinary tables, and reconciles all open Yjs documents.

For tables with CRDT cells, add both the parent table and the configured
`crdtUpdatesTable` to `supabase_realtime`. Updates-table subscriptions are
created lazily for open rows and filtered by `crdtRowIdColumn`.

Startup, focus, reconnect, periodic, and manual synchronization use the same
reconciliation path. With `updatedAtColumn`, the initial pull is a full
authoritative snapshot and later pulls fetch rows at or after the stored
watermark. Without it, every wake-up performs a paginated full pull. Realtime
remains a hint; reconciliation recovers missed events independently of
notification delivery.
