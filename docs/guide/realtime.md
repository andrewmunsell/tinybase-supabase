# Realtime

Set `realtime: true` on a mapped table after adding that table to the
`supabase_realtime` publication.

```ts
todos: {realtime: {debounceMs: 200}, table: 'todos'}
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
cursor-based reconciliation path. The initial pull is a full authoritative
snapshot; later pulls fetch rows at or after the stored `updated_at` watermark.
Realtime remains a wake-up hint, while cursor pulls recover missed events and
soft-delete tombstones within the configured cursor lookback window.
