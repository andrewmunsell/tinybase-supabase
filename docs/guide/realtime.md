# Realtime

Set `realtime: true` on a mapped table after adding that table to the
`supabase_realtime` publication.

```ts
todos: {realtime: {debounceMs: 200}, table: 'todos'}
```

Realtime is a **wake-up signal**, not the write channel or durable source of
truth. A matching event schedules a debounced authenticated pull. Startup,
focus, reconnect, periodic, and manual pulls remain necessary for missed events,
deletions, and authorization changes.
