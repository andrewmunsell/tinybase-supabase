# Offline writes and conflicts

A local TinyBase change first persists to IndexedDB with its coalesced outbox
operation. The persister then tries Supabase. If the browser is offline, it
retries with capped exponential backoff and again on browser reconnect or tab
focus.

## Conflict rule

Ordinary cells synchronized through direct Supabase CRUD use **whole-row,
server-arrival last-write-wins**. When
two offline clients change the same row, the write the server accepts last wins;
the next complete pull brings the other client to that state.

This ordinary mode does not provide exactly-once delivery or cross-table
transactions. For concurrent changes within selected text, map, or array
values, configure [collaborative CRDT cells](./collaborative-crdts).

## Rejected writes

Validation and RLS failures become rejected operations. The optimistic TinyBase
row remains visible so the application can explain the error and let the user
repair it.

```ts
const rejected = await persister.getRejectedOperations();

await persister.retryRejected();
// or stop retrying; this does not mutate the local optimistic row:
await persister.discardRejected();
```
