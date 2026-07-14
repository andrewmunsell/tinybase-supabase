# tinybase-supabase

## 0.2.2

### Patch Changes

- afe5ec9: Allow read-only CRDT tables to hydrate and follow remote Yjs content without
  allowing local mutations, persisting local updates, or uploading them. Rejected
  CRDT updates now quarantine their document's causal history for safe retry or
  authoritative discard while Supabase RLS remains the write boundary.

## 0.2.1

### Patch Changes

- d9e77e8: Add collaborative Yjs XML fragment cells with serialized XML TinyBase projections.

## 0.2.0

### Minor Changes

- Add optional per-cell Yjs collaboration for TinyBase text, map, and array
  cells, including durable offline updates, Supabase RLS and Realtime support,
  and configurable per-document update buffering and compaction.
