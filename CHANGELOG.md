# tinybase-supabase

## 0.3.0

### Minor Changes

- 0a0f783: Add opt-in durable per-table cursors for incremental parent-row pulls while preserving full pulls by default.

  Existing configurations remain on automatically paginated full authoritative pulls. To enable incremental pulls, add a non-null server-managed timestamp, set `updatedAtColumn`, and include the configured ID, updated-at, and deleted-at columns in custom `select` projections.

  For opted-in tables, the first hydration remains a full authoritative pull and later pulls are incremental. Use durable soft-delete tombstones and version `scopeKey` when grants or revocations change which rows are visible.

  Reload other tabs running the previous package version if the IndexedDB v2 upgrade reports that it is blocked.

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
