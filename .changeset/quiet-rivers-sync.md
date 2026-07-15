---
'tinybase-supabase': minor
---

Require server-managed update timestamps and use durable per-table cursors for incremental parent-row pulls.

Before upgrading, add non-null `updated_at` and nullable `deleted_at` columns to every mapped table or view, including read-only mappings. Update custom `select` projections to include the configured ID, updated-at, and deleted-at columns.

The first hydration remains a full authoritative pull. Later pulls are incremental, so physical deletes and changes to the RLS-visible row set are no longer detected by absence. Use durable soft-delete tombstones and version `scopeKey` when grants or revocations change which rows are visible.

Reload other tabs running the previous package version if the IndexedDB v2 upgrade reports that it is blocked.
