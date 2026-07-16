---
'tinybase-supabase': minor
---

Add opt-in durable per-table cursors for incremental parent-row pulls while preserving full pulls by default.

Existing configurations remain on automatically paginated full authoritative pulls. To enable incremental pulls, add a non-null server-managed timestamp, set `updatedAtColumn`, and include the configured ID, updated-at, and deleted-at columns in custom `select` projections.

For opted-in tables, the first hydration remains a full authoritative pull and later pulls are incremental. Use durable soft-delete tombstones and version `scopeKey` when grants or revocations change which rows are visible.

Reload other tabs running the previous package version if the IndexedDB v2 upgrade reports that it is blocked.
