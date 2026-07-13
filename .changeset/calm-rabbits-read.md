---
'tinybase-supabase': patch
---

Allow read-only CRDT tables to hydrate and follow remote Yjs content without
allowing local mutations, persisting local updates, or uploading them. Rejected
CRDT updates now quarantine their document's causal history for safe retry or
authoritative discard while Supabase RLS remains the write boundary.
