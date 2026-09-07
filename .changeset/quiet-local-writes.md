---
"tinybase-supabase": patch
---

Preserve concurrent local edits and deletions during remote pulls and hydration,
wait for durable remote application before successful synchronization completes,
and acknowledge or reject only the outbox revision sent to Supabase.
