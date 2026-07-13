# Supabase schema and RLS

Use client-created text or UUID IDs. Every read-write table needs a nullable
soft-delete timestamp column. A minimal todo row might look like:

```sql
create table public.todos (
  id text primary key,
  owner_id uuid not null references auth.users,
  project_id text not null references public.projects,
  title text not null,
  completed boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  deleted_at timestamptz
);
```

Enable RLS for private data. The browser must only use a publishable/anon key;
never put a service-role key into an app or this documentation example.

Public reference data can be mapped as `read-only`. It may be a table without
RLS or a table with an explicit public `select` policy. The local test fixture
includes both shapes.

## Mixed read and write access

`mode` applies to the whole table mapping. Use `read-only` when every row in the
mapping is view-only for this persister instance. A read-only CRDT mapping still
fetches existing updates, follows Realtime notifications, and projects
collaborative values for rendering.

When the same user can write some rows and only read others, leave the mapping
read-write, expose editing controls for writable rows, and enforce permissions
with Supabase RLS. Permanent RLS failures are available through
`getRejectedOperations()`.

Rejected ordinary writes remain individually retryable or discardable. A
rejected CRDT write quarantines that document's unaccepted causal history so a
later update cannot be uploaded ahead of it. See
[Offline writes and conflicts](./offline-and-conflicts) for recovery behavior.
