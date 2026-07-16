# Supabase schema and RLS

Use client-created text or UUID IDs. Every mapped table needs a nullable
soft-delete timestamp. We strongly recommend a non-null server-managed update
timestamp and `updatedAtColumn: 'updated_at'` for incremental pulls. Omitting
the option requires no update timestamp and retains paginated full pulls. A
recommended todo row might look like:

```sql
create table public.todos (
  id text primary key,
  owner_id uuid not null references auth.users,
  project_id text not null references public.projects,
  title text not null,
  completed boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  deleted_at timestamptz,
  updated_at timestamptz not null default clock_timestamp()
);

create index todos_sync_cursor_idx on public.todos (updated_at, id);

create function public.set_updated_at()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  new.updated_at = clock_timestamp();
  return new;
end;
$$;

create trigger todos_set_updated_at
before insert or update on public.todos
for each row execute function public.set_updated_at();
```

Use `deleted_at` instead of physically deleting mapped rows. The trigger
advances `updated_at`, allowing other clients to receive the tombstone through
their incremental pull.

Revoke browser DELETE privileges as a defense in depth measure:

```sql
revoke delete on public.todos from anon, authenticated;
```

Enable RLS for private data. The browser must only use a publishable/anon key;
never put a service-role key into an app or this documentation example.

Public reference data can be mapped as `read-only`. It may be a table without
RLS or a table with an explicit public `select` policy. The local test fixture
includes both shapes.

In incremental mode, RLS must continue exposing tombstones to scopes that
previously read the live row. Cursor pulls also cannot discover an older row
that becomes newly visible, or a cached row that becomes invisible, unless the
parent row is updated. When grants or revocations change the visible row set,
use an authorization version in `scopeKey` so the new scope performs a full
initial hydration. Full-pull mode reconciles the currently visible set by
absence on every pull.

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
