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
