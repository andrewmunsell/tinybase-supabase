create table public.projects (
    id text primary key,
    owner_id uuid not null references auth.users (id) on delete cascade,
    title text not null,
    active boolean not null default true,
    settings jsonb not null default '{}'::jsonb,
    deleted_at timestamptz,
    created_at timestamptz not null default now()
);

create table public.todos (
    id text primary key,
    project_id text not null references public.projects (id),
    owner_id uuid not null references auth.users (id) on delete cascade,
    title text not null,
    completed boolean not null default false,
    priority integer not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    tags text[] not null default '{}',
    due_at timestamptz,
    attachment_path text,
    deleted_at timestamptz,
    created_at timestamptz not null default now()
);

create table public.audit_logs (
    id text primary key,
    owner_id uuid not null references auth.users (id) on delete cascade,
    action text not null,
    payload jsonb not null default '{}'::jsonb,
    deleted_at timestamptz,
    created_at timestamptz not null default now()
);

-- This table intentionally has no RLS: it models data that is safely public.
create table public.public_templates (
    id text primary key,
    title text not null,
    metadata jsonb not null default '{}'::jsonb
);

-- This table uses RLS but permits public reads and no browser writes.
create table public.shared_templates (
    id text primary key,
    title text not null,
    metadata jsonb not null default '{}'::jsonb
);

alter table public.projects enable row level security;
alter table public.todos enable row level security;
alter table public.audit_logs enable row level security;
alter table public.shared_templates enable row level security;

grant select on public.public_templates to anon, authenticated;
grant select on public.shared_templates to anon, authenticated;

create policy "projects are private to their owner"
on public.projects
for all
to authenticated
using ((select auth.uid()) = owner_id)
with check ((select auth.uid()) = owner_id);

create policy "todos are private to their owner"
on public.todos
for all
to authenticated
using ((select auth.uid()) = owner_id)
with check ((select auth.uid()) = owner_id);

create policy "audit logs are private to their owner"
on public.audit_logs
for all
to authenticated
using ((select auth.uid()) = owner_id)
with check ((select auth.uid()) = owner_id);

create policy "shared templates are publicly readable"
on public.shared_templates
for select
to anon, authenticated
using (true);

alter publication supabase_realtime add table public.projects, public.todos;
