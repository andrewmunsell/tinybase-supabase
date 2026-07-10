create table public.crdt_workspaces (
    id text primary key,
    owner_id uuid not null references auth.users (id) on delete cascade
);

create table public.crdt_workspace_members (
    workspace_id text not null references public.crdt_workspaces (id) on delete cascade,
    user_id uuid not null references auth.users (id) on delete cascade,
    can_write boolean not null default false,
    primary key (workspace_id, user_id)
);

create table public.crdt_documents (
    id text primary key,
    workspace_id text references public.crdt_workspaces (id) on delete cascade,
    owner_id uuid not null references auth.users (id) on delete cascade,
    status text not null default 'draft',
    deleted_at timestamptz
);

create table public.crdt_document_collaborators (
    document_id text not null references public.crdt_documents (id) on delete cascade,
    user_id uuid not null references auth.users (id) on delete cascade,
    can_write boolean not null default false,
    primary key (document_id, user_id)
);

create table public.crdt_document_updates (
    id uuid primary key,
    document_id text not null references public.crdt_documents (id) on delete cascade,
    update bytea not null,
    created_at timestamptz not null default now()
);

create index crdt_document_updates_document_order_idx
on public.crdt_document_updates (document_id, created_at, id);

alter table public.crdt_workspaces enable row level security;
alter table public.crdt_workspace_members enable row level security;
alter table public.crdt_documents enable row level security;
alter table public.crdt_document_collaborators enable row level security;
alter table public.crdt_document_updates enable row level security;

create function public.can_read_crdt_document(requested_document text)
returns boolean
language sql
security definer
set search_path = ''
stable
as $$
    select exists (
        select 1 from public.crdt_documents d
        where d.id = requested_document
          and (
            d.owner_id = (select auth.uid())
            or exists (
                select 1 from public.crdt_workspace_members wm
                where wm.workspace_id = d.workspace_id and wm.user_id = (select auth.uid())
            )
            or exists (
                select 1 from public.crdt_document_collaborators dc
                where dc.document_id = d.id and dc.user_id = (select auth.uid())
            )
          )
    );
$$;

create function public.can_write_crdt_document(requested_document text)
returns boolean
language sql
security definer
set search_path = ''
stable
as $$
    select exists (
        select 1 from public.crdt_documents d
        where d.id = requested_document
          and d.deleted_at is null
          and (
            d.owner_id = (select auth.uid())
            or exists (
                select 1 from public.crdt_workspace_members wm
                where wm.workspace_id = d.workspace_id
                  and wm.user_id = (select auth.uid())
                  and wm.can_write
            )
            or exists (
                select 1 from public.crdt_document_collaborators dc
                where dc.document_id = d.id
                  and dc.user_id = (select auth.uid())
                  and dc.can_write
            )
          )
    );
$$;

revoke all on function public.can_read_crdt_document(text) from public, anon;
revoke all on function public.can_write_crdt_document(text) from public, anon;
grant execute on function public.can_read_crdt_document(text) to authenticated;
grant execute on function public.can_write_crdt_document(text) to authenticated;

create policy "workspace owners manage workspaces"
on public.crdt_workspaces for all to authenticated
using ((select auth.uid()) = owner_id)
with check ((select auth.uid()) = owner_id);

create policy "workspace owners manage members"
on public.crdt_workspace_members for all to authenticated
using (exists (
    select 1 from public.crdt_workspaces w
    where w.id = workspace_id and w.owner_id = (select auth.uid())
))
with check (exists (
    select 1 from public.crdt_workspaces w
    where w.id = workspace_id and w.owner_id = (select auth.uid())
));

create policy "documents visible through owner workspace or collaboration"
on public.crdt_documents for select to authenticated
using (public.can_read_crdt_document(id));

create policy "document owners write documents"
on public.crdt_documents for all to authenticated
using (owner_id = (select auth.uid()))
with check (owner_id = (select auth.uid()));

create policy "document owners manage collaborators"
on public.crdt_document_collaborators for all to authenticated
using (exists (
	select 1 from public.crdt_documents d
	where d.id = document_id and d.owner_id = (select auth.uid())
))
with check (exists (
    select 1 from public.crdt_documents d
    where d.id = document_id and d.owner_id = (select auth.uid())
));

create policy "authorized relationships read CRDT updates"
on public.crdt_document_updates for select to authenticated
using (exists (
	select 1 from public.crdt_documents d
	where d.id = document_id
	  and d.deleted_at is null
      and public.can_read_crdt_document(d.id)
));

create policy "authorized relationships write CRDT updates"
on public.crdt_document_updates for insert to authenticated
with check (public.can_write_crdt_document(document_id));

revoke all on public.crdt_document_updates from anon, authenticated;
grant select on public.crdt_document_updates to authenticated;
grant insert (id, document_id, update) on public.crdt_document_updates to authenticated;
alter publication supabase_realtime add table public.crdt_document_updates;
