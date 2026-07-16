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

alter table public.projects add column updated_at timestamptz not null default clock_timestamp();
alter table public.todos add column updated_at timestamptz not null default clock_timestamp();
alter table public.audit_logs add column updated_at timestamptz not null default clock_timestamp();
alter table public.public_templates
    add column deleted_at timestamptz,
    add column updated_at timestamptz not null default clock_timestamp();
alter table public.shared_templates
    add column deleted_at timestamptz,
    add column updated_at timestamptz not null default clock_timestamp();
alter table public.crdt_documents
    add column updated_at timestamptz not null default clock_timestamp();

create trigger projects_set_updated_at before insert or update on public.projects
for each row execute function public.set_updated_at();
create trigger todos_set_updated_at before insert or update on public.todos
for each row execute function public.set_updated_at();
create trigger audit_logs_set_updated_at before insert or update on public.audit_logs
for each row execute function public.set_updated_at();
create trigger public_templates_set_updated_at before insert or update on public.public_templates
for each row execute function public.set_updated_at();
create trigger shared_templates_set_updated_at before insert or update on public.shared_templates
for each row execute function public.set_updated_at();
create trigger crdt_documents_set_updated_at before insert or update on public.crdt_documents
for each row execute function public.set_updated_at();

create index projects_sync_cursor_idx on public.projects (updated_at, id);
create index todos_sync_cursor_idx on public.todos (updated_at, id);
create index audit_logs_sync_cursor_idx on public.audit_logs (updated_at, id);
create index public_templates_sync_cursor_idx on public.public_templates (updated_at, id);
create index shared_templates_sync_cursor_idx on public.shared_templates (updated_at, id);
create index crdt_documents_sync_cursor_idx on public.crdt_documents (updated_at, id);

alter table public.projects
    drop constraint projects_owner_id_fkey,
    add constraint projects_owner_id_fkey foreign key (owner_id) references auth.users (id);
alter table public.todos
    drop constraint todos_owner_id_fkey,
    add constraint todos_owner_id_fkey foreign key (owner_id) references auth.users (id);
alter table public.audit_logs
    drop constraint audit_logs_owner_id_fkey,
    add constraint audit_logs_owner_id_fkey foreign key (owner_id) references auth.users (id);
alter table public.crdt_documents
    drop constraint crdt_documents_workspace_id_fkey,
    add constraint crdt_documents_workspace_id_fkey foreign key (workspace_id) references public.crdt_workspaces (id),
    drop constraint crdt_documents_owner_id_fkey,
    add constraint crdt_documents_owner_id_fkey foreign key (owner_id) references auth.users (id);

revoke delete on public.projects from anon, authenticated;
revoke delete on public.todos from anon, authenticated;
revoke delete on public.audit_logs from anon, authenticated;
revoke delete on public.public_templates from anon, authenticated;
revoke delete on public.shared_templates from anon, authenticated;
revoke delete on public.crdt_documents from anon, authenticated;
