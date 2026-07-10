# Collaborative CRDT cells

`createSupabasePersister` supports collaborative edits within selected cells.
The factory uses one Yjs document per TinyBase row. Configured
`text`, `map`, and `array` cells are Yjs shared types; every other cell uses the
ordinary whole-row Supabase synchronization path.

CRDT support is entirely optional. A table may omit `crdtCells` or set it to an
empty object. If every table does so, the factory does not create Yjs documents,
open CRDT IndexedDB state, subscribe to an updates table, or require any CRDT
schema. A single configuration can contain ordinary tables, CRDT-enabled
tables, and mixed rows with both ordinary and collaborative cells.

CRDT-enabled tables cannot use `mode: 'read-only'`, because Yjs shared types are
mutable handles. Enforce viewer/editor distinctions with updates-table RLS;
unauthorized local edits remain durable rejected operations that can be retried
or discarded explicitly.

Keep IDs, owners, tenant IDs, foreign keys, and roles out of Yjs. Those fields
belong on the parent row so Postgres can enforce relational integrity and RLS.

```ts
const persister = await createSupabasePersister(store, {
	crdtUpdateBufferMs: 500,
	databaseName: 'notes',
	scopeKey: user.id,
	supabase,
	tables: {
		documents: {
			crdtCells: {
				body: {type: 'text'},
				blocks: {type: 'array'},
				properties: {type: 'map'},
			},
			crdtRowIdColumn: 'document_id',
			crdtUpdatesTable: 'document_yjs_updates',
			table: 'documents',
		},
	},
});

await persister.startAutoPersisting();
const document = await persister.openRow('documents', documentId);
document.getText('body').insert(0, 'Collaborative text');
document.getArray('blocks').push([{type: 'paragraph'}]);
document.getMap('properties').set('theme', 'paper');

// Ordinary metadata still uses TinyBase writes.
store.setCell('documents', documentId, 'status', 'published');
```

The projected TinyBase cells are read-only. Edit them through the Yjs handles;
TinyBase receives reactive detached projections: strings for `text`, arrays for
`array`, and objects for `map`. CRDT cells are absent until `openRow` finishes.

Each local Yjs update is persisted to IndexedDB immediately. The persister then
collects updates for each row during `crdtUpdateBufferMs` (500 ms by default),
merges them with `Y.mergeUpdates`, and uploads one append-only update row. The
window begins with the first buffered update, so continuous editing does not
postpone synchronization indefinitely. Different documents are compacted
independently. `syncNow()` flushes the current buffer immediately, and setting
`crdtUpdateBufferMs: 0` disables the delay while retaining the same durable
path.

With `realtime` enabled, both parent-row and updates-table notifications feed a
single debounced reconciliation scheduler. Polling, reconnect, tab visibility,
and `syncNow()` use that same path, so a missed notification is recovered by a
later safety synchronization.

Locally cached Yjs updates can be opened and edited when an authoritative pull
fails transiently; the shared scheduler reports the offline state and retries.
Permanent authorization failures still reject `openRow`. For a full page reload
to work without a network connection, the application must also precache its
JavaScript assets, including any bundler chunk containing the optional CRDT
implementation, as part of its normal offline/PWA setup.

## Required Supabase tables

The parent table is application-owned. This example uses text IDs, but
`document_id` must have exactly the same type as the parent key.

```sql
create table public.documents (
	id text primary key,
	owner_id uuid not null references auth.users (id),
	workspace_id text not null references public.workspaces (id),
	status text not null default 'draft',
	deleted_at timestamptz
);

create table public.document_yjs_updates (
	id uuid primary key,
	document_id text not null
		references public.documents (id)
		on delete cascade,
	update bytea not null,
	created_at timestamptz not null default now()
);

create index document_yjs_updates_document_order_idx
	on public.document_yjs_updates (document_id, created_at, id);

alter table public.document_yjs_updates enable row level security;
revoke all on public.document_yjs_updates from anon, authenticated;
grant select, insert on public.document_yjs_updates to authenticated;
alter publication supabase_realtime add table public.document_yjs_updates;
```

The column contract is strict:

- `id` is a durable update UUID used for idempotent retries.
- The configured `crdtRowIdColumn` is a foreign key to the parent row.
- `update` is the complete binary Y.Doc update in Postgres `bytea`. The
  persister converts between Supabase's hexadecimal `bytea` representation and
  `Uint8Array`.
- `created_at` plus `id` provides deterministic pagination. Yjs correctness
  does not depend on application order.

The table is append-only. Client-side buffering reduces the number of newly
created rows, but this version still replays the full server history and does
not require snapshots or server-side compaction.

A configured CRDT type is permanent for that cell name. Changing `text` to
`map` or `array`, or enabling CRDT for a populated ordinary column, requires an
application migration. Seed the existing value into the corresponding Yjs type
before removing the ordinary parent column; otherwise the old value will not
appear in the CRDT projection.

## RLS through a many-to-one relationship

Use separate read and write policies. This lets workspace viewers hydrate the
document while only editors can append changes.

```sql
create policy "workspace members read document updates"
on public.document_yjs_updates for select to authenticated
using (exists (
	select 1
	from public.documents d
	join public.workspace_members wm on wm.workspace_id = d.workspace_id
	where d.id = document_yjs_updates.document_id
		and wm.user_id = (select auth.uid())
		and d.deleted_at is null
));

create policy "workspace editors write document updates"
on public.document_yjs_updates for insert to authenticated
with check (exists (
	select 1
	from public.documents d
	join public.workspace_members wm on wm.workspace_id = d.workspace_id
	where d.id = document_yjs_updates.document_id
		and wm.user_id = (select auth.uid())
		and wm.role in ('owner', 'editor')
		and d.deleted_at is null
));
```

## RLS through a many-to-many relationship

For per-document collaboration, authorize through the join table instead.

```sql
create policy "collaborators read document updates"
on public.document_yjs_updates for select to authenticated
using (exists (
	select 1
	from public.document_collaborators dc
	join public.documents d on d.id = dc.document_id
	where dc.document_id = document_yjs_updates.document_id
		and dc.user_id = (select auth.uid())
		and d.deleted_at is null
));

create policy "collaborating editors write document updates"
on public.document_yjs_updates for insert to authenticated
with check (exists (
	select 1
	from public.document_collaborators dc
	join public.documents d on d.id = dc.document_id
	where dc.document_id = document_yjs_updates.document_id
		and dc.user_id = (select auth.uid())
		and dc.role in ('owner', 'editor')
		and d.deleted_at is null
));
```

Complex schemas may prefer `security definer` authorization functions to avoid
recursive RLS between parent and membership policies. Set an empty
`search_path`, fully qualify every relation, derive the caller from `auth.uid()`
inside the function, and grant only `execute` to the roles that need it. Do not
accept an arbitrary user ID from the caller.

Revoking access cannot erase data already cached on a device, but it prevents
future reads and inserts. RLS is enforced on authoritative pulls and writes;
Realtime is only a wake-up signal.
