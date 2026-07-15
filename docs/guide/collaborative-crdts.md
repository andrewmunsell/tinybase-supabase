# Collaborative CRDT cells

`createSupabasePersister` supports collaborative edits within selected cells.
The factory uses one Yjs document per TinyBase row. Configured
`text`, `map`, `array`, and `xml-fragment` cells are Yjs shared types; every
other cell uses the ordinary whole-row Supabase synchronization path.

CRDT support is entirely optional. A table may omit `crdtCells` or set it to an
empty object. If every table does so, the factory does not create Yjs documents,
open CRDT IndexedDB state, subscribe to an updates table, or require any CRDT
schema. A single configuration can contain ordinary tables, CRDT-enabled
tables, and mixed rows with both ordinary and collaborative cells.

CRDT-enabled tables may use `mode: 'read-only'`. Their rows still fetch existing
updates, subscribe to new updates, and project Yjs values into TinyBase, while
local mutation attempts through the Yjs handles throw before changing the
document. No local Yjs update is queued, persisted, or uploaded.

For mixed-access tables, keep the mapping read-write, control editing in the
application, and enforce viewer/editor distinctions with parent- and
updates-table RLS. If RLS rejects a CRDT update, the document's outbound history
is quarantined so dependent updates remain local until the rejection is retried
or discarded.

Keep IDs, owners, tenant IDs, foreign keys, and roles out of Yjs. Those fields
belong on the parent row so Postgres can enforce relational integrity and RLS.

```ts
import * as Y from 'yjs';

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
				structuredBody: {type: 'xml-fragment'},
			},
			crdtRowIdColumn: 'document_id',
			crdtUpdatesTable: 'document_yjs_updates',
			table: 'documents',
			updatedAtColumn: 'updated_at',
		},
	},
});

await persister.startAutoPersisting();
// The ordinary parent row must already exist in TinyBase before it can be opened.
const document = await persister.openRow('documents', documentId);
document.getText('body').insert(0, 'Collaborative text');
document.getArray('blocks').push([{type: 'paragraph'}]);
document.getMap('properties').set('theme', 'paper');
const paragraph = new Y.XmlElement('p');
paragraph.insert(0, [new Y.XmlText('Structured content')]);
document.getXmlFragment('structuredBody').insert(0, [paragraph]);

// Ordinary metadata still uses TinyBase writes.
store.setCell('documents', documentId, 'status', 'published');
```

The projected TinyBase cells are read-only. Edit them through the Yjs handles;
TinyBase receives reactive detached projections: strings for `text`, serialized
XML strings for `xml-fragment`, arrays for `array`, and objects for `map`. CRDT
cells are absent until `openRow` finishes. The XML projection is intended for
reactive display and indexing; make edits through the live `Y.XmlFragment`
returned by `getXmlFragment`.

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
and `syncNow()` use that same path, so a missed notification can be recovered by
a later cursor pull. If `updatedAtColumn` is omitted, parent-row reconciliation
uses full pulls instead.

Locally cached Yjs updates can be opened and edited when an authoritative pull
fails transiently; the shared scheduler reports the offline state and retries.
Explicit authorization errors reject `openRow`, while RLS-filtered empty results
cannot revoke data already cached locally; see the operational gotchas below.
For a full page reload to work without a network connection, the application
must also precache its JavaScript assets, including any bundler chunk containing
the optional CRDT implementation, as part of its normal offline/PWA setup.

## Required Supabase tables

The parent table is application-owned. This example uses text IDs, but
`document_id` must have exactly the same type as the parent key.

```sql
create table public.documents (
	id text primary key,
	owner_id uuid not null references auth.users (id),
	workspace_id text not null references public.workspaces (id),
	status text not null default 'draft',
	deleted_at timestamptz,
	updated_at timestamptz not null default clock_timestamp()
);

create index documents_sync_cursor_idx
	on public.documents (updated_at, id);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
	new.updated_at = clock_timestamp();
	return new;
end;
$$;

create trigger documents_set_updated_at
	before insert or update on public.documents
	for each row execute function public.set_updated_at();

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
grant select on public.document_yjs_updates to authenticated;
grant insert (id, document_id, update) on public.document_yjs_updates to authenticated;
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

A configured CRDT type is permanent for that cell name. Changing a configured
type (`text`, `map`, `array`, or `xml-fragment`), or enabling CRDT for a
populated ordinary column, requires an application migration. Seed the existing
value into the corresponding Yjs type before removing the ordinary parent
column; otherwise the old value will not appear in the CRDT projection.

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

## Operational gotchas

- Use one active persister for each `databaseName` and `scopeKey` in a browser
  storage partition. The IndexedDB outboxes do not use a cross-tab leader lock;
  concurrent instances sharing the same database can create equivalent upload
  envelopes and report stale pending counts. Coordinate tabs at the application
  layer or give intentionally isolated instances different database names.
- Call `row.destroy()` or `persister.closeRow(tableId, rowId)` when a document is
  unmounted or deleted. `persister.destroy()` closes every remaining row.
  Closing releases the Y.Doc and its row-filtered Realtime channel.
- A rejected CRDT update quarantines the complete document while continuing to
  capture optimistic local edits. `retryRejected()` merges and uploads the
  rejected update with every held successor. `discardRejected()` abandons all
  unaccepted local updates, closes each affected CRDT row, and removes its
  projection; call `openRow()` again to restore accepted cached and remote state.
- Grant updates-table `insert` only to users trusted to submit valid Yjs binary
  updates. An authorized client can bypass this package and insert malformed or
  excessively large `bytea`, causing hydration failures or resource exhaustion.
  Applications with untrusted writers should validate and rate-limit updates in
  a controlled server endpoint instead of granting direct table inserts.
- RLS commonly hides unauthorized rows by returning an empty result rather than
  an error. Revoking access cannot erase CRDT content already cached on a device,
  and a cached row may remain locally readable. Revocation prevents future
  authoritative reads and inserts; rejected local writes remain visible through
  `getRejectedOperations()`.
- The 500 ms buffer and per-document outbox coalescing reduce new rows but are
  not historical compaction. Uploaded envelopes remain append-only. Long-running
  deployments still need a separate future retention or snapshot strategy if
  full-history replay becomes too large.

RLS is enforced on authoritative pulls and writes; Realtime is only a wake-up
signal.
