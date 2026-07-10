-- The tests create their own authenticated users and rows. Keeping this file
-- allows `supabase db reset` to exercise the normal seed workflow.
insert into public.public_templates (id, title, metadata)
values ('public-template', 'Public template', '{"source":"no-rls"}');

insert into public.shared_templates (id, title, metadata)
values ('shared-template', 'Shared template', '{"source":"rls"}');
