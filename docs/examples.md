# Interactive todo example

This framework-free example imports the package source built into this docs site.
It is intentionally unconfigured: supply a **disposable** Supabase project URL,
publishable key, and test-account credentials. It never stores a credential in
the documentation build.

The local repository fixture provides `projects` and `todos` tables. Start it
with `supabase start`, create an account, copy the publishable key from
`supabase status`, then connect below.

<TodoDemo />
