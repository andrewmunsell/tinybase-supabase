# Testing locally

The repository includes a complete Supabase CLI fixture and Jest tests for
unit, real HTTP/RLS integration, and Chromium browser behavior.

```sh
pnpm install
supabase start
eval "$(supabase status -o env)"
export SUPABASE_TEST_URL="$API_URL"
export SUPABASE_TEST_ANON_KEY="$ANON_KEY"

pnpm test:unit
pnpm test:integration
pnpm test:e2e
```

The interactive example uses the same fixture at `http://127.0.0.1:55421`.
Create a disposable local account in Supabase Studio or with the Auth API before
signing in.

The browser suite includes a read-only CRDT reader that hydrates existing
content, follows a writer over Realtime, attempts a local edit, verifies that no
update is uploaded, and restarts from authoritative remote content. It also
covers RLS rejection, durable document quarantine across restart, and discard
back to authoritative CRDT content.
