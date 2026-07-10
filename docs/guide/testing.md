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
