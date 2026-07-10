# tinybase-supabase

Browser-only offline persistence and Supabase synchronization for TinyBase.

This package is under active development. It will provide durable IndexedDB
persistence, an offline outbox, direct Supabase CRUD synchronization, and
optional Supabase Realtime wake-and-reconcile subscriptions.

## Development

```sh
pnpm install
pnpm check
pnpm test
```

The package is formatted with Biome using four-space indentation, semicolons,
single quotes, and trailing commas.
