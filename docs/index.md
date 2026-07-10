---
layout: home

hero:
  name: tinybase-supabase
  text: Offline-first TinyBase synchronization for Supabase
  tagline: Durable browser state, a private outbox, direct CRUD, RLS, and optional Realtime reconciliation.
  actions:
    - theme: brand
      text: Get started
      link: /guide/getting-started
    - theme: alt
      text: Try the todo example
      link: /examples

features:
  - title: Local-first
    details: Persist TinyBase state and queued writes atomically in IndexedDB before attempting the network.
  - title: RLS-aware
    details: Use the standard Supabase browser client. Authentication and row-level security remain the authority.
  - title: Reconcile safely
    details: Realtime events wake authenticated pulls; complete pulls, reconnects, and focus events keep state convergent.
---

## Use it from any JavaScript project

The published package has both ESM and CommonJS exports. It does not require a
framework, server, service worker, or privileged Supabase credential.

```js
const {createSupabasePersister} = require('tinybase-supabase');
```
