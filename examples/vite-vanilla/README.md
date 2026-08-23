# vite-vanilla (self-host replay)

Port of instant-v1's `examples/vite-vanilla` todo app, pointed at a local
instantd.

## Run

```sh
pnpm install
VITE_INSTANT_APP_ID=<app-uuid> \
VITE_INSTANT_API_URI=http://127.0.0.1:18891 \
VITE_INSTANT_WS_URI=ws://127.0.0.1:18891/runtime/session \
pnpm dev
```

## Conformance status

Verified against instantd with the frozen `@instantdb/core` **1.0.65** wheel
(the exact code the browser bundle runs):

- `subscribeQuery` initial snapshot (rides the enriched `add-query-ok`)
- `db.transact(db.tx.todos[id].update(...))` — add / toggle / delete
- LIVE pushes for every mutation (WS refresh-ok)

Note: `@instantdb/core` >= 1.0 names the socket override `websocketURI`;
passing `wsURI` is silently ignored and the SDK dials the hosted platform.
This example maps `VITE_INSTANT_WS_URI` → `websocketURI`.
