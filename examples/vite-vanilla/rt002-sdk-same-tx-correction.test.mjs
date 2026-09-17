// RT-002 task F — same-transaction correction in the REAL Instant SDK.
//
// Frozen dependency: @instantdb/core 1.0.65 (see examples/vite-vanilla/pnpm-lock.yaml).
// Run: `node --test rt002-sdk-same-tx-correction.test.mjs` from examples/vite-vanilla.
//
// What this proves (delivery semantics: still-attached member receives a
// same-tx corrective frame when bounded retry repairs a partial fan-out):
//   1. A stale frame for tx T is delivered and observed (title "old").
//   2. A CORRECTIVE frame for the SAME tx T is delivered (title "new").
//   3. The SDK applies the corrective instead of dropping it as a duplicate
//      (observable query data becomes the corrected state, NOT the stale one,
//      while processedTxId stays T — i.e. no txID-equality dedupe).
//   4. A later tx T+1 still notifies (subscription remains live).
//
// REAL SDK PATH (not a handwritten collector):
//   - `Reactor` is imported from node_modules/@instantdb/core (frozen 1.0.65).
//   - Frames are fed through the production message handler
//     `reactor._handleReceive(connId, msg)` — the same entry point the live
//     websocket transport calls (`_transportOnMessage` -> `_handleReceive`).
//   - State flows through production `querySubs` (PersistedObject),
//     production `dataForQuery` (instaql over the real triple store), and
//     production `notifyOne` subscriber fan-out.
//   - In @instantdb/core 1.0.65, the `refresh-ok` case in
//     dist/esm/Reactor.js `_handleReceive` unconditionally overwrites
//     `querySubs[hash].result` with the new store + processedTxId and calls
//     `notifyOne(hash)`; there is no `processedTx-id` equality guard (the only
//     dedupe is `notifyOne`'s data-level `areObjectsDeepEqual` check, which a
//     corrective frame with changed data passes). There is no
//     `refresh-ok-delta` op in 1.0.65.
//   - If the SDK ever drops the same-tx corrective (state stays stale), this
//     test FAILS by design — the oracle must not be weakened; RT-002 stays
//     PARTIAL and the incompatibility is reported.
//
// Test-only shims (documented, minimal — none touch the reducer path):
//   - `globalThis.window = {}` set BEFORE importing the SDK: the Reactor
//     constructor early-returns on the server (`if (!isClient()) return`),
//     which would leave `querySubs` unset. A bare object (deliberately WITHOUT
//     localStorage) also keeps `utils/flags.js` on its guarded path. Node 25's
//     built-in localStorage has no sync getItem, so `window = globalThis`
//     crashes flags.js — hence the bare object.
//   - `InMemoryStorage` (shipped inside @instantdb/core) passed as the Reactor
//     `Storage` param instead of IndexedDBStorage (no browser IndexedDB here).
//   - `NeverOnline` network listener: `getIsOnline()` never resolves, so the
//     Reactor never opens a real socket during the test (no network).
//   - `reactor._transport` stub (`isOpen() === false`): neutralizes only the
//     SEND side (`_trySend` checks `isOpen()` first and returns). Receives go
//     through the untouched production `_handleReceive` path.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// ---- shim BEFORE any SDK module is evaluated (see header) ----
globalThis.window = {};

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SDK_ESM = path.join(HERE, 'node_modules', '@instantdb', 'core', 'dist', 'esm');
const [{ default: Reactor }, { default: InMemoryStorage }, { default: weakHash }] =
  await Promise.all([
    import(path.join(SDK_ESM, 'Reactor.js')),
    import(path.join(SDK_ESM, 'InMemoryStorage.js')),
    import(path.join(SDK_ESM, 'utils', 'weakHash.js')),
  ]);

// ---- frozen-dependency evidence ----
const sdkPkg = JSON.parse(
  fs.readFileSync(path.join(HERE, 'node_modules', '@instantdb', 'core', 'package.json'), 'utf8'),
);
const SDK_VERSION = sdkPkg.version;
const LOCK_PATH = path.join(HERE, 'pnpm-lock.yaml');
const LOCK_SHA256 = crypto.createHash('sha256').update(fs.readFileSync(LOCK_PATH)).digest('hex');
console.log(`RT-002/F pinned SDK: @instantdb/core@${SDK_VERSION}`);
console.log(`RT-002/F lockfile: pnpm-lock.yaml sha256=${LOCK_SHA256}`);

const EXPECTED_SDK_VERSION = '1.0.65';
const TX_STALE = 7;
const TX_NEXT = 8;
const E1 = 'e1-todo-1';
const E2 = 'e2-todo-2';
const ID_ATTR = 'id-attr-todos';
const TITLE_ATTR = 'title-attr-todos';

const ATTRS = [
  {
    id: ID_ATTR,
    'forward-identity': ['fwd-todos-id', 'todos', 'id'],
    'value-type': 'blob',
    cardinality: 'one',
    'checked-data-type': 'string',
  },
  {
    id: TITLE_ATTR,
    'forward-identity': ['fwd-todos-title', 'todos', 'title'],
    'value-type': 'blob',
    cardinality: 'one',
    'checked-data-type': 'string',
  },
];

// Minimal production-shaped instaql-result: idNodes whose datalog-result
// join-rows carry [entity-id, attr-id, value] triples (the exact shape
// `extractTriples` in the SDK consumes for both add-query-ok and refresh-ok).
function resultNode(pairs) {
  const triples = [];
  for (const [eid, title] of pairs) {
    triples.push([eid, ID_ATTR, eid]);
    triples.push([eid, TITLE_ATTR, title]);
  }
  return [
    { data: { 'datalog-result': { 'join-rows': [triples] } }, 'child-nodes': [] },
  ];
}

test('real SDK applies same-tx corrective refresh-ok (RT-002/F)', () => {
  assert.equal(SDK_VERSION, EXPECTED_SDK_VERSION, 'SDK must be the frozen pinned version');

  const reactor = new Reactor(
    {
      appId: '123e4567-e89b-12d3-a456-426614174000',
      apiURI: 'http://127.0.0.1:1',
      websocketURI: 'ws://127.0.0.1:1/runtime/session',
      disableValidation: true,
    },
    InMemoryStorage,
    { getIsOnline: () => new Promise(() => {}), listen: () => () => {} },
  );
  // Send-side stub only; the receive/reducer path under test is untouched.
  reactor._transport = { id: 'test-stub', isOpen: () => false, send: () => {} };

  try {
    const Q = { todos: {} };
    const hash = weakHash(Q);
    const seen = [];
    const unsub = reactor.subscribeQuery(Q, (data) => {
      seen.push(JSON.parse(JSON.stringify(data)));
    });
    try {
      // Establish session state (production init-ok handler: sets attrs,
      // flushes pending messages which initializes kv.pendingMutations so
      // dataForQuery can serve results, marks AUTHENTICATED).
      reactor._handleReceive('conn-1', { op: 'init-ok', attrs: ATTRS, 'session-id': 's1' });
      assert.equal(reactor.status, 'authenticated');

      // Step 1: stale frame for tx T reaches the still-attached member.
      reactor._handleReceive('conn-1', {
        op: 'add-query-ok',
        q: Q,
        result: resultNode([[E1, 'old']]),
        'processed-tx-id': TX_STALE,
      });
      const STALE = { data: { todos: [{ id: E1, title: 'old' }] } };
      assert.equal(seen.length, 1, 'stale frame must notify the attached subscriber');
      assert.deepEqual(seen[0], STALE);

      // Step 2: corrective frame for the SAME tx T (bounded-retry repair).
      reactor._handleReceive('conn-1', {
        op: 'refresh-ok',
        computations: [{ 'instaql-query': Q, 'instaql-result': resultNode([[E1, 'new']]) }],
        'processed-tx-id': TX_STALE,
      });
      const CORRECTED = { data: { todos: [{ id: E1, title: 'new' }] } };
      assert.equal(
        seen.length,
        2,
        'same-tx corrective must notify (SDK must not dedupe on txID equality)',
      );
      // Oracle: observable state EQUALS corrected server state, NOT stale.
      assert.deepEqual(seen[1], CORRECTED);
      assert.notDeepEqual(seen[1], STALE);
      // Correction applied AT the same tx: processedTxId is unchanged.
      assert.equal(
        reactor.querySubs.currentValue[hash].result.processedTxId,
        TX_STALE,
        'corrective applied at equal txID, not via a newer tx',
      );
      // Observable query state via the production read path agrees.
      assert.deepEqual(reactor.dataForQuery(hash)?.data, CORRECTED);
      console.log(`RT-002/F corrective applied at equal tx ${TX_STALE}: ${JSON.stringify(seen[1])}`);

      // Step 3: tx T+1 proves the subscription is still live.
      reactor._handleReceive('conn-1', {
        op: 'refresh-ok',
        computations: [
          {
            'instaql-query': Q,
            'instaql-result': resultNode([
              [E1, 'new'],
              [E2, 'second'],
            ]),
          },
        ],
        'processed-tx-id': TX_NEXT,
      });
      const NEXT = {
        data: { todos: [{ id: E1, title: 'new' }, { id: E2, title: 'second' }] },
      };
      assert.equal(seen.length, 3, 'tx T+1 must still notify (subscription live)');
      assert.deepEqual(seen[2], NEXT);
      assert.deepEqual(reactor.dataForQuery(hash)?.data, NEXT);
      console.log(`RT-002/F live after tx ${TX_NEXT}: ${JSON.stringify(seen[2])}`);
    } finally {
      unsub();
    }
  } finally {
    try {
      reactor._broadcastChannel?.close();
    } catch {}
    // Teardown hygiene ONLY (assertions already done): production
    // PersistedObject.gc() schedules a ~60s setTimeout after persists, which
    // would keep `node --test` waiting on an idle handle. Clear pending
    // persist/GC timers so the runner exits promptly. This touches nothing
    // on the reducer/oracle path.
    for (const po of [reactor.querySubs, reactor.kv]) {
      for (const k of ['_nextGc', '_nextSave']) {
        try {
          if (po?.[k]) clearTimeout(po[k]);
        } catch {}
      }
    }
    try {
      reactor.shutdown();
    } catch {}
  }
});
