// Initialize the database

import { init } from "@instantdb/core";
import schema from "../instant.schema";

// ---------
// Self-host: point at a local instantd with VITE_INSTANT_API_URI /
// VITE_INSTANT_WS_URI (platform defaults when unset). Note @instantdb/core
// >=1.0 names the socket override `websocketURI`; `wsURI` is silently
// ignored and the SDK would dial the hosted platform instead.
const apiURI = import.meta.env.VITE_INSTANT_API_URI || "https://api.instantdb.com";
const websocketURI = import.meta.env.VITE_INSTANT_WS_URI || "wss://api.instantdb.com/runtime/session";

export const db = init({
  appId: import.meta.env.VITE_INSTANT_APP_ID,
  apiURI,
  websocketURI,
  schema,
  useDateObjects: true,
});
