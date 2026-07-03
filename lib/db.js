'use strict';

const { createClient } = require('@libsql/client');

let _client = null;

function getClient() {
  if (!_client) {
    const url       = process.env.TURSO_DATABASE_URL;
    const authToken = process.env.TURSO_AUTH_TOKEN;
    if (!url) throw new Error('TURSO_DATABASE_URL not set');
    _client = createClient({ url, authToken: authToken || undefined });
  }
  return _client;
}

// Convert a libsql Row (array-like with named keys) to a plain JS object.
// Needed so res.json() serialises rows as objects, not arrays.
function toObj(row, columns) {
  if (!row) return null;
  const obj = {};
  columns.forEach((col, i) => { obj[col] = row[i] ?? null; });
  return obj;
}

// SELECT → array of plain objects
async function query(sql, args = []) {
  const result = await getClient().execute({ sql, args });
  const cols = result.columns;
  return result.rows.map(r => toObj(r, cols));
}

// SELECT → first row or null
async function queryOne(sql, args = []) {
  const result = await getClient().execute({ sql, args });
  if (!result.rows.length) return null;
  return toObj(result.rows[0], result.columns);
}

// INSERT / UPDATE / DELETE → { changes, lastInsertRowid }
async function run(sql, args = []) {
  const result = await getClient().execute({ sql, args });
  return { changes: result.rowsAffected, lastInsertRowid: result.lastInsertRowid };
}

// DDL / raw exec (no return value needed)
async function exec(sql) {
  await getClient().execute(sql);
}

// Batch of statements in a single HTTP round-trip
// statements: [{ sql, args }]
async function batch(statements, mode = 'write') {
  return getClient().batch(statements, mode);
}

module.exports = { query, queryOne, run, exec, batch, getClient };
