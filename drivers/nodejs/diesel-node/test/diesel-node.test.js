/**
 * CRUD acceptance tests for the DieselDB Node.js driver (pg-compatible API).
 *
 * The JUnit harness (NodeTest) starts an in-process LibpqServer and passes
 * the port via DIESEL_PORT; defaults keep the tests runnable by hand against
 * a local server on 5432.
 */

const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { Client, DbError } = require('../dist');

const HOST = process.env.DIESEL_HOST || 'localhost';
const PORT = Number(process.env.DIESEL_PORT || 5432);
const USER = process.env.DIESEL_USER || 'testuser';
const PASSWORD = process.env.DIESEL_PASSWORD || 'testpass';
const DATABASE = process.env.DIESEL_DB || 'testdb';

/** @type {Client} */
let client;

async function dropIfExists(table) {
  try {
    await client.query(`DROP TABLE ${table}`);
  } catch (err) {
    if (!(err instanceof DbError)) {
      throw err;
    }
  }
}

before(async () => {
  client = new Client({
    host: HOST, port: PORT, database: DATABASE,
    user: USER, password: PASSWORD,
  });
  await client.connect();
  await dropIfExists('node_demo');
});

after(async () => {
  if (client) {
    await dropIfExists('node_demo');
    await client.end();
  }
});

test('connect and basic SELECT', async () => {
  const res = await client.query('SELECT 1');
  assert.equal(res.rows.length, 1);
  assert.equal(Object.values(res.rows[0])[0], 1);
  assert.ok(client.connected);
});

test('connection string constructor', async () => {
  const c = new Client(
    `postgres://${USER}:${PASSWORD}@${HOST}:${PORT}/${DATABASE}`);
  await c.connect();
  try {
    const res = await c.query('SELECT 1');
    assert.equal(res.rows.length, 1);
  } finally {
    await c.end();
  }
  assert.ok(!c.connected);
});

test('full CRUD lifecycle with $n parameters', async () => {
  await client.query(
    'CREATE TABLE node_demo (ID INTEGER, NAME STRING, AGE INTEGER)');

  await client.query(
    'INSERT INTO node_demo (ID, NAME, AGE) VALUES ($1, $2, $3)',
    [1, 'Alice', 25]);
  await client.query(
    'INSERT INTO node_demo (ID, NAME, AGE) VALUES ($1, $2, $3)',
    [2, 'Bob', 30]);

  const all = await client.query('SELECT ID, NAME, AGE FROM node_demo');
  assert.equal(all.rows.length, 2);
  assert.deepEqual(all.fields.map((f) => f.name), ['ID', 'NAME', 'AGE']);
  assert.deepEqual(all.rows[0], { ID: 1, NAME: 'Alice', AGE: 25 });
  assert.deepEqual(all.rows[1], { ID: 2, NAME: 'Bob', AGE: 30 });

  const filtered = await client.query(
    'SELECT NAME FROM node_demo WHERE ID = $1', [2]);
  assert.deepEqual(filtered.rows, [{ NAME: 'Bob' }]);

  await client.query(
    'UPDATE node_demo SET AGE = $1 WHERE ID = $2', [26, 1]);
  const updated = await client.query(
    'SELECT AGE FROM node_demo WHERE ID = 1');
  assert.deepEqual(updated.rows, [{ AGE: 26 }]);

  await client.query('DELETE FROM node_demo WHERE ID = 1');
  const remaining = await client.query('SELECT ID FROM node_demo');
  assert.equal(remaining.rows.length, 1);
  assert.equal(remaining.rows[0].ID, 2);
});

test('string escaping in parameters', async () => {
  await client.query(
    'INSERT INTO node_demo (ID, NAME, AGE) VALUES ($1, $2, $3)',
    [7, "O'Brien", 40]);
  const res = await client.query(
    'SELECT NAME FROM node_demo WHERE ID = $1', [7]);
  assert.deepEqual(res.rows, [{ NAME: "O'Brien" }]);
});

test('null parameter', async () => {
  await client.query(
    'INSERT INTO node_demo (ID, NAME, AGE) VALUES ($1, $2, $3)',
    [8, null, null]);
  const res = await client.query(
    'SELECT ID, NAME, AGE FROM node_demo WHERE ID = $1', [8]);
  assert.deepEqual(res.rows, [{ ID: 8, NAME: null, AGE: null }]);
});

test('error surfaces as DbError and connection stays usable', async () => {
  await assert.rejects(
    () => client.query('SELECT * FROM table_that_does_not_exist'),
    (err) => err instanceof DbError && err.message.length > 0);
  const res = await client.query('SELECT 1');
  assert.equal(res.rows.length, 1);
});

test('query with config object', async () => {
  const res = await client.query({
    text: 'SELECT NAME FROM node_demo WHERE ID = $1',
    values: [2],
  });
  assert.deepEqual(res.rows, [{ NAME: 'Bob' }]);
});

test('query on ended client rejects', async () => {
  const c = new Client({ host: HOST, port: PORT, database: DATABASE,
    user: USER, password: PASSWORD });
  await c.connect();
  await c.end();
  await assert.rejects(() => c.query('SELECT 1'), DbError);
});

test('arity mismatch binds error', async () => {
  await assert.rejects(
    () => client.query('SELECT $1, $2', [1]),
    (err) => err instanceof DbError);
  await assert.rejects(
    () => client.query('SELECT $1', [1, 2]),
    (err) => err instanceof DbError);
});
