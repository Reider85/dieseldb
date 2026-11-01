/**
 * DieselDB Node.js driver (pg-compatible API).
 *
 * Quick start:
 *
 * ```js
 * const { Client } = require('diesel-node');
 *
 * const client = new Client({
 *   host: 'localhost', port: 5432, database: 'testdb',
 *   user: 'alice', password: 'secret',
 * });
 * await client.connect();
 * await client.query('CREATE TABLE demo (ID INTEGER, NAME STRING)');
 * await client.query('INSERT INTO demo (ID, NAME) VALUES ($1, $2)',
 *                    [1, 'Alice']);
 * const res = await client.query('SELECT ID, NAME FROM demo WHERE ID = $1', [1]);
 * console.log(res.rows); // [{ ID: 1, NAME: 'Alice' }]
 * await client.end();
 * ```
 */

export { Client, parseConnectionString, quoteLiteral, bindParameters } from './client';
export type { ClientConfig, FieldDef, QueryConfig, QueryResult } from './client';
export { DbError, InterfaceError, NotSupportedError } from './errors';
export { WireConnection } from './connection';
export type { FieldDescription, ParsedMessage, RawQueryResult } from './connection';

export const version = '0.1.0';
