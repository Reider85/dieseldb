/**
 * pg-compatible `Client` for the DieselDB Node.js driver.
 *
 * API mirrors the `pg` package's Client surface:
 * `new Client(config)`, `await client.connect()`,
 * `await client.query(text, values?)`, `await client.end()`, with
 * `$1..$n` placeholders bound client-side (the DieselDB server's extended
 * protocol Parse/Bind/Execute is a stub).
 */

import { EventEmitter } from 'events';
import { WireConnection } from './connection';
import { DbError, InterfaceError } from './errors';

export interface ClientConfig {
  host?: string;
  port?: number;
  database?: string;
  /** pg alias for `database`. */
  dbname?: string;
  user?: string;
  password?: string;
  connectionTimeoutMillis?: number;
}

export interface FieldDef {
  name: string;
  dataTypeID: number;
  dataTypeSize: number | null;
  dataTypeModifier: number | null;
  format: string;
}

export interface QueryResult {
  command: string;
  rowCount: number | null;
  rows: Record<string, unknown>[];
  fields: FieldDef[];
  /** Raw command tag from the server (e.g. `SELECT 1`, `INSERT 0 1`). */
  commandComplete?: string;
}

export interface QueryConfig {
  text: string;
  values?: unknown[];
}

const OID_BOOL = 16;
const OID_INT8 = 20;
const OID_INT2 = 21;
const OID_INT4 = 23;
const OID_FLOAT4 = 700;
const OID_FLOAT8 = 701;

function decodeValue(text: string | null, oid: number): unknown {
  if (text === null) {
    return null;
  }
  if (oid === OID_INT2 || oid === OID_INT4 || oid === OID_INT8) {
    const n = Number(text);
    return Number.isNaN(n) ? text : n;
  }
  if (oid === OID_FLOAT4 || oid === OID_FLOAT8) {
    const n = Number(text);
    return Number.isNaN(n) ? text : n;
  }
  if (oid === OID_BOOL) {
    if (text === 't' || text === 'true' || text === 'TRUE' || text === '1') {
      return true;
    }
    if (text === 'f' || text === 'false' || text === 'FALSE' || text === '0') {
      return false;
    }
    return text;
  }
  return text;
}

/** Render a JS value as a SQL literal (client-side binding). */
export function quoteLiteral(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new DbError(`Cannot bind non-finite number: ${value}`, '22000');
    }
    return String(value);
  }
  if (typeof value === 'boolean') {
    // DieselDB stores booleans as integers in the engine's type system.
    return value ? '1' : '0';
  }
  if (value instanceof Date) {
    return `'${value.toISOString()}'`;
  }
  if (Buffer.isBuffer(value)) {
    return `'${value.toString('utf8').replace(/'/g, "''")}'`;
  }
  return `'${String(value).replace(/'/g, "''")}'`;
}

/** Substitute `$1..$n` placeholders (client-side, pg-style). */
export function bindParameters(sql: string, values: unknown[]): string {
  let count = 0;
  const bound = sql.replace(/\$(\d+)/g, (_match, digits: string) => {
    const idx = Number(digits);
    count = Math.max(count, idx);
    if (idx < 1 || idx > values.length) {
      throw new DbError(
        `Prepared statement has ${values.length} parameters, but $${idx} was supplied`,
        '08P01');
    }
    return quoteLiteral(values[idx - 1]);
  });
  if (count !== values.length) {
    throw new DbError(
      `Prepared statement has ${values.length} parameters, ` +
      `but only $1..$${count} were referenced`, '08P01');
  }
  return bound;
}

function rowsFromTag(tag: string | null): number | null {
  if (!tag) {
    return null;
  }
  const parts = tag.split(/\s+/);
  for (let i = parts.length - 1; i >= 0; i--) {
    const n = Number(parts[i]);
    if (!Number.isNaN(n) && parts[i].trim() !== '') {
      return n;
    }
  }
  return null;
}

export class Client extends EventEmitter {
  private config: ClientConfig;
  private wire: WireConnection | null = null;

  constructor(config: ClientConfig | string = {}) {
    super();
    if (typeof config === 'string') {
      this.config = parseConnectionString(config);
    } else {
      this.config = { ...config };
    }
  }

  /** True between a successful `connect()` and `end()`. */
  get connected(): boolean {
    return this.wire !== null && !this.wire.closed;
  }

  async connect(): Promise<void> {
    if (this.wire && !this.wire.closed) {
      throw new InterfaceError('Client is already connected');
    }
    const host = this.config.host ?? 'localhost';
    const port = this.config.port ?? 5432;
    const user = this.config.user ?? 'postgres';
    const database = this.config.database ?? this.config.dbname ?? 'postgres';
    const password = this.config.password ?? '';
    const timeout = this.config.connectionTimeoutMillis ?? 0;

    const wire = await WireConnection.connect(host, port, timeout);
    try {
      await wire.handshake(user, database, password);
    } catch (err) {
      await wire.closeGracefully().catch(() => undefined);
      throw err;
    }
    this.wire = wire;
    this.emit('connect');
  }

  async query(textOrConfig: string | QueryConfig,
              values?: unknown[]): Promise<QueryResult> {
    const wire = this.wire;
    if (!wire || wire.closed) {
      throw new InterfaceError('Client is not connected');
    }
    let sql: string;
    let params: unknown[] | undefined;
    if (typeof textOrConfig === 'string') {
      sql = textOrConfig;
      params = values;
    } else {
      sql = textOrConfig.text;
      params = textOrConfig.values;
    }
    if (params && params.length > 0) {
      sql = bindParameters(sql, params);
    }

    const raw = await wire.simpleQuery(sql);
    const fields: FieldDef[] = [];
    const rows: Record<string, unknown>[] = [];
    if (raw.columnNames) {
      for (let i = 0; i < raw.columnNames.length; i++) {
        fields.push({
          name: raw.columnNames[i],
          dataTypeID: raw.columnTypeOids ? raw.columnTypeOids[i] : 25,
          dataTypeSize: null,
          dataTypeModifier: null,
          format: 'text',
        });
      }
      for (const rawRow of raw.rows) {
        const row: Record<string, unknown> = {};
        for (let i = 0; i < fields.length; i++) {
          row[fields[i].name] = decodeValue(rawRow[i], fields[i].dataTypeID);
        }
        rows.push(row);
      }
    }
    const command = raw.commandTag ?? '';
    const rowCount = raw.columnNames
      ? rows.length
      : rowsFromTag(raw.commandTag);
    return { command, rowCount, rows, fields,
      commandComplete: raw.commandTag ?? undefined };
  }

  async end(): Promise<void> {
    const wire = this.wire;
    this.wire = null;
    if (wire) {
      await wire.closeGracefully();
    }
    this.emit('end');
  }
}

/** Parse a postgres:// style connection string (subset). */
export function parseConnectionString(connString: string): ClientConfig {
  const url = new URL(connString);
  const config: ClientConfig = {};
  if (url.hostname) {
    config.host = url.hostname;
  }
  if (url.port) {
    config.port = Number(url.port);
  }
  if (url.username) {
    config.user = decodeURIComponent(url.username);
  }
  if (url.password) {
    config.password = decodeURIComponent(url.password);
  }
  const db = url.pathname.replace(/^\//, '');
  if (db) {
    config.database = db;
  }
  return config;
}
