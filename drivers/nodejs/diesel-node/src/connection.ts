/**
 * Minimal libpq wire-protocol client for the DieselDB Node.js driver.
 *
 * Speaks the subset implemented by `diesel.LibpqServer`:
 * startup handshake (DieselDB-style leading type byte `0` - the server's
 * `LibpqClientHandler.readMessage` expects a 5-byte header on the startup
 * message too, unlike stock PostgreSQL), trust/cleartext-password auth,
 * simple-query protocol, and response decoding (RowDescription, DataRow,
 * CommandComplete, ErrorResponse, NoticeResponse, ParameterStatus,
 * ReadyForQuery).
 *
 * The server's extended protocol (Parse/Bind/Execute) is a stub executing a
 * hardcoded `SELECT 1`, so this driver only uses simple queries with
 * client-side parameter substitution (mirrors the DieselDB JDBC driver).
 */

import * as net from 'net';
import { DbError, InterfaceError, NotSupportedError } from './errors';

const AUTH_REQUEST = 0x52; // 'R'
const PARAMETER_STATUS = 0x53; // 'S'
const READY_FOR_QUERY = 0x5a; // 'Z'
const ROW_DESCRIPTION = 0x54; // 'T'
const DATA_ROW = 0x44; // 'D'
const COMMAND_COMPLETE = 0x43; // 'C'
const ERROR_RESPONSE = 0x45; // 'E'
const NOTICE_RESPONSE = 0x4e; // 'N'
const EMPTY_QUERY_RESPONSE = 0x49; // 'I'

const STARTUP = 0x30; // '0' - DieselDB-style startup framing
const SIMPLE_QUERY = 0x51; // 'Q'
const PASSWORD_MESSAGE = 0x70; // 'p'
const TERMINATE = 0x58; // 'X'

const PROTOCOL_VERSION_3 = 196608;

export interface ParsedMessage {
  type: number;
  payload: Buffer;
}

export interface FieldDescription {
  name: string;
  typeOid: number;
}

export interface RawQueryResult {
  columnNames: string[] | null;
  columnTypeOids: number[] | null;
  rows: (string | null)[][];
  commandTag: string | null;
}

function readCString(buf: Buffer, offset: number): { value: string; next: number } {
  let end = offset;
  while (end < buf.length && buf[end] !== 0) {
    end++;
  }
  return { value: buf.toString('utf8', offset, end), next: end + 1 };
}

function parseErrorFields(payload: Buffer): DbError {
  let severity: string | undefined;
  let sqlstate: string | undefined;
  let message: string | undefined;
  let first: string | undefined;
  let i = 0;
  while (i < payload.length) {
    const fieldType = payload[i];
    i++;
    if (fieldType === 0) {
      break;
    }
    const { value, next } = readCString(payload, i);
    i = next;
    if (first === undefined) {
      first = value;
    }
    if (fieldType === 0x53) {
      severity = value; // 'S'
    } else if (fieldType === 0x56) {
      severity = value; // 'V' nonlocalized severity, prefer if present
    } else if (fieldType === 0x43) {
      sqlstate = value; // 'C'
    } else if (fieldType === 0x4d) {
      message = value; // 'M'
    }
  }
  return new DbError(message ?? first ?? 'unknown error', sqlstate ?? '00000',
    severity ?? 'ERROR');
}

function parseRowDescription(payload: Buffer): FieldDescription[] {
  const count = payload.readInt16BE(0);
  let offset = 2;
  const fields: FieldDescription[] = [];
  for (let i = 0; i < count; i++) {
    offset += 4; // table OID
    const nameRead = readCString(payload, offset);
    offset = nameRead.next;
    offset += 2 + 2; // table id, column id
    const typeOid = payload.readInt32BE(offset);
    offset += 4 + 2 + 2 + 2; // type size, modifier, format
    fields.push({ name: nameRead.value, typeOid });
  }
  return fields;
}

function parseDataRow(payload: Buffer): (string | null)[] {
  const count = payload.readInt16BE(0);
  let offset = 2;
  const row: (string | null)[] = [];
  for (let i = 0; i < count; i++) {
    const length = payload.readInt32BE(offset);
    offset += 4;
    if (length < 0) {
      row.push(null);
    } else {
      row.push(payload.toString('utf8', offset, offset + length));
      offset += length;
    }
  }
  return row;
}

/** Low-level framing: buffered socket reader with async message queue. */
export class WireConnection {
  private socket: net.Socket;
  private buffer: Buffer = Buffer.alloc(0);
  private queue: ParsedMessage[] = [];
  private waiters: {
    resolve: (m: ParsedMessage) => void;
    reject: (e: Error) => void;
  }[] = [];
  private closeWaiters: (() => void)[] = [];
  private ended = false;

  constructor(socket: net.Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => this.onData(chunk));
    socket.on('error', (err: Error) => this.failAll(err));
    socket.on('close', () => {
      this.ended = true;
      this.failAll(new InterfaceError('Connection closed'));
      for (const w of this.closeWaiters.splice(0)) {
        w();
      }
    });
  }

  static async connect(host: string, port: number,
                       timeoutMillis: number): Promise<WireConnection> {
    const socket = net.createConnection({ host, port });
    socket.setNoDelay(true);
    await new Promise<void>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      const onError = (err: Error) => {
        if (timer) clearTimeout(timer);
        socket.destroy();
        reject(new DbError(`could not connect to ${host}:${port}: ${err.message}`,
          '08001', 'ERROR'));
      };
      if (timeoutMillis > 0) {
        timer = setTimeout(() => onError(
          new Error(`connection timeout after ${timeoutMillis}ms`)), timeoutMillis);
      }
      socket.once('error', onError);
      socket.once('connect', () => {
        if (timer) clearTimeout(timer);
        socket.removeListener('error', onError);
        resolve();
      });
    });
    return new WireConnection(socket);
  }

  private onData(chunk: Buffer): void {
    this.buffer = this.buffer.length === 0
      ? chunk : Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 5) {
      const length = this.buffer.readInt32BE(1);
      if (length < 4) {
        this.failAll(new InterfaceError(`Invalid message length: ${length}`));
        return;
      }
      if (this.buffer.length < 1 + length) {
        break;
      }
      const type = this.buffer[0];
      const payload = this.buffer.subarray(5, 1 + length);
      this.buffer = this.buffer.subarray(1 + length);
      const waiter = this.waiters.shift();
      if (waiter) {
        waiter.resolve({ type, payload });
      } else {
        this.queue.push({ type, payload });
      }
    }
  }

  private failAll(err: Error): void {
    for (const w of this.waiters.splice(0)) {
      w.reject(err);
    }
    this.queue = [];
  }

  writeMessage(type: number, payload: Buffer): void {
    if (this.ended || this.socket.destroyed) {
      throw new InterfaceError('Connection is closed');
    }
    const header = Buffer.alloc(5);
    header[0] = type;
    header.writeInt32BE(4 + payload.length, 1);
    this.socket.write(Buffer.concat([header, payload]));
  }

  readMessage(): Promise<ParsedMessage> {
    const queued = this.queue.shift();
    if (queued) {
      return Promise.resolve(queued);
    }
    if (this.ended) {
      return Promise.reject(new InterfaceError('Connection closed'));
    }
    return new Promise<ParsedMessage>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  async closeGracefully(): Promise<void> {
    if (this.ended) {
      return;
    }
    try {
      this.writeMessage(TERMINATE, Buffer.alloc(0));
    } catch {
      // best effort
    }
    await new Promise<void>((resolve) => {
      this.closeWaiters.push(resolve);
      this.socket.end();
      setTimeout(() => {
        this.socket.destroy();
        resolve();
      }, 1000).unref?.();
    });
    this.ended = true;
  }

  get closed(): boolean {
    return this.ended || this.socket.destroyed;
  }

  /** Startup handshake: protocol 3.0, DieselDB type-byte framing. */
  async handshake(user: string, database: string, password: string): Promise<void> {
    const parts: Buffer[] = [];
    const pushCString = (s: string) => {
      parts.push(Buffer.from(s, 'utf8'));
      parts.push(Buffer.from([0]));
    };
    const version = Buffer.alloc(4);
    version.writeInt32BE(PROTOCOL_VERSION_3, 0);
    parts.push(version);
    pushCString('user');
    pushCString(user);
    pushCString('database');
    pushCString(database);
    pushCString('client_encoding');
    pushCString('UTF8');
    parts.push(Buffer.from([0]));
    const payload = Buffer.concat(parts);
    // DieselDB framing: type byte '0' + length (includes itself) + payload.
    const header = Buffer.alloc(5);
    header[0] = STARTUP;
    header.writeInt32BE(4 + payload.length, 1);
    this.socket.write(Buffer.concat([header, payload]));

    for (;;) {
      const msg = await this.readMessage();
      if (msg.type === AUTH_REQUEST) {
        if (msg.payload.length < 4) {
          throw new InterfaceError('Malformed AuthenticationRequest');
        }
        const authType = msg.payload.readInt32BE(0);
        if (authType === 0) {
          continue; // AuthenticationOk
        }
        if (authType === 3) {
          const pwd = Buffer.concat([Buffer.from(password, 'utf8'),
            Buffer.from([0])]);
          this.writeMessage(PASSWORD_MESSAGE, pwd);
          continue;
        }
        throw new NotSupportedError(
          `Unsupported authentication method: ${authType} ` +
          '(supported: trust, cleartext password)');
      }
      if (msg.type === PARAMETER_STATUS || msg.type === NOTICE_RESPONSE) {
        continue;
      }
      if (msg.type === READY_FOR_QUERY) {
        return;
      }
      if (msg.type === ERROR_RESPONSE) {
        throw new DbError(`Startup failed: ${parseErrorFields(msg.payload).message}`);
      }
      // ignore unknown startup-phase messages
    }
  }

  /** Execute one statement via the simple-query protocol. */
  async simpleQuery(sql: string): Promise<RawQueryResult> {
    this.writeMessage(SIMPLE_QUERY, Buffer.concat([
      Buffer.from(sql, 'utf8'), Buffer.from([0])]));

    let columnNames: string[] | null = null;
    let columnTypeOids: number[] | null = null;
    const rows: (string | null)[][] = [];
    let commandTag: string | null = null;
    let error: DbError | null = null;

    for (;;) {
      const msg = await this.readMessage();
      switch (msg.type) {
        case ROW_DESCRIPTION: {
          const fields = parseRowDescription(msg.payload);
          columnNames = fields.map((f) => f.name);
          columnTypeOids = fields.map((f) => f.typeOid);
          break;
        }
        case DATA_ROW:
          rows.push(parseDataRow(msg.payload));
          break;
        case COMMAND_COMPLETE:
          commandTag = readCString(msg.payload, 0).value;
          break;
        case ERROR_RESPONSE:
          error = parseErrorFields(msg.payload);
          break;
        case NOTICE_RESPONSE:
        case PARAMETER_STATUS:
        case EMPTY_QUERY_RESPONSE:
          break;
        case READY_FOR_QUERY:
          if (error) {
            throw error;
          }
          return { columnNames, columnTypeOids, rows, commandTag };
        default:
          break;
      }
    }
  }
}
