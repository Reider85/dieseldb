package com.diesel.jdbc;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal libpq wire-protocol client used by the DieselDB JDBC driver.
 *
 * <p>Implements the subset of the protocol spoken by {@code diesel.LibpqServer}:
 * startup handshake, trust/password authentication (server-side simplified),
 * simple query protocol, and response decoding (RowDescription, DataRow,
 * CommandComplete, ErrorResponse, ReadyForQuery).
 */
final class LibpqWireClient {

    /** Result of a simple-query execution. */
    static final class QueryResult {
        final String[] columnNames;
        final int[] columnTypeOids;
        final List<String[]> rows;
        final String commandTag;
        final String error;

        QueryResult(String[] columnNames, int[] columnTypeOids,
                    List<String[]> rows, String commandTag, String error) {
            this.columnNames = columnNames;
            this.columnTypeOids = columnTypeOids;
            this.rows = rows;
            this.commandTag = commandTag;
            this.error = error;
        }

        boolean isError() {
            return error != null;
        }
    }

    // libpq message type bytes (subset)
    private static final byte AUTH_REQUEST = 'R';
    private static final byte PARAMETER_STATUS = 'S';
    private static final byte READY_FOR_QUERY = 'Z';
    private static final byte ROW_DESCRIPTION = 'T';
    private static final byte DATA_ROW = 'D';
    private static final byte COMMAND_COMPLETE = 'C';
    private static final byte ERROR_RESPONSE = 'E';
    private static final byte NOTICE_RESPONSE = 'N';
    private static final byte STARTUP = '0';

    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;

    LibpqWireClient(String host, int port, String user, String database,
                    String password, int connectTimeoutMillis) throws IOException {
        this.socket = new Socket();
        try {
            this.socket.connect(new java.net.InetSocketAddress(host, port), connectTimeoutMillis);
            this.socket.setTcpNoDelay(true);
        } catch (IOException e) {
            try { socket.close(); } catch (IOException ignore) { }
            throw e;
        }
        this.input = new DataInputStream(this.socket.getInputStream());
        this.output = this.socket.getOutputStream();
        try {
            handshake(user, database, password);
        } catch (IOException | RuntimeException e) {
            close();
            throw e;
        }
    }

    private void handshake(String user, String database, String password) throws IOException {
        sendStartup(user, database);
        // Read until ReadyForQuery. Handle auth challenges along the way.
        while (true) {
            Message msg = readMessage();
            if (msg == null) {
                throw new EOFException("Connection closed during startup handshake");
            }
            switch (msg.type) {
                case AUTH_REQUEST:
                    handleAuthRequest(msg.payload, password);
                    break;
                case PARAMETER_STATUS:
                case NOTICE_RESPONSE:
                    // ignored
                    break;
                case READY_FOR_QUERY:
                    return;
                case ERROR_RESPONSE:
                    throw new IOException("Startup failed: " + parseErrorFields(msg.payload));
                default:
                    // ignore unknown startup messages
                    break;
            }
        }
    }

    private void handleAuthRequest(byte[] payload, String password) throws IOException {
        if (payload.length < 4) {
            throw new IOException("Malformed AuthenticationRequest");
        }
        int authType = ByteBuffer.wrap(payload, 0, 4).getInt();
        switch (authType) {
            case 0: // AuthenticationOk
                break;
            case 3: // AuthenticationCleartextPassword
                sendPasswordMessage(password == null ? "" : password);
                break;
            default:
                throw new IOException("Unsupported authentication method: " + authType
                        + " (supported: trust, cleartext password)");
        }
    }

    private void sendStartup(String user, String database) throws IOException {
        // Payload: protocol version 3.0 (196608) + key/value pairs + double null
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        ByteBuffer ver = ByteBuffer.allocate(4);
        ver.putInt(196608);
        bos.write(ver.array(), 0, 4);
        writeCString(bos, "user");
        writeCString(bos, user == null ? "postgres" : user);
        writeCString(bos, "database");
        writeCString(bos, database == null ? "postgres" : database);
        writeCString(bos, "client_encoding");
        writeCString(bos, "UTF8");
        bos.write(0); // terminator
        byte[] payload = bos.toByteArray();
        // length includes itself (4) but not the type byte
        ByteBuffer header = ByteBuffer.allocate(5);
        header.put(STARTUP);
        header.putInt(4 + payload.length);
        output.write(header.array());
        output.write(payload);
        output.flush();
    }

    private void sendPasswordMessage(String password) throws IOException {
        byte[] pwd = (password + "\0").getBytes(StandardCharsets.UTF_8);
        ByteBuffer header = ByteBuffer.allocate(5);
        header.put((byte) 'p');
        header.putInt(4 + pwd.length);
        output.write(header.array());
        output.write(pwd);
        output.flush();
    }

    /**
     * Executes a query via the simple-query protocol and collects the full
     * response (RowDescription / DataRow* / CommandComplete or ErrorResponse).
     */
    QueryResult simpleQuery(String sql) throws IOException {
        byte[] q = (sql + "\0").getBytes(StandardCharsets.UTF_8);
        ByteBuffer header = ByteBuffer.allocate(5);
        header.put((byte) 'Q');
        header.putInt(4 + q.length);
        output.write(header.array());
        output.write(q);
        output.flush();

        String[] columns = null;
        int[] typeOids = null;
        List<String[]> rows = new ArrayList<>();
        String commandTag = null;
        String error = null;

        while (true) {
            Message msg = readMessage();
            if (msg == null) {
                throw new EOFException("Connection closed while reading query result");
            }
            switch (msg.type) {
                case ROW_DESCRIPTION: {
                    FieldDesc[] fields = parseRowDescription(msg.payload);
                    columns = new String[fields.length];
                    typeOids = new int[fields.length];
                    for (int i = 0; i < fields.length; i++) {
                        columns[i] = fields[i].name;
                        typeOids[i] = fields[i].typeOid;
                    }
                    break;
                }
                case DATA_ROW:
                    rows.add(parseDataRow(msg.payload));
                    break;
                case COMMAND_COMPLETE:
                    commandTag = readCString(msg.payload, 0);
                    break;
                case ERROR_RESPONSE:
                    error = parseErrorFields(msg.payload);
                    break;
                case NOTICE_RESPONSE:
                case PARAMETER_STATUS:
                    break;
                case READY_FOR_QUERY:
                    return new QueryResult(columns, typeOids, rows, commandTag, error);
                default:
                    break;
            }
        }
    }

    /** Sends Terminate and closes the socket. */
    void close() {
        try {
            if (!socket.isClosed()) {
                ByteBuffer header = ByteBuffer.allocate(5);
                header.put((byte) 'X');
                header.putInt(4);
                output.write(header.array());
                output.flush();
            }
        } catch (IOException ignore) {
            // best effort
        }
        try {
            socket.close();
        } catch (IOException ignore) {
            // best effort
        }
    }

    boolean isClosed() {
        return socket.isClosed();
    }

    // ---- wire decoding helpers ----

    private static final class Message {
        final byte type;
        final byte[] payload;
        Message(byte type, byte[] payload) {
            this.type = type;
            this.payload = payload;
        }
    }

    private Message readMessage() throws IOException {
        int type = input.read();
        if (type < 0) {
            return null; // EOF
        }
        int len = readInt();
        if (len < 4) {
            throw new IOException("Invalid message length: " + len);
        }
        byte[] payload = new byte[len - 4];
        readFully(payload);
        return new Message((byte) type, payload);
    }

    private int readInt() throws IOException {
        int b1 = input.read(), b2 = input.read(), b3 = input.read(), b4 = input.read();
        if ((b1 | b2 | b3 | b4) < 0) {
            throw new EOFException("Unexpected end of stream");
        }
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    private void readFully(byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = input.read(buf, off, buf.length - off);
            if (n < 0) {
                throw new EOFException("Unexpected end of stream");
            }
            off += n;
        }
    }

    private static final class FieldDesc {
        final String name;
        final int typeOid;
        FieldDesc(String name, int typeOid) {
            this.name = name;
            this.typeOid = typeOid;
        }
    }

    /**
     * Parses the server's RowDescription payload. The DieselDB server encodes:
     * int16 fieldCount, then per field: int32 objectId, cstring name,
     * int16 tableId, int16 columnId, int32 fieldType, int16 typeSize,
     * int16 typeModifier, int16 formatCode.
     */
    private static FieldDesc[] parseRowDescription(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int count = buf.getShort() & 0xFFFF;
        FieldDesc[] fields = new FieldDesc[count];
        for (int i = 0; i < count; i++) {
            buf.getInt(); // object id
            String name = readCString(buf);
            buf.getShort(); // table id
            buf.getShort(); // column id
            int typeOid = buf.getInt();
            buf.getShort(); // type size
            buf.getShort(); // type modifier
            buf.getShort(); // format code
            fields[i] = new FieldDesc(name, typeOid);
        }
        return fields;
    }

    /**
     * Parses a DataRow payload: int16 columnCount, then per column
     * int32 length (-1 = null) + bytes.
     */
    private static String[] parseDataRow(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int count = buf.getShort() & 0xFFFF;
        String[] row = new String[count];
        for (int i = 0; i < count; i++) {
            int len = buf.getInt();
            if (len < 0) {
                row[i] = null;
            } else {
                byte[] bytes = new byte[len];
                buf.get(bytes);
                row[i] = new String(bytes, StandardCharsets.UTF_8);
            }
        }
        return row;
    }

    /**
     * Parses ErrorResponse fields: sequence of (byte fieldType, cstring)
     * terminated by a zero byte. Returns the 'M' (message) field, falling back
     * to the first field value.
     */
    private static String parseErrorFields(byte[] payload) {
        String message = null;
        String first = null;
        int i = 0;
        while (i < payload.length) {
            int fieldType = payload[i++] & 0xFF;
            if (fieldType == 0) {
                break;
            }
            int start = i;
            while (i < payload.length && payload[i] != 0) {
                i++;
            }
            String value = new String(payload, start, i - start, StandardCharsets.UTF_8);
            i++; // skip null
            if (first == null) {
                first = value;
            }
            if (fieldType == 'M') {
                message = value;
            }
        }
        return message != null ? message : (first != null ? first : "unknown error");
    }

    private static String readCString(ByteBuffer buf) {
        int start = buf.position();
        while (buf.hasRemaining() && buf.get() != 0) {
            // advance
        }
        int end = buf.position() - 1; // position of the null
        byte[] bytes = new byte[end - start];
        buf.position(start);
        buf.get(bytes);
        buf.get(); // consume null
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readCString(byte[] payload, int offset) {
        int end = offset;
        while (end < payload.length && payload[end] != 0) {
            end++;
        }
        return new String(payload, offset, end - offset, StandardCharsets.UTF_8);
    }

    private static void writeCString(java.io.ByteArrayOutputStream bos, String s) throws IOException {
        bos.write(s.getBytes(StandardCharsets.UTF_8));
        bos.write(0);
    }
}
