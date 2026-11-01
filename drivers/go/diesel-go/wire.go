// Package diesel is a minimal libpq wire-protocol client for the DieselDB
// Go driver, implementing the subset of the protocol spoken by
// diesel.LibpqServer:
//
//   - startup handshake (protocol 3.0, DieselDB-style leading type byte '0' -
//     the server's LibpqClientHandler.readMessage expects a 5-byte header on
//     the startup message too, unlike stock PostgreSQL),
//   - trust / cleartext-password authentication,
//   - simple-query protocol with client-side parameter substitution,
//   - response decoding: RowDescription, DataRow, CommandComplete,
//     ErrorResponse, NoticeResponse, ParameterStatus, ReadyForQuery.
//
// The server's extended protocol (Parse/Bind/Execute) is a stub that executes
// a hardcoded SELECT 1, so this driver only ever uses simple queries.
package diesel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// Backend message type bytes (subset used by the DieselDB server).
const (
	msgAuthRequest     = 'R'
	msgParameterStatus = 'S'
	msgReadyForQuery   = 'Z'
	msgRowDescription  = 'T'
	msgDataRow         = 'D'
	msgCommandComplete = 'C'
	msgErrorResponse   = 'E'
	msgNoticeResponse  = 'N'
	msgEmptyQuery      = 'I'
)

// Frontend message type bytes.
const (
	msgStartup      = '0' // DieselDB-style startup framing (type byte + length)
	msgSimpleQuery  = 'Q'
	msgPassword     = 'p'
	msgTerminate    = 'X'
	protocolVersion = 196608
)

// Authentication sub-codes inside AuthenticationRequest.
const (
	authOK               = 0
	authCleartextPassword = 3
)

// ServerError is a raw server error with SQLSTATE and severity.
type ServerError struct {
	Severity string
	SQLState string
	Message  string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error %s: %s", e.SQLState, e.Message)
}

// QueryResult is the outcome of one simple-query round trip.
type QueryResult struct {
	ColumnNames    []string
	ColumnTypeOids []int32
	Rows           [][]*string
	CommandTag     string
}

type wireClient struct {
	conn   net.Conn
	closed bool
}

func dialWire(host string, port int, user, database, password string, timeout time.Duration) (*wireClient, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	w := &wireClient{conn: conn}
	if err := w.handshake(user, database, password); err != nil {
		w.close()
		return nil, err
	}
	return w, nil
}

// ---- handshake ----

func (w *wireClient) handshake(user, database, password string) error {
	if err := w.sendStartup(user, database); err != nil {
		return err
	}
	for {
		mtype, payload, err := w.readMessage()
		if err != nil {
			return fmt.Errorf("connection closed during startup handshake: %w", err)
		}
		switch mtype {
		case msgAuthRequest:
			if err := w.handleAuthRequest(payload, password); err != nil {
				return err
			}
		case msgParameterStatus, msgNoticeResponse:
			// ignore
		case msgReadyForQuery:
			return nil
		case msgErrorResponse:
			err := parseErrorFields(payload)
			return fmt.Errorf("startup failed: %w", err)
		}
	}
}

func (w *wireClient) sendStartup(user, database string) error {
	var params []byte
	params = append(params, "user\x00"...)
	params = append(params, user...)
	params = append(params, 0)
	params = append(params, "database\x00"...)
	params = append(params, database...)
	params = append(params, 0)
	params = append(params, "client_encoding\x00UTF8\x00"...)
	params = append(params, 0) // terminator

	payload := make([]byte, 4, 4+len(params))
	binary.BigEndian.PutUint32(payload, protocolVersion)
	payload = append(payload, params...)

	// DieselDB framing: type byte '0' + length (includes itself) + payload.
	header := make([]byte, 5)
	header[0] = msgStartup
	binary.BigEndian.PutUint32(header[1:], uint32(4+len(payload)))
	if _, err := w.conn.Write(append(header, payload...)); err != nil {
		return err
	}
	return nil
}

func (w *wireClient) handleAuthRequest(payload []byte, password string) error {
	if len(payload) < 4 {
		return errors.New("malformed AuthenticationRequest")
	}
	authType := int32(binary.BigEndian.Uint32(payload))
	switch authType {
	case authOK:
		return nil
	case authCleartextPassword:
		return w.writeMessage(msgPassword, append([]byte(password), 0))
	default:
		return fmt.Errorf("unsupported authentication method: %d (supported: trust, cleartext password)", authType)
	}
}

// ---- public API ----

// simpleQuery executes one statement via the simple-query protocol.
func (w *wireClient) simpleQuery(sql string) (*QueryResult, error) {
	if err := w.writeMessage(msgSimpleQuery, append([]byte(sql), 0)); err != nil {
		return nil, err
	}

	var (
		result   = &QueryResult{}
		haveDesc bool
		errResp  *ServerError
	)

	for {
		mtype, payload, err := w.readMessage()
		if err != nil {
			return nil, fmt.Errorf("connection closed while reading query result: %w", err)
		}
		switch mtype {
		case msgRowDescription:
			names, oids, err := parseRowDescription(payload)
			if err != nil {
				return nil, err
			}
			result.ColumnNames, result.ColumnTypeOids = names, oids
			haveDesc = true
		case msgDataRow:
			row, err := parseDataRow(payload)
			if err != nil {
				return nil, err
			}
			result.Rows = append(result.Rows, row)
		case msgCommandComplete:
			result.CommandTag = readCString(payload, 0)
		case msgErrorResponse:
			errResp = parseErrorFields(payload)
		case msgNoticeResponse, msgParameterStatus, msgEmptyQuery:
			// ignore
		case msgReadyForQuery:
			goto done
		}
	}

done:
	if errResp != nil {
		return nil, errResp
	}
	if !haveDesc {
		result.ColumnNames = nil
	}
	return result, nil
}

func (w *wireClient) close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.writeMessage(msgTerminate, nil)
	return w.conn.Close()
}

// ---- framing ----

func (w *wireClient) writeMessage(mtype byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = mtype
	binary.BigEndian.PutUint32(header[1:], uint32(4+len(payload)))
	if _, err := w.conn.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (w *wireClient) readMessage() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(w.conn, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, io.EOF
		}
		return 0, nil, err
	}
	mtype := header[0]
	length := int32(binary.BigEndian.Uint32(header[1:]))
	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length: %d", length)
	}
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(w.conn, payload); err != nil {
		return 0, nil, fmt.Errorf("connection closed mid-payload: %w", err)
	}
	return mtype, payload, nil
}

// ---- parsing ----

func parseErrorFields(payload []byte) *ServerError {
	var severity, sqlstate, message, first string
	i := 0
	for i < len(payload) {
		fieldType := payload[i]
		i++
		if fieldType == 0 {
			break
		}
		end := i
		for end < len(payload) && payload[end] != 0 {
			end++
		}
		value := string(payload[i:end])
		i = end + 1
		if first == "" {
			first = value
		}
		switch fieldType {
		case 'S', 'V':
			severity = value
		case 'C':
			sqlstate = value
		case 'M':
			message = value
		}
	}
	if message == "" {
		message = first
		if message == "" {
			message = "unknown error"
		}
	}
	if severity == "" {
		severity = "ERROR"
	}
	if sqlstate == "" {
		sqlstate = "00000"
	}
	return &ServerError{Severity: severity, SQLState: sqlstate, Message: message}
}

// parseRowDescription parses the DieselDB RowDescription layout:
// int16 count, then per field int32 tableOid, cstring name, int16 tableId,
// int16 columnId, int32 typeOid, int16 typeSize, int16 typeModifier,
// int16 formatCode.
func parseRowDescription(payload []byte) ([]string, []int32, error) {
	if len(payload) < 2 {
		return nil, nil, errors.New("short RowDescription")
	}
	count := int(binary.BigEndian.Uint16(payload))
	offset := 2
	names := make([]string, 0, count)
	oids := make([]int32, 0, count)
	for f := 0; f < count; f++ {
		if offset+4 > len(payload) {
			return nil, nil, errors.New("short RowDescription field")
		}
		offset += 4 // tableOid (unused)
		end := offset
		for end < len(payload) && payload[end] != 0 {
			end++
		}
		names = append(names, string(payload[offset:end]))
		offset = end + 1
		if offset+14 > len(payload) {
			return nil, nil, errors.New("short RowDescription tail")
		}
		oids = append(oids, int32(binary.BigEndian.Uint32(payload[offset+4:])))
		offset += 14 // tableId, columnId, typeOid, typeSize, typeMod, format
	}
	return names, oids, nil
}

// parseDataRow parses int16 count, then per column int32 length (-1 = NULL)
// + length bytes (text format).
func parseDataRow(payload []byte) ([]*string, error) {
	if len(payload) < 2 {
		return nil, errors.New("short DataRow")
	}
	count := int(binary.BigEndian.Uint16(payload))
	offset := 2
	row := make([]*string, 0, count)
	for c := 0; c < count; c++ {
		if offset+4 > len(payload) {
			return nil, errors.New("short DataRow column")
		}
		length := int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		if length < 0 {
			row = append(row, nil)
			continue
		}
		if offset+int(length) > len(payload) {
			return nil, errors.New("short DataRow value")
		}
		value := string(payload[offset : offset+int(length)])
		offset += int(length)
		row = append(row, &value)
	}
	return row, nil
}

func readCString(payload []byte, offset int) string {
	end := offset
	for end < len(payload) && payload[end] != 0 {
		end++
	}
	return string(payload[offset:end])
}
