package diesel

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PostgreSQL type OIDs the DieselDB server can emit (LibpqClientHandler
// .typeOidFor) plus common text-family OIDs for forward compatibility.
const (
	oidBool    = 16
	oidInt8    = 20
	oidInt2    = 21
	oidInt4    = 23
	oidText    = 25
	oidFloat4  = 700
	oidFloat8  = 701
	oidVarchar = 1043
	oidDate    = 1082
	oidTime    = 1083
	oidTimestamp = 1114
	oidNumeric = 1700
)

// decodeValue converts a text-format wire value to a database/sql driver.Value
// by type OID.
func decodeValue(text *string, oid int32) driver.Value {
	if text == nil {
		return nil
	}
	s := *text
	switch oid {
	case oidInt2, oidInt4, oidInt8:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	case oidFloat4, oidFloat8:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	case oidBool:
		switch s {
		case "t", "true", "TRUE", "True", "1", "y", "yes":
			return true
		case "f", "false", "FALSE", "False", "0", "n", "no":
			return false
		}
	case oidDate:
		if t, err := time.Parse("2006-01-02", s); err == nil {
			return t
		}
	case oidTimestamp:
		for _, layout := range []string{
			"2006-01-02 15:04:05.999999999",
			"2006-01-02T15:04:05.999999999",
			"2006-01-02 15:04:05",
		} {
			if t, err := time.Parse(layout, s); err == nil {
				return t
			}
		}
	case oidNumeric:
		// No Decimal type in database/sql; keep the exact text form.
		return s
	}
	// text / varchar / unknown / failed conversions
	return s
}

// quoteLiteral renders a driver.Value as a SQL literal (client-side binding).
func quoteLiteral(v driver.Value) string {
	switch value := v.(type) {
	case nil:
		return "NULL"
	case bool:
		// DieselDB stores booleans as integers in the engine's type system.
		if value {
			return "1"
		}
		return "0"
	case int64:
		return strconv.FormatInt(value, 10)
	case float64:
		return strconv.FormatFloat(value, 'g', -1, 64)
	case string:
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	case []byte:
		escaped := strings.ReplaceAll(string(value), "'", "''")
		return "'" + escaped + "'"
	case time.Time:
		return "'" + value.Format("2006-01-02 15:04:05.999999") + "'"
	default:
		escaped := strings.ReplaceAll(fmt.Sprint(value), "'", "''")
		return "'" + escaped + "'"
	}
}

// bindDollar substitutes $1..$n placeholders with quoted literals
// (client-side binding; the server's extended protocol is a stub).
func bindDollar(query string, args []driver.Value) (string, error) {
	if len(args) == 0 && !strings.Contains(query, "$") {
		return query, nil
	}
	var b strings.Builder
	maxIndex := 0
	i := 0
	for i < len(query) {
		if query[i] == '$' {
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			if j > i+1 {
				n, err := strconv.Atoi(query[i+1 : j])
				if err != nil || n < 1 {
					return "", fmt.Errorf("invalid placeholder %q", query[i:j])
				}
				if n > maxIndex {
					maxIndex = n
				}
				if n > len(args) {
					return "", fmt.Errorf("missing argument for placeholder $%d", n)
				}
				b.WriteString(quoteLiteral(args[n-1]))
				i = j
				continue
			}
		}
		b.WriteByte(query[i])
		i++
	}
	if maxIndex > len(args) {
		return "", fmt.Errorf("arity mismatch: %d placeholders, %d arguments", maxIndex, len(args))
	}
	if len(args) > maxIndex {
		return "", fmt.Errorf("arity mismatch: %d placeholders, %d arguments", maxIndex, len(args))
	}
	return b.String(), nil
}

// namedValuesToDriver converts database/sql named values to driver values.
func namedValuesToDriver(args []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		if arg.Name != "" {
			return nil, errors.New("named parameters are not supported (use $1..$n)")
		}
		values[i] = arg.Value
	}
	return values, nil
}

// rowsFromTag is a best-effort affected-row count from a CommandComplete tag.
// The DieselDB server hardcodes counts (SELECT 1 / INSERT 0 1 / UPDATE 1 /
// DELETE 1 - LibpqClientHandler.getCommandCompleteTag), so this only reflects
// the (approximate) server-provided number.
func rowsFromTag(tag string) int64 {
	parts := strings.Fields(tag)
	for i := len(parts) - 1; i >= 0; i-- {
		if n, err := strconv.ParseInt(parts[i], 10, 64); err == nil {
			return n
		}
	}
	return 0
}
