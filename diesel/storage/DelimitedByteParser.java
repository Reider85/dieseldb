package diesel.storage;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;

/**
 * Direct byte[]→Object[] parser for CSV/TSV files. Parses file bytes without
 * creating intermediate String objects for typed columns (Long, Integer,
 * Double, Float, Boolean, BigDecimal). String columns still require
 * {@code new String(bytes, offset, len, charset)}, but only for those columns.
 *
 * <p>The main performance win comes from:
 * <ul>
 *   <li>No {@code List<String>} lines — scans byte[] directly for line boundaries</li>
 *   <li>No {@code List<String>} fields for unquoted lines — scans byte[] for delimiters</li>
 *   <li>Direct ASCII→primitive parsing — no intermediate String for typed columns</li>
 *   <li>Only one {@code Object[]} allocation per row (the final result)</li>
 * </ul>
 *
 * <p>Quoted fields and multi-line CSV rows fall back to the existing
 * {@link CsvRowReader#parseDataFieldsQuoted(String)} path via
 * {@code new String(bytes, ...)}, which is acceptable since quoted fields
 * are rare (~5% of lines in typical datasets).
 */
public final class DelimitedByteParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelimitedByteParser.class);

    private DelimitedByteParser() {
        throw new AssertionError("No instances");
    }

    /**
     * Parse a whole delimited file directly from its raw bytes into Object[] rows.
     *
     * @param bytes          raw file bytes (already decompressed by CompressionCodec)
     * @param charset        charset for String columns and for line/field scanning if non-ASCII
     * @param columns        schema column names (header must match, in order)
     * @param columnTypes    schema column types
     * @param delimiter      ',' for CSV, '\t' for TSV (single byte, ASCII)
     * @param quoteChar      '"' for CSV, 0 for TSV (TSV has no quoting)
     * @param fileName       for error diagnostics
     * @return list of Object[] rows, one per data line, in file order
     */
    public static List<Object[]> parse(byte[] bytes, Charset charset,
                                       List<String> columns, Map<String, Class<?>> columnTypes,
                                       byte delimiter, byte quoteChar, String fileName) {
        if (bytes.length == 0) {
            return List.of();
        }

        // Fall back to legacy path for non-standard modes that need full CsvRowReader behavior:
        // - Sentinel mode: \N sentinel for nulls (handled differently by CsvRowReader)
        // - Non-fail error modes: skip_row/skip_value need line-number tracking from CsvRowReader
        if (shouldUseLegacyPath()) {
            return parseViaLegacy(bytes, charset, columns, columnTypes, fileName);
        }

        // 1. Find the header line end
        int headerEnd = findLineEnd(bytes, 0);
        if (headerEnd < 0) {
            // Single line — header only, no data
            return List.of();
        }

        // Quick check: if the header line contains quoteChar, fall back to legacy path
        // because header parsing via byte scan can't handle quoted fields with delimiters inside.
        if (quoteChar != 0 && containsByte(bytes, 0, headerEnd, quoteChar)) {
            return parseViaLegacy(bytes, charset, columns, columnTypes, fileName);
        }

        // 2. Parse header to build column mapping
        int dataStart = nextLineStart(bytes, headerEnd);
        if (dataStart < 0 || dataStart >= bytes.length) {
            return List.of();
        }

        // Parse header bytes into column names and build mapping
        int[] columnMapping = parseHeaderAndBuildMapping(bytes, 0, headerEnd, charset, delimiter, columns, fileName);

        // 3. Parse data lines
        return parseDataLines(bytes, dataStart, charset, columns, columnTypes,
                delimiter, quoteChar, fileName, columnMapping);
    }

    /**
     * Returns true when the byte[] fast path should NOT be used and the legacy
     * CsvRowReader path should be used instead. This happens when any non-default
     * configuration is active that requires CsvRowReader's full diagnostics,
     * or when charset validation with CodingErrorAction.REPORT is needed.
     *
     * <p>The byte[] fast path is an optimization for the common happy path:
     * UTF-8/ASCII/ISO-8859-1 data, fail error mode, no sentinel mode, and
     * well-formed files. For anything else, the legacy path provides better
     * diagnostics (file:line:column, exception chaining, charset validation).
     */
    private static boolean shouldUseLegacyPath() {
        // Sentinel mode: \N sentinel for nulls
        if (TsvRowWriter.isSentinelMode()) {
            return true;
        }
        // Non-default error modes need line-number diagnostics from CsvRowReader
        String errorMode = System.getProperty("storage.load.error.mode");
        if (errorMode != null && !"fail".equalsIgnoreCase(errorMode.trim())) {
            return true;
        }
        // Non-default header mismatch modes need CsvRowReader's warning/error path
        String headerMode = System.getProperty("storage.header.mismatch.mode");
        if (headerMode != null && !"fail".equalsIgnoreCase(headerMode.trim())) {
            return true;
        }
        // Charset validation: the byte parser uses new String() which silently
        // replaces malformed bytes. The legacy path uses DelimitedContent.decode()
        // with CodingErrorAction.REPORT which throws on malformed input.
        // For non-ASCII-compatible charsets (KOI8-R, windows-1251, etc.) loaded
        // through UTF-8, the legacy path correctly throws DieselIOException.
        // We can't distinguish these at parse time, so we let the isByteFastPathCompatible
        // check in the caller handle this (only UTF-8/ASCII/ISO-8859-1 reach here).
        return false;
    }

    /**
     * Validate that the bytes are valid for the given charset.
     * Uses CharsetDecoder with REPORT action to match DelimitedContent.decode() behavior.
     */
    private static void validateCharset(byte[] bytes, Charset charset, String fileName) {
        java.nio.charset.CharsetDecoder decoder = charset.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
        try {
            decoder.decode(java.nio.ByteBuffer.wrap(bytes));
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new DieselIOException(
                    "Malformed " + charset.name() + " input in delimited file " + fileName, e);
        }
    }

    /**
     * Parse a byte range for the partitioned parallel path.
     *
     * @param bytes          raw file bytes
     * @param start          start byte offset (inclusive, must be line-aligned)
     * @param end            end byte offset (exclusive)
     * @param charset        charset for String columns
     * @param columns        schema column names
     * @param columnTypes    schema column types
     * @param columnMapping  header-to-schema mapping from {@link #parse}
     * @param firstDataLine  1-based physical line number of the first data line in this range
     * @param delimiter      ',' for CSV, '\t' for TSV
     * @param quoteChar      '"' for CSV, 0 for TSV
     * @param fileName       for error diagnostics
     * @return list of Object[] rows
     */
    public static List<Object[]> parseRange(byte[] bytes, int start, int end, Charset charset,
                                            List<String> columns, Map<String, Class<?>> columnTypes,
                                            int[] columnMapping, long firstDataLine,
                                            byte delimiter, byte quoteChar, String fileName) {
        if (start >= end || start >= bytes.length) {
            return List.of();
        }
        end = Math.min(end, bytes.length);
        return parseDataLines(bytes, start, charset, columns, columnTypes,
                delimiter, quoteChar, fileName, columnMapping);
    }

    // ─── ASCII fast-path parsers (zero allocation) ─────────────────

    /**
     * Parse a long directly from ASCII bytes. Returns null on parse failure
     * (caller falls back to String-based path for diagnostics).
     */
    static Long parseLongAscii(byte[] b, int off, int len) {
        if (len == 0) return null;
        boolean negative = false;
        int i = off;
        int end = off + len;
        if (b[i] == (byte) '-') {
            negative = true;
            i++;
        } else if (b[i] == (byte) '+') {
            i++;
        }
        long result = 0;
        boolean hasDigits = false;
        while (i < end) {
            byte c = b[i];
            if (c < (byte) '0' || c > (byte) '9') {
                return null; // non-digit → not a valid long
            }
            result = result * 10 + (c - (byte) '0');
            hasDigits = true;
            i++;
        }
        if (!hasDigits) return null;
        return negative ? -result : result;
    }

    /**
     * Parse an integer directly from ASCII bytes. Returns null on parse failure.
     */
    static Integer parseIntAscii(byte[] b, int off, int len) {
        if (len == 0) return null;
        boolean negative = false;
        int i = off;
        int end = off + len;
        if (b[i] == (byte) '-') {
            negative = true;
            i++;
        } else if (b[i] == (byte) '+') {
            i++;
        }
        long result = 0;
        boolean hasDigits = false;
        while (i < end) {
            byte c = b[i];
            if (c < (byte) '0' || c > (byte) '9') {
                return null;
            }
            result = result * 10 + (c - (byte) '0');
            hasDigits = true;
            i++;
        }
        if (!hasDigits) return null;
        long val = negative ? -result : result;
        if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) return null;
        return (int) val;
    }

    /**
     * Parse a double directly from ASCII bytes. Returns null on parse failure.
     * Handles: digits, sign, decimal point, exponent (e/E).
     */
    static Double parseDoubleAscii(byte[] b, int off, int len) {
        if (len == 0) return null;
        // Quick reject: check for any non-ASCII byte
        for (int i = off; i < off + len; i++) {
            if (b[i] < 0) return null; // non-ASCII → not valid double
        }
        // Delegate to String-based parse (Double.parseDouble is heavily optimized by HotSpot)
        try {
            return Double.parseDouble(new String(b, off, len, StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parse a float directly from ASCII bytes. Returns null on parse failure.
     */
    static Float parseFloatAscii(byte[] b, int off, int len) {
        if (len == 0) return null;
        for (int i = off; i < off + len; i++) {
            if (b[i] < 0) return null;
        }
        try {
            return Float.parseFloat(new String(b, off, len, StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parse a boolean directly from ASCII bytes. Accepts "true"/"false" only.
     * Returns null for any other value.
     */
    static Boolean parseBooleanAscii(byte[] b, int off, int len) {
        if (len == 4 && b[off] == (byte) 't' && b[off + 1] == (byte) 'r'
                && b[off + 2] == (byte) 'u' && b[off + 3] == (byte) 'e') {
            return Boolean.TRUE;
        }
        if (len == 5 && b[off] == (byte) 'f' && b[off + 1] == (byte) 'a'
                && b[off + 2] == (byte) 'l' && b[off + 3] == (byte) 's'
                && b[off + 4] == (byte) 'e') {
            return Boolean.FALSE;
        }
        // Also accept "1"/"0" and case-insensitive "TRUE"/"FALSE"
        if (len == 1) {
            if (b[off] == (byte) '1') return Boolean.TRUE;
            if (b[off] == (byte) '0') return Boolean.FALSE;
        }
        if (len <= 5) {
            String s = new String(b, off, len, StandardCharsets.US_ASCII);
            return switch (s.toLowerCase(java.util.Locale.ROOT)) {
                case "true", "1", "yes", "t" -> Boolean.TRUE;
                case "false", "0", "no", "f" -> Boolean.FALSE;
                default -> null;
            };
        }
        return null;
    }

    /**
     * Parse a BigDecimal directly from ASCII bytes. This is the only numeric
     * type that requires a String intermediate (BigDecimal has no byte[] ctor).
     */
    static BigDecimal parseBigDecimalAscii(byte[] b, int off, int len) {
        if (len == 0) return null;
        for (int i = off; i < off + len; i++) {
            if (b[i] < 0) return null;
        }
        return new BigDecimal(new String(b, off, len, StandardCharsets.US_ASCII));
    }

    // ─── Line scanning utilities ────────────────────────────────────

    /**
     * Finds the end of the current line (exclusive) starting from startOffset.
     * Handles \n, \r\n, and \r line terminators.
     * Returns -1 if no line terminator is found (end of buffer).
     */
    static int findLineEnd(byte[] b, int startOffset) {
        for (int i = startOffset; i < b.length; i++) {
            byte c = b[i];
            if (c == (byte) '\n' || c == (byte) '\r') {
                return i;
            }
        }
        return -1; // no terminator found — last line
    }

    /**
     * Returns the start offset of the next line after lineEnd.
     * lineEnd is the position of the line terminator character.
     * Returns -1 if there is no next line.
     */
    static int nextLineStart(byte[] b, int lineEnd) {
        if (lineEnd >= b.length) return -1;
        byte term = b[lineEnd];
        if (term == (byte) '\r' && lineEnd + 1 < b.length && b[lineEnd + 1] == (byte) '\n') {
            return lineEnd + 2;
        }
        return lineEnd + 1;
    }

    /**
     * Returns true if the byte range [start, end) contains the given byte.
     */
    static boolean containsByte(byte[] b, int start, int end, byte target) {
        for (int i = start; i < end; i++) {
            if (b[i] == target) return true;
        }
        return false;
    }

    /**
     * Finds the next occurrence of target byte in [start, end).
     * Returns the index, or -1 if not found.
     */
    static int findByte(byte[] b, int start, int end, byte target) {
        for (int i = start; i < end; i++) {
            if (b[i] == target) return i;
        }
        return -1;
    }

    // ─── Header parsing ─────────────────────────────────────────────

    private static int[] parseHeaderAndBuildMapping(byte[] bytes, int start, int end,
                                                     Charset charset, byte delimiter,
                                                     List<String> schemaColumns,
                                                     String fileName) {
        // Parse header line into column names
        List<String> headerCols = new ArrayList<>();
        int fieldStart = start;
        for (int i = start; i <= end; i++) {
            if (i == end || bytes[i] == delimiter) {
                String col = new String(bytes, fieldStart, i - fieldStart, charset).trim();
                // Strip BOM from first column
                if (headerCols.isEmpty() && col.startsWith("\uFEFF")) {
                    col = col.substring(1);
                }
                headerCols.add(col);
                fieldStart = i + 1;
            }
        }

        // Build column mapping (schema → file)
        int[] mapping = new int[schemaColumns.size()];
        TreeMap<String, Integer> fileIndexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < headerCols.size(); i++) {
            String name = headerCols.get(i);
            if (!name.isEmpty()) {
                fileIndexByName.put(name, i);
            }
        }
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < schemaColumns.size(); i++) {
            String schemaCol = schemaColumns.get(i);
            Integer fileIdx = fileIndexByName.get(schemaCol);
            mapping[i] = (fileIdx != null) ? fileIdx : -1;
            if (fileIdx == null) {
                missing.add(schemaCol);
            }
        }
        if (!missing.isEmpty()) {
            String msg = "Header columns missing from file (required by schema): " + missing;
            String mode = System.getProperty("storage.header.mismatch.mode", "fail");
            if ("fail".equalsIgnoreCase(mode)) {
                throw new DieselIOException((fileName != null ? fileName + ":" : "") + msg,
                        new IOException(msg));
            } else {
                LOGGER.warn("{}{}", fileName != null ? fileName + ":" : "", msg);
            }
        }
        return mapping;
    }

    // ─── Data line parsing ──────────────────────────────────────────

    /**
     * Fallback: parse entire file via legacy CsvRowReader/TsvRowReader path.
     * Used when the byte[] fast path can't handle the file (e.g. quoted headers).
     */
    private static List<Object[]> parseViaLegacy(byte[] bytes, Charset charset,
                                                  List<String> columns, Map<String, Class<?>> columnTypes,
                                                  String fileName) {
        // Use DelimitedContent.decode which uses REPORT charset decoder
        // to throw on malformed input (matching production CsvRowReader behavior)
        java.nio.charset.CharsetDecoder decoder = charset.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
        String text;
        try {
            text = decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new DieselIOException(
                    "Malformed " + charset.name() + " input in delimited file " + fileName, e);
        }
        List<String> lines = new ArrayList<>();
        int start = 0;
        int len = text.length();
        while (start <= len) {
            int nl = text.indexOf('\n', start);
            int cr = text.indexOf('\r', start);
            int next;
            if (nl < 0 && cr < 0) {
                if (start < len) lines.add(text.substring(start));
                break;
            }
            if (nl < 0) next = cr;
            else if (cr < 0) next = nl;
            else next = Math.min(nl, cr);
            lines.add(text.substring(start, next));
            if (cr >= 0 && next + 1 < len && text.charAt(next + 1) == '\n') {
                start = next + 2;
            } else {
                start = next + 1;
            }
        }
        try (LineSource source = LineSource.over(lines);
             CsvRowReader csvReader = new CsvRowReader(source, columns, columnTypes, fileName)) {
            csvReader.readHeader();
            List<Object[]> loaded = new ArrayList<>(Math.max(16, lines.size()));
            while (csvReader.hasNext()) {
                Object[] row = csvReader.nextArray();
                if (row != null) {
                    loaded.add(row);
                }
            }
            return loaded;
        } catch (java.io.IOException e) {
            throw new DieselIOException("Failed to parse delimited file via legacy path: " + fileName, e);
        }
    }

    // ─── Data line parsing ──────────────────────────────────────────

    private static List<Object[]> parseDataLines(byte[] bytes, int dataStart, Charset charset,
                                                  List<String> columns, Map<String, Class<?>> columnTypes,
                                                  byte delimiter, byte quoteChar, String fileName,
                                                  int[] columnMapping) {
        // Build reverse mapping once: fileColIdx → schemaIdx
        int maxFileIdx = -1;
        for (int m : columnMapping) {
            if (m > maxFileIdx) maxFileIdx = m;
        }
        int[] reverseMapping = new int[maxFileIdx + 1];
        java.util.Arrays.fill(reverseMapping, -1);
        for (int s = 0; s < columnMapping.length; s++) {
            if (columnMapping[s] >= 0 && columnMapping[s] < reverseMapping.length) {
                reverseMapping[columnMapping[s]] = s;
            }
        }

        List<Object[]> rows = new ArrayList<>(Math.max(16, (bytes.length - dataStart) / 80));
        int lineStart = dataStart;
        long lineNumber = 2; // header is line 1, first data line is line 2

        while (lineStart < bytes.length) {
            // Single-pass scan: find line end and check for quotes simultaneously
            boolean hasQuote = false;
            int lineEnd = -1;
            for (int i = lineStart; i < bytes.length; i++) {
                byte c = bytes[i];
                if (c == (byte) '\n' || c == (byte) '\r') {
                    lineEnd = i;
                    break;
                }
                if (quoteChar != 0 && c == quoteChar) {
                    hasQuote = true;
                }
            }
            int lineLen;
            boolean hasTerminator;
            if (lineEnd < 0) {
                lineLen = bytes.length - lineStart;
                hasTerminator = false;
            } else {
                lineLen = lineEnd - lineStart;
                hasTerminator = true;
            }

            if (hasQuote) {
                String lineStr = new String(bytes, lineStart, lineLen, charset);
                StringBuilder sb = new StringBuilder(lineStr);
                boolean inQuotes = CsvRowReader.endsInsideQuotes(sb.toString());
                while (inQuotes && hasTerminator) {
                    int nextStart = nextLineStart(bytes, lineEnd);
                    if (nextStart < 0 || nextStart >= bytes.length) break;
                    lineEnd = findLineEnd(bytes, nextStart);
                    if (lineEnd < 0) {
                        lineLen = bytes.length - nextStart;
                        hasTerminator = false;
                    } else {
                        lineLen = lineEnd - nextStart;
                        hasTerminator = true;
                    }
                    String more = new String(bytes, nextStart, lineLen, charset);
                    sb.append('\n').append(more);
                    inQuotes = CsvRowReader.endsInsideQuotes(sb.toString());
                }
                Object[] row = parseLineViaLegacy(sb.toString(), columns, columnTypes, columnMapping);
                if (row != null) {
                    rows.add(row);
                }
            } else {
                Object[] row = parseUnquotedLine(bytes, lineStart, lineLen, reverseMapping,
                        columns, columnTypes, delimiter, charset, fileName, lineNumber);
                if (row != null) {
                    rows.add(row);
                }
            }

            if (lineEnd < 0) {
                break;
            }
            lineStart = nextLineStart(bytes, lineEnd);
            if (lineStart < 0 || lineStart >= bytes.length) {
                break;
            }
            lineNumber++;
        }
        return rows;
    }

    /**
     * Parse an unquoted line directly from bytes. Scans for delimiter bytes
     * and parses typed columns directly from byte ranges without creating
     * intermediate Strings.
     */
    private static Object[] parseUnquotedLine(byte[] bytes, int lineStart, int lineLen,
                                               int[] reverseMapping, List<String> columns,
                                               Map<String, Class<?>> columnTypes, byte delimiter,
                                               Charset charset, String fileName, long lineNumber) {
        int lineEnd = lineStart + lineLen;
        Object[] values = new Object[columns.size()];
        int fieldStart = lineStart;
        int fileColIdx = 0;

        // Scan fields separated by delimiter
        while (fieldStart <= lineEnd) {
            int commaPos = findByte(bytes, fieldStart, lineEnd, delimiter);
            int fieldEnd = (commaPos < 0) ? lineEnd : commaPos;
            int fieldLen = fieldEnd - fieldStart;

            // O(1) lookup via reverse mapping
            if (fileColIdx < reverseMapping.length) {
                int schemaIdx = reverseMapping[fileColIdx];
                if (schemaIdx >= 0) {
                    Class<?> type = columnTypes.get(columns.get(schemaIdx));
                    values[schemaIdx] = parseField(bytes, fieldStart, fieldLen, type, charset,
                            fileName, lineNumber, columns.get(schemaIdx));
                }
            }

            fileColIdx++;
            if (commaPos < 0) break;
            fieldStart = commaPos + 1;
        }

        // Warn if the data row has more fields than the schema
        if (fileColIdx > columns.size()) {
            org.slf4j.LoggerFactory.getLogger("diesel.storage.CsvRowReader").warn(
                    "{}line {}: row has {} fields but schema expects {} - ignoring extra fields",
                    fileName != null ? fileName + ":" : "", lineNumber, fileColIdx, columns.size());
        }

        return values;
    }

    /**
     * Parse a single field from byte range into the appropriate type.
     * Throws DieselIOException if the value can't be parsed and error mode is "fail".
     */
    private static Object parseField(byte[] b, int off, int len, Class<?> type, Charset charset,
                                      String fileName, long lineNumber, String colName) {
        if (len == 0) {
            return null; // empty field → null
        }

        // Check for non-ASCII bytes — if found, validate charset with REPORT action
        // and fall back to String path (matches DelimitedContent.decode behavior)
        boolean hasNonAscii = false;
        for (int i = off; i < off + len; i++) {
            if (b[i] < 0) {
                hasNonAscii = true;
                break;
            }
        }

        if (type == null || type == String.class) {
            if (hasNonAscii) {
                return decodeFieldWithValidation(b, off, len, charset, fileName);
            }
            return new String(b, off, len, charset);
        }

        if (hasNonAscii) {
            // Non-ASCII bytes in a typed column → validate and fall back to String then convert
            String s = decodeFieldWithValidation(b, off, len, charset, fileName);
            return convertStringToTypeThrowing(s, type, fileName, lineNumber, colName);
        }

        // ASCII fast-path for typed columns
        String typeName = type.getSimpleName();
        Object result = switch (typeName) {
            case "Long" -> parseLongAscii(b, off, len);
            case "Integer" -> parseIntAscii(b, off, len);
            case "Double" -> parseDoubleAscii(b, off, len);
            case "Float" -> parseFloatAscii(b, off, len);
            case "Boolean" -> parseBooleanAscii(b, off, len);
            case "BigDecimal" -> parseBigDecimalAscii(b, off, len);
            case "LocalDate" -> LocalDate.parse(new String(b, off, len, StandardCharsets.US_ASCII));
            case "LocalDateTime" -> LocalDateTime.parse(new String(b, off, len, StandardCharsets.US_ASCII));
            case "UUID" -> UUID.fromString(new String(b, off, len, StandardCharsets.US_ASCII));
            default -> new String(b, off, len, charset);
        };
        if (result == null) {
            // Fast-path parser returned null → invalid value, throw or handle per error mode
            String raw = new String(b, off, len, charset);
            return convertStringToTypeThrowing(raw, type, fileName, lineNumber, colName);
        }
        return result;
    }

    /**
     * Decode bytes to String using CharsetDecoder with REPORT action.
     * Throws DieselIOException on malformed input (matching DelimitedContent.decode behavior).
     */
    private static String decodeFieldWithValidation(byte[] b, int off, int len, Charset charset, String fileName) {
        java.nio.charset.CharsetDecoder decoder = charset.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
        try {
            return decoder.decode(java.nio.ByteBuffer.wrap(b, off, len)).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new DieselIOException(
                    "Malformed " + charset.name() + " input in delimited file " + fileName, e);
        }
    }

    /**
     * Convert a String to the target type, throwing DieselIOException on failure
     * when error mode is "fail".
     */
    private static Object convertStringToTypeThrowing(String s, Class<?> type,
                                                       String fileName, long lineNumber, String colName) {
        if (s.isEmpty()) return null;
        try {
            return switch (type.getSimpleName()) {
                case "Long" -> Long.parseLong(s);
                case "Integer" -> Integer.parseInt(s);
                case "Double" -> Double.parseDouble(s);
                case "Float" -> Float.parseFloat(s);
                case "Boolean" -> DelimitedRowReader.parseBooleanStrict(s);
                case "BigDecimal" -> new BigDecimal(s);
                case "LocalDate" -> LocalDate.parse(s);
                case "LocalDateTime" -> LocalDateTime.parse(s);
                case "UUID" -> UUID.fromString(s);
                default -> s;
            };
        } catch (RuntimeException e) {
            return handleConversionErrorThrowing(e, s, type.getSimpleName(), fileName, lineNumber, colName);
        }
    }

    private static Object handleConversionErrorThrowing(RuntimeException e, String raw, String typeName,
                                                         String fileName, long lineNumber, String colName) {
        String colInfo = colName != null ? " column '" + colName + "'" : "";
        String msg = (fileName != null ? fileName + ":" : "") + "line " + lineNumber + ":"
                + colInfo + " cannot parse \"" + raw + "\" as " + typeName;
        String mode = System.getProperty("storage.load.error.mode", "fail");
        if ("skip_value".equalsIgnoreCase(mode)) {
            LOGGER.warn(msg);
            return null;
        }
        if ("skip_row".equalsIgnoreCase(mode)) {
            LOGGER.warn(msg);
            return null; // caller handles skip_row via null row
        }
        throw new DieselIOException(msg, e);
    }

    /**
     * Parse a line via the legacy String-based path (for quoted fields and
     * multi-line CSV rows). Delegates to CsvRowReader's parseLine.
     */
    private static Object[] parseLineViaLegacy(String line, List<String> columns,
                                                Map<String, Class<?>> columnTypes, int[] columnMapping) {
        List<String> fields = CsvRowReader.parseLine(line);
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int fileIdx = columnMapping[i];
            boolean present = fileIdx >= 0 && fileIdx < fields.size();
            String field = present ? fields.get(fileIdx) : "";
            if (field.isEmpty()) {
                values[i] = null;
            } else {
                Class<?> type = columnTypes.get(columns.get(i));
                values[i] = convertStringToTypeThrowing(field, type, null, 0, columns.get(i));
            }
        }
        return values;
    }
}
