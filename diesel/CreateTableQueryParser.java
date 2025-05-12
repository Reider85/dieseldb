package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

class CreateTableQueryParser {
    private static final Logger LOGGER = Logger.getLogger(CreateTableQueryParser.class.getName());

    Query<Void> parseCreateTableQuery(String normalized, String original) {
        int firstParen = original.indexOf('(');
        int lastParen = original.lastIndexOf(')');
        if (firstParen == -1 || lastParen == -1 || lastParen < firstParen) {
            throw new IllegalArgumentException("Invalid CREATE TABLE query format: missing or mismatched parentheses");
        }

        String tableName = original.substring(0, firstParen).replace("CREATE TABLE", "").trim();
        String columnsPart = original.substring(firstParen + 1, lastParen).trim();

        List<String> columnDefs = Splitter.splitColumnDefinitions(columnsPart);
        List<String> columns = new ArrayList<>();
        Map<String, Class<?>> columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Sequence> sequences = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String primaryKeyColumn = null;

        for (String colDef : columnDefs) {
            String[] colParts = colDef.trim().split("\\s+", 3);
            if (colParts.length < 2) {
                throw new IllegalArgumentException("Invalid column definition: " + colDef);
            }
            String colName = colParts[0];
            String type = colParts[1].toUpperCase();
            String constraints = colParts.length > 2 ? colParts[2].toUpperCase() : "";
            boolean isPrimaryKey = constraints.contains("PRIMARY KEY");
            boolean hasSequence = constraints.contains("SEQUENCE") || type.endsWith("_SEQUENCE");

            columns.add(colName);

            if (hasSequence) {
                if (!type.equals("LONG") && !type.equals("INTEGER") && !type.startsWith("LONG_SEQUENCE") && !type.startsWith("INTEGER_SEQUENCE")) {
                    throw new IllegalArgumentException("Sequence is only supported for LONG or INTEGER types: " + colDef);
                }
                String seqDef;
                if (type.endsWith("_SEQUENCE")) {
                    seqDef = colParts.length > 2 ? colParts[2].replaceAll(".*SEQUENCE\\(([^)]+)\\).*", "$1").trim() : "";
                } else {
                    seqDef = constraints.replaceAll(".*SEQUENCE\\(([^)]+)\\).*", "$1").trim();
                }
                String[] seqParts = seqDef.split("\\s+");
                if (seqParts.length < 3) {
                    throw new IllegalArgumentException("Invalid SEQUENCE definition in column: " + colDef);
                }
                String seqName = seqParts[0];
                long start = Long.parseLong(seqParts[1]);
                long increment = Long.parseLong(seqParts[2]);
                Class<?> seqType = type.equals("LONG") || type.startsWith("LONG_SEQUENCE") ? Long.class : Integer.class;
                sequences.put(colName, new Sequence(seqName, seqType, start, increment));
                columnTypes.put(colName, seqType);
            } else {
                switch (type) {
                    case "STRING":
                        columnTypes.put(colName, String.class);
                        break;
                    case "INTEGER":
                        columnTypes.put(colName, Integer.class);
                        break;
                    case "LONG":
                        columnTypes.put(colName, Long.class);
                        break;
                    case "SHORT":
                        columnTypes.put(colName, Short.class);
                        break;
                    case "BYTE":
                        columnTypes.put(colName, Byte.class);
                        break;
                    case "BIGDECIMAL":
                        columnTypes.put(colName, BigDecimal.class);
                        break;
                    case "FLOAT":
                        columnTypes.put(colName, Float.class);
                        break;
                    case "DOUBLE":
                        columnTypes.put(colName, Double.class);
                        break;
                    case "CHAR":
                        columnTypes.put(colName, Character.class);
                        break;
                    case "UUID":
                        columnTypes.put(colName, UUID.class);
                        break;
                    case "BOOLEAN":
                        columnTypes.put(colName, Boolean.class);
                        break;
                    case "DATE":
                        columnTypes.put(colName, LocalDate.class);
                        break;
                    case "DATETIME":
                    case "DATETIME_MS":
                        columnTypes.put(colName, LocalDateTime.class);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported column type: " + type);
                }
            }

            if (isPrimaryKey) {
                if (primaryKeyColumn != null) {
                    throw new IllegalArgumentException("Multiple primary keys defined in table " + tableName);
                }
                primaryKeyColumn = colName;
            }
        }

        LOGGER.log(Level.INFO, "Parsed CREATE TABLE query: table={0}, columns={1}, types={2}, primaryKey={3}, sequences={4}",
                new Object[]{tableName, columns, columnTypes, primaryKeyColumn, sequences.keySet()});

        return new CreateTableQuery(tableName, columns, columnTypes, primaryKeyColumn, sequences);
    }


}