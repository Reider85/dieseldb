package diesel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CliRepl {
    private static final Logger LOGGER = LoggerFactory.getLogger(CliRepl.class);
    private static final String PROMPT = "diesel> ";
    private static final String ERROR_PREFIX = "Error: ";

    private final DatabaseClient client;
    private final Database database;

    public CliRepl(DatabaseClient client) {
        this.client = client;
        this.database = null;
    }

    public CliRepl(Database database) {
        this.client = null;
        this.database = database;
    }

    public static void main(String[] args) {
        if (args.length > 0 && "--local".equals(args[0])) {
            String dataDir = args.length > 1 ? args[1] : ".";
            new CliRepl(new Database(dataDir)).run();
            return;
        }
        String host = "localhost";
        int port = 3306;
        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid port {}, using default {}", args[1], port);
            }
        }
        DatabaseClient client = new DatabaseClient(host, port);
        try {
            client.connect();
        } catch (RuntimeException e) {
            System.out.println(ERROR_PREFIX + e.getMessage());
            return;
        }
        new CliRepl(client).run();
    }

    public void run() {
        printHelp();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print(PROMPT);
            System.out.flush();
            String line;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                LOGGER.error("Error reading input: {}", e.getMessage());
                break;
            }
            if (line == null || line.trim().isEmpty()) {
                break;
            }
            String query = line.trim();
            if (query.endsWith(";")) {
                query = query.substring(0, query.length() - 1).trim();
            }
            if (query.isEmpty()) {
                break;
            }
            String command = query.toUpperCase();
            if (command.equals("EXIT") || command.equals("QUIT")) {
                break;
            }
            if (command.equals("HELP")) {
                printHelp();
                continue;
            }
            execute(query);
        }
        disconnect();
    }

    private void execute(String query) {
        Object result;
        try {
            result = client != null ? client.executeQuery(query) : database.executeQuery(query, null);
        } catch (RuntimeException e) {
            String message = e.getMessage();
            if (message == null) {
                message = "unknown error";
            } else if (message.startsWith(ERROR_PREFIX)) {
                message = message.substring(ERROR_PREFIX.length());
            }
            System.out.println(ERROR_PREFIX + message);
            return;
        }
        printResult(result);
    }

    private void printResult(Object result) {
        if (result == null) {
            System.out.println("OK");
        } else if (result instanceof List) {
            printTable((List<?>) result);
        } else {
            System.out.println(result);
        }
    }

    private void printTable(List<?> rows) {
        if (rows.isEmpty()) {
            System.out.println("(0 rows)");
            return;
        }
        List<Map<String, Object>> maps = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        for (Object row : rows) {
            if (row instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) row;
                maps.add(map);
                for (String key : map.keySet()) {
                    if (!columns.contains(key)) {
                        columns.add(key);
                    }
                }
            }
        }
        if (maps.isEmpty()) {
            System.out.println(rows);
            return;
        }
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            widths[i] = columns.get(i).length();
        }
        List<List<String>> formattedRows = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            List<String> formattedRow = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                Object value = map.get(columns.get(i));
                String text = value == null ? "null" : value.toString();
                formattedRow.add(text);
                if (text.length() > widths[i]) {
                    widths[i] = text.length();
                }
            }
            formattedRows.add(formattedRow);
        }
        printBorder(columns.size(), widths);
        printRow(columns, widths);
        printBorder(columns.size(), widths);
        for (List<String> formattedRow : formattedRows) {
            printRow(formattedRow, widths);
        }
        printBorder(columns.size(), widths);
        System.out.println("(" + rows.size() + " rows)");
    }

    private void printRow(List<String> cells, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < cells.size(); i++) {
            sb.append(' ').append(cells.get(i));
            for (int j = cells.get(i).length(); j < widths[i]; j++) {
                sb.append(' ');
            }
            sb.append(" |");
        }
        System.out.println(sb);
    }

    private void printBorder(int columnCount, int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int i = 0; i < columnCount; i++) {
            for (int j = 0; j < widths[i] + 2; j++) {
                sb.append('-');
            }
            sb.append('+');
        }
        System.out.println(sb);
    }

    private void printHelp() {
        System.out.println("DieselDB CLI REPL");
        System.out.println("Type SQL queries and press Enter to execute them.");
        System.out.println("Commands:");
        System.out.println("  HELP                    show this help");
        System.out.println("  EXIT or QUIT            exit the REPL");
        System.out.println("Usage:");
        System.out.println("  CliRepl [host] [port]   connect to a running server (default localhost:3306)");
        System.out.println("  CliRepl --local [dir]   use a local database directly (default dir .)");
    }

    private void disconnect() {
        if (client != null) {
            client.disconnect();
        }
    }
}
