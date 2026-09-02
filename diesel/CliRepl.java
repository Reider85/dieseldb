package diesel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Interactive command-line REPL for manual testing of DieselDB.
 *
 * <p>It accepts SQL queries from stdin and prints the results. Two modes are
 * supported: remote mode wraps a {@link DatabaseClient} connected to a live
 * {@link DatabaseServer} (so transactions and error responses behave exactly
 * as over the wire), while local mode creates a {@link Database} directly and
 * executes queries in memory with auto-commit on by default.
 *
 * <p>The loop prompts with {@code diesel> }, ends on EXIT/QUIT or EOF, prints
 * HELP on request, strips a trailing semicolon, prints SELECT results as an
 * aligned column table with a row count, null results as OK, String results
 * as-is and errors on a single {@code Error: } line.
 *
 * @see DatabaseClient
 * @see Database
 */
@SuppressWarnings("java:S106")
public class CliRepl {
    private static final Logger LOGGER = LoggerFactory.getLogger(CliRepl.class);
    private static final String PROMPT = "diesel> ";
    private static final String ERROR_PREFIX = "Error: ";

    private final DatabaseClient client;
    private final Database database;
    private final PrintWriter out;

    /**
     * Creates a REPL in remote mode backed by the given client.
     *
     * @param client the connected database client
     */
    public CliRepl(DatabaseClient client) {
        this.client = client;
        this.database = null;
        this.out = new PrintWriter(System.out, true);
    }

    /**
     * Creates a REPL in local mode backed by the given in-memory database.
     *
     * @param database the database to run queries against
     */
    public CliRepl(Database database) {
        this.client = null;
        this.database = database;
        this.out = new PrintWriter(System.out, true);
    }

    /**
     * Entry point. With {@code --local [dataDir]} a local in-memory database
     * is used; otherwise the arguments are interpreted as {@code [host] [port]}
     * (defaults {@code localhost:3306}) and the REPL connects to the server,
     * printing the error and exiting when the connection fails.
     *
     * @param args the command-line arguments
     */
    public static void main(String[] args) {
        if (args.length > 0 && "--local".equals(args[0])) {
            String dataDir = args.length > 1 ? args[1] : "data";
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
            LOGGER.error("Connection failed: {}", e.getMessage());
            return;
        }
        new CliRepl(client).run();
    }

    /**
     * Runs the REPL loop until EXIT/QUIT is entered, an empty line or EOF is
     * read, or an input error occurs. The connection is closed afterwards.
     */
    public void run() {
        printHelp();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            out.print(PROMPT);
            out.flush();
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
            if (command.equals(ErrorMessages.EXIT_COMMAND) || command.equals("QUIT")) {
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
            LOGGER.error("{}", message);
            return;
        }
        printResult(result);
    }

    private void printResult(Object result) {
        if (result == null) {
            out.println("OK");
        } else if (result instanceof List<?> list) {
            printTable(list);
        } else {
            out.println(result);
        }
    }

    private void printTable(List<?> rows) {
        if (rows.isEmpty()) {
            out.println("(0 rows)");
            return;
        }
        List<Map<String, Object>> maps = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        for (Object row : rows) {
            if (row instanceof Map<?, ?> map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stringMap = (Map<String, Object>) map;
                maps.add(stringMap);
                for (String key : stringMap.keySet()) {
                    if (!columns.contains(key)) {
                        columns.add(key);
                    }
                }
            }
        }
        if (maps.isEmpty()) {
            out.println(rows);
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
        out.println("(" + rows.size() + " rows)");
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
        out.println(sb);
    }

    private void printBorder(int columnCount, int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int i = 0; i < columnCount; i++) {
            for (int j = 0; j < widths[i] + 2; j++) {
                sb.append('-');
            }
            sb.append('+');
        }
        out.println(sb);
    }

    private void printHelp() {
        out.println("DieselDB CLI REPL");
        out.println("Type SQL queries and press Enter to execute them.");
        out.println("Commands:");
        out.println("  HELP                    show this help");
        out.println("  EXIT or QUIT            exit the REPL");
        out.println("Usage:");
        out.println("  CliRepl [host] [port]   connect to a running server (default localhost:3306)");
        out.println("  CliRepl --local [dir]   use a local database directly (default dir .)");
    }

    private void disconnect() {
        if (client != null) {
            client.disconnect();
        }
    }
}
