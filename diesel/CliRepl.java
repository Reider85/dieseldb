package diesel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.security.UserStore;
import diesel.backup.DumpFormat;
import diesel.backup.LogicalDump;
import diesel.backup.LogicalRestore;
import diesel.backup.PhysicalBackup;
import diesel.backup.PhysicalRestore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
     * Parsed command-line arguments of {@link #main} (prompt4 #31, #35).
     *
     * @param host       remote server host (default {@code localhost})
     * @param port       remote server port (default {@code 3306})
     * @param dataDir    local data directory for {@code --local}/{@code --reset-admin}
     * @param local      whether to run against a local {@link Database}
     * @param resetAdmin whether to reset the admin password and exit
     * @param user       login account, or null for no credentials
     * @param password   login password, or null
     * @param database   requested database name, or null for the default
     * @param tls        whether to use TLS encryption (requires remote mode)
     * @param tlsCa      path to CA/truststore file (PEM or PKCS12), or null for system truststore
     * @param tlsCert    path to client certificate file (PEM or PKCS12), or null
     * @param tlsKey     path to client private key file (PEM), or null (ignored if tlsCert is PKCS12)
     */
    record CliArgs(String host, int port, String dataDir, boolean local, boolean resetAdmin,
                   String user, String password, String database, boolean tls, String tlsCa, String tlsCert, String tlsKey) {
        static CliArgs defaults() {
            return new CliArgs("localhost", 3306, "data", false, false, null, null, null, false, null, null, null);
        }
    }

    /**
     * Parses {@code --local [dir]}, {@code --user u}, {@code --password p},
     * {@code --database d} and {@code --reset-admin} flags; remaining
     * positionals stay {@code [host] [port]} (remote) or {@code [dir]}
     * (local / reset-admin). Package-private for tests (prompt4 #31).
     */
    static CliArgs parseArgs(String[] args) {
        CliArgs d = CliArgs.defaults();
        String host = d.host();
        int port = d.port();
        String dataDir = d.dataDir();
        boolean local = false;
        boolean resetAdmin = false;
        String user = null;
        String password = null;
        String database = null;
        boolean tls = false;
        String tlsCa = null;
        String tlsCert = null;
        String tlsKey = null;
        List<String> positional = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--local":
                    local = true;
                    break;
                case "--reset-admin":
                    resetAdmin = true;
                    break;
                case "--user":
                    user = requireFlagValue(args, ++i, "--user");
                    break;
                case "--password":
                    password = requireFlagValue(args, ++i, "--password");
                    break;
                case "--database":
                    database = requireFlagValue(args, ++i, "--database");
                    break;
                case "--tls":
                    tls = true;
                    break;
                case "--tls-ca":
                    tlsCa = requireFlagValue(args, ++i, "--tls-ca");
                    break;
                case "--tls-cert":
                    tlsCert = requireFlagValue(args, ++i, "--tls-cert");
                    break;
                case "--tls-key":
                    tlsKey = requireFlagValue(args, ++i, "--tls-key");
                    break;
                default:
                    positional.add(args[i]);
                    break;
            }
        }
        
        // Validate TLS flags
        if (tls && local) {
            throw new IllegalArgumentException("--tls requires remote mode, not --local");
        }
        if (tlsCa != null && !tls) {
            throw new IllegalArgumentException("--tls-ca requires --tls");
        }
        if (tlsCert != null && !tls) {
            throw new IllegalArgumentException("--tls-cert requires --tls");
        }
        if (tlsKey != null && !tls) {
            throw new IllegalArgumentException("--tls-key requires --tls");
        }
        if (tlsKey != null && tlsCert == null) {
            throw new IllegalArgumentException("--tls-key requires --tls-cert");
        }
        
        if (local || resetAdmin) {
            if (!positional.isEmpty()) {
                dataDir = positional.get(0);
            }
        } else {
            if (positional.size() > 0) {
                host = positional.get(0);
            }
            if (positional.size() > 1) {
                try {
                    port = Integer.parseInt(positional.get(1));
                } catch (NumberFormatException e) {
                    LOGGER.warn("Invalid port {}, using default {}", positional.get(1), port);
                }
            }
        }
        return new CliArgs(host, port, dataDir, local, resetAdmin, user, password, database, tls, tlsCa, tlsCert, tlsKey);
    }

    private static String requireFlagValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + flag);
        }
        return args[index];
    }

    /**
     * Entry point. {@code --local [dataDir]} runs against a local database;
     * {@code --reset-admin [dataDir]} resets the admin account and exits
     * (filesystem access to the data directory is required, prompt4 #31);
     * otherwise flags {@code --user}/{@code --password}/{@code --database}
     * are combined with {@code [host] [port]} (defaults {@code localhost:3306}).
     *
     * @param args the command-line arguments
     */
    public static void main(String[] args) {
        CliArgs cli;
        try {
            cli = parseArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }
        if (cli.resetAdmin()) {
            resetAdmin(cli.dataDir());
            return;
        }
        if (cli.local()) {
            new CliRepl(new Database(cli.dataDir())).run();
            return;
        }
        
        // Remote mode: when no --user flag was given, ask for the login
        // interactively (password without echo via System.console()). An
        // empty login line or EOF keeps the old connect-without-credentials
        // behaviour, so scripted invocations stay non-blocking.
        String user = cli.user();
        String password = cli.password();
        if (user == null) {
            String[] credentials = promptCredentials();
            user = credentials[0];
            password = credentials[1];
        }
        
        // Load client config and merge with CLI flags
        Properties clientConfig = loadClientConfig();
        boolean tls = cli.tls();
        if (!tls && Boolean.parseBoolean(clientConfig.getProperty("tls.enabled", "false"))) {
            tls = true;
        }
        
        if (tls) {
            // Build TLS configuration
            Properties tlsConfig = new Properties();
            tlsConfig.setProperty("tls.enabled", "true");
            if (cli.tlsCa() != null) {
                tlsConfig.setProperty("tls.ca.path", cli.tlsCa());
            } else {
                String caPath = clientConfig.getProperty("tls.ca.path");
                if (caPath != null) {
                    tlsConfig.setProperty("tls.ca.path", caPath);
                }
            }
            if (cli.tlsCert() != null) {
                tlsConfig.setProperty("tls.cert.path", cli.tlsCert());
            } else {
                String certPath = clientConfig.getProperty("tls.cert.path");
                if (certPath != null) {
                    tlsConfig.setProperty("tls.cert.path", certPath);
                }
            }
            if (cli.tlsKey() != null) {
                tlsConfig.setProperty("tls.key.path", cli.tlsKey());
            } else {
                String keyPath = clientConfig.getProperty("tls.key.path");
                if (keyPath != null) {
                    tlsConfig.setProperty("tls.key.path", keyPath);
                }
            }
            // Optional passwords from config
            String trustPass = clientConfig.getProperty("tls.truststore.password");
            if (trustPass != null) {
                tlsConfig.setProperty("tls.truststore.password", trustPass);
            }
            String keyPass = clientConfig.getProperty("tls.keystore.password");
            if (keyPass != null) {
                tlsConfig.setProperty("tls.keystore.password", keyPass);
            }
            
            try {
                javax.net.ssl.SSLContext context = diesel.net.SslContextFactory.createClient(tlsConfig);
                javax.net.ssl.SSLSocketFactory factory = context.getSocketFactory();
                DatabaseClient client = user != null
                        ? new DatabaseClient(cli.host(), cli.port(), user, password, cli.database())
                        : new DatabaseClient(cli.host(), cli.port());
                client.setSslSocketFactory(factory);
                client.connect();
            } catch (Exception e) {
                LOGGER.error("TLS connection failed: {}", e.getMessage());
                if (e.getCause() != null) {
                    LOGGER.debug("Cause: {}", e.getCause().getMessage());
                }
                return;
            }
        }
        
        DatabaseClient client;
        if (tls) {
            client = user != null
                    ? new DatabaseClient(cli.host(), cli.port(), user, password, cli.database())
                    : new DatabaseClient(cli.host(), cli.port());
            // TLS connection already done above
        } else {
            client = user != null
                    ? new DatabaseClient(cli.host(), cli.port(), user, password, cli.database())
                    : new DatabaseClient(cli.host(), cli.port());
            try {
                client.connect();
            } catch (RuntimeException e) {
                LOGGER.error("Connection failed: {}", e.getMessage());
                return;
            }
        }
        
        if (client.isMustChangePassword()) {
            boolean changed = forcePasswordChange(client, user);
            if (!changed) {
                client.disconnect();
                return;
            }
        }
        new CliRepl(client).run();
    }

    /**
     * Loads client TLS configuration from diesel-client.conf if present.
     * Configuration keys (all optional, CLI flags override):
     * tls.enabled, tls.ca.path, tls.cert.path, tls.key.path,
     * tls.truststore.password, tls.keystore.password
     */
    private static Properties loadClientConfig() {
        Properties config = new Properties();
        String configPath = System.getProperty("diesel.client.config", "diesel-client.conf");
        try {
            java.nio.file.Path path = java.nio.file.Path.of(configPath);
            if (java.nio.file.Files.isRegularFile(path)) {
                try (var in = java.nio.file.Files.newBufferedReader(path)) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.startsWith("#")) {
                            continue;
                        }
                        int eq = line.indexOf('=');
                        if (eq > 0) {
                            String key = line.substring(0, eq).trim();
                            String value = line.substring(eq + 1).trim();
                            config.setProperty(key, value);
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Could not read client config {}: {}", configPath, e.getMessage());
        }
        return config;
    }

    /**
     * Resets the admin account to {@code admin}/{@code admin} with the
     * forced-change flag (prompt4 #31 {@code --reset-admin}). Deliberately
     * local-only: it opens the data directory directly, so it works only
     * with filesystem access and never over the wire.
     */
    static void resetAdmin(String dataDir) {
        Database database = null;
        try {
            database = new Database(dataDir);
            database.loadTablesFromDisk();
            database.getUserStore().resetAdminPassword();
            database.saveTablesToDisk();
            System.out.println("Admin password reset to '" + UserStore.DEFAULT_ADMIN_PASSWORD
                    + "' in data directory '" + database.getDataDir() + "'.");
            System.out.println("Password change is required at the next login.");
        } catch (RuntimeException e) {
            System.err.println("Reset failed: filesystem access to the data directory '"
                    + dataDir + "' is required: " + e.getMessage());
        } finally {
            if (database != null) {
                database.close();
            }
        }
    }

    /**
     * Runs the forced password change demanded after the first admin login
     * (prompt4 #31): up to {@value #PASSWORD_CHANGE_ATTEMPTS} attempts of
     * typing the new password twice, applied with {@code ALTER USER}.
     *
     * @return true when the password was changed successfully
     */
    static boolean forcePasswordChange(DatabaseClient client, String userName) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        out().println("Password must be changed at first login.");
        for (int attempt = 1; attempt <= PASSWORD_CHANGE_ATTEMPTS; attempt++) {
            try {
                out().print("New password: ");
                out().flush();
                String first = reader.readLine();
                if (first == null) {
                    return false;
                }
                out().print("Repeat new password: ");
                out().flush();
                String second = reader.readLine();
                if (first.isEmpty()) {
                    out().println("Password must not be empty.");
                    continue;
                }
                if (!first.equals(second)) {
                    out().println("Passwords do not match.");
                    continue;
                }
                client.executeQuery("ALTER USER " + userName + " PASSWORD '" + first + "'");
                out().println("Password changed.");
                return true;
            } catch (IOException e) {
                LOGGER.error("Error reading input: {}", e.getMessage());
                return false;
            } catch (RuntimeException e) {
                out().println("Password change failed: " + e.getMessage());
            }
        }
        out().println("Giving up after " + PASSWORD_CHANGE_ATTEMPTS + " attempts.");
        return false;
    }

    /**
     * Asks for the login interactively when no {@code --user} flag was given
     * ({@code start-client.sh} / plain {@code java CliRepl}). The password is
     * read without echo through {@link System#console()}; with piped stdin
     * (no console attached) both values come from plain lines. An empty
     * login line or end-of-input returns null credentials, keeping the
     * connect-without-credentials behaviour for scripted invocations.
     *
     * @return {@code {user, password}}, both null when nothing was entered
     */
    static String[] promptCredentials() {
        java.io.Console console = System.console();
        if (console != null) {
            String user = console.readLine("Login: ");
            if (user == null || user.trim().isEmpty()) {
                return new String[]{null, null};
            }
            char[] pwd = console.readPassword("Password: ");
            return new String[]{user.trim(), pwd == null ? null : new String(pwd)};
        }
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String user = reader.readLine();
            if (user == null || user.trim().isEmpty()) {
                return new String[]{null, null};
            }
            out().print("Password: ");
            out().flush();
            String password = reader.readLine();
            return new String[]{user.trim(), password};
        } catch (IOException e) {
            return new String[]{null, null};
        }
    }

    private static PrintWriter out() {
        return new PrintWriter(System.out, true);
    }

    private static final int PASSWORD_CHANGE_ATTEMPTS = 3;

    /**
     * Runs the REPL loop until EXIT/QUIT is entered, an empty line or EOF is
     * read, or an input error occurs. The connection is closed afterwards.
     */
    public void run() {
        printHelp();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = readCliLine(reader);
        while (line != null && !line.trim().isEmpty()
                && !line.trim().toUpperCase().equals(ErrorMessages.EXIT_COMMAND)
                && !line.trim().toUpperCase().equals("QUIT")) {
            out.print(PROMPT);
            out.flush();
            String query = line.trim();
            if (query.endsWith(";")) {
                query = query.substring(0, query.length() - 1).trim();
            }
            if (query.equalsIgnoreCase("HELP")) {
                printHelp();
            } else if (query.toUpperCase().startsWith("DUMP ")) {
                executeDump(query.substring(5).trim());
            } else if (query.toUpperCase().startsWith("BACKUP ")) {
                executeBackup(query.substring(7).trim());
            } else if (query.toUpperCase().startsWith("PHYSICAL-RESTORE ")) {
                executePhysicalRestore(query.substring(16).trim());
            } else if (query.toUpperCase().startsWith("RESTORE ")) {
                executeRestore(query.substring(9).trim());
            } else {
                execute(query);
            }
            line = readCliLine(reader);
        }
        disconnect();
    }

    private String readCliLine(BufferedReader reader) {
        try {
            return reader.readLine();
        } catch (IOException e) {
            LOGGER.error("Error reading input: {}", e.getMessage());
            return null;
        }
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
        extractMapsAndColumns(rows, maps, columns);
        if (maps.isEmpty()) {
            out.println(rows);
            return;
        }
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            widths[i] = columns.get(i).length();
        }
        List<List<String>> formattedRows = buildFormattedRows(maps, columns, widths);
        printBorder(columns.size(), widths);
        printRow(columns, widths);
        printBorder(columns.size(), widths);
        for (List<String> formattedRow : formattedRows) {
            printRow(formattedRow, widths);
        }
        printBorder(columns.size(), widths);
        out.println("(" + rows.size() + " rows)");
    }

    @SuppressWarnings("unchecked")
    private void extractMapsAndColumns(List<?> rows, List<Map<String, Object>> maps, List<String> columns) {
        Set<String> seenColumns = new LinkedHashSet<>();
        for (Object row : rows) {
            if (row instanceof Map<?, ?> map) {
                Map<String, Object> stringMap = (Map<String, Object>) map;
                maps.add(stringMap);
                seenColumns.addAll(stringMap.keySet());
            }
        }
        columns.addAll(seenColumns);
    }

    private List<List<String>> buildFormattedRows(List<Map<String, Object>> maps, List<String> columns, int[] widths) {
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
        return formattedRows;
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

    private void executeDump(String args) {
        try {
            // Parse arguments
            String[] parts = args.split("\\s+");
            if (parts.length == 0 || parts[0].isEmpty()) {
                out.println("Error: Path is required");
                out.println("Usage: DUMP <path> [FORMAT sql|csv|jsonl|avro] [SCHEMA] [INDEXES] [BATCH-SIZE N]");
                return;
            }
            
            Path outputPath = Path.of(parts[0]);
            DumpFormat format = DumpFormat.SQL; // default
            boolean includeSchema = false;
            boolean includeIndexes = false;
            int batchSize = 1000; // default
            
            // Parse options
            for (int i = 1; i < parts.length; i++) {
                String part = parts[i].toUpperCase();
                if (part.equals("FORMAT") && i + 1 < parts.length) {
                    String formatStr = parts[i + 1].toUpperCase();
                    try {
                        format = DumpFormat.valueOf(formatStr);
                        i++; // skip next token
                    } catch (IllegalArgumentException e) {
                        out.println("Error: Invalid format '" + parts[i + 1] + "'. Valid formats: sql, csv, jsonl, avro");
                        return;
                    }
                } else if (part.equals("SCHEMA")) {
                    includeSchema = true;
                } else if (part.equals("INDEXES")) {
                    includeIndexes = true;
                } else if (part.equals("BATCH-SIZE") && i + 1 < parts.length) {
                    try {
                        batchSize = Integer.parseInt(parts[i + 1]);
                        if (batchSize <= 0) {
                            out.println("Error: Batch size must be positive");
                            return;
                        }
                        i++; // skip next token
                    } catch (NumberFormatException e) {
                        out.println("Error: Invalid batch size '" + parts[i + 1] + "'");
                        return;
                    }
                } else {
                    out.println("Error: Unknown option '" + parts[i] + "'");
                    out.println("Usage: DUMP <path> [FORMAT sql|csv|jsonl|avro] [SCHEMA] [INDEXES] [BATCH-SIZE N]");
                    return;
                }
            }
            
            // Get database instance
            Database db = database;
            if (db == null) {
                out.println("Error: No database connection available");
                return;
            }
            
            // Execute dump
            LogicalDump dump = new LogicalDump(db, format, outputPath, includeSchema, includeIndexes, batchSize);
            dump.execute();
            out.println("Dump completed successfully to: " + outputPath);
            
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
        }
    }

    private void executeBackup(String args) {
        try {
            // Parse arguments
            String[] parts = args.split("\\s+");
            if (parts.length == 0 || parts[0].isEmpty()) {
                out.println("Error: Backup directory path is required");
                out.println("Usage: BACKUP <backup-dir>");
                return;
            }
            
            Path backupDir = Path.of(parts[0]);
            
            // Get database instance
            Database db = database;
            if (db == null) {
                out.println("Error: No database connection available");
                return;
            }
            
            // Execute physical backup
            PhysicalBackup backup = new PhysicalBackup(Path.of(db.getDataDir()), backupDir);
            backup.backup();
            out.println("Physical backup completed successfully to: " + backupDir);
            
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
        }
    }

    private void executePhysicalRestore(String args) {
        try {
            // Parse arguments
            String[] parts = args.split("\\s+");
            if (parts.length < 2) {
                out.println("Error: Backup directory and target directory are required");
                out.println("Usage: PHYSICAL-RESTORE <backup-dir> <target-dir>");
                return;
            }
            
            Path backupDir = Path.of(parts[0]);
            Path targetDir = Path.of(parts[1]);
            
            // Get database instance (for data dir reference)
            Database db = database;
            if (db == null) {
                out.println("Error: No database connection available");
                return;
            }
            
            // Execute physical restore
            PhysicalRestore restore = new PhysicalRestore(backupDir, targetDir);
            restore.restore();
            out.println("Physical restore completed successfully from: " + backupDir + " to: " + targetDir);
            
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
        }
    }

    private void executeRestore(String args) {
        try {
            // Parse arguments
            String[] parts = args.split("\\s+");
            if (parts.length == 0 || parts[0].isEmpty()) {
                out.println("Error: Path is required");
                out.println("Usage: RESTORE <path> [FORMAT sql|csv|jsonl|avro] [ON-CONFLICT error|replace|skip] [CREATE-TABLES] [BATCH-SIZE N] [VALIDATE]");
                return;
            }
            
            Path inputPath = Path.of(parts[0]);
            DumpFormat format = DumpFormat.SQL; // default
            LogicalRestore.OnConflictStrategy conflictStrategy = LogicalRestore.OnConflictStrategy.ERROR; // default
            boolean createTables = false;
            int batchSize = 1000; // default
            boolean validateData = false;
            
            // Parse options
            for (int i = 1; i < parts.length; i++) {
                String part = parts[i].toUpperCase();
                if (part.equals("FORMAT") && i + 1 < parts.length) {
                    String formatStr = parts[i + 1].toUpperCase();
                    try {
                        format = DumpFormat.valueOf(formatStr);
                        i++; // skip next token
                    } catch (IllegalArgumentException e) {
                        out.println("Error: Invalid format '" + parts[i + 1] + "'. Valid formats: sql, csv, jsonl, avro");
                        return;
                    }
                } else if (part.equals("ON-CONFLICT") && i + 1 < parts.length) {
                    String strategyStr = parts[i + 1].toUpperCase();
                    try {
                        conflictStrategy = LogicalRestore.OnConflictStrategy.valueOf(strategyStr);
                        i++; // skip next token
                    } catch (IllegalArgumentException e) {
                        out.println("Error: Invalid conflict strategy '" + parts[i + 1] + "'. Valid strategies: error, replace, skip");
                        return;
                    }
                } else if (part.equals("CREATE-TABLES")) {
                    createTables = true;
                } else if (part.equals("BATCH-SIZE") && i + 1 < parts.length) {
                    try {
                        batchSize = Integer.parseInt(parts[i + 1]);
                        if (batchSize <= 0) {
                            out.println("Error: Batch size must be positive");
                            return;
                        }
                        i++; // skip next token
                    } catch (NumberFormatException e) {
                        out.println("Error: Invalid batch size '" + parts[i + 1] + "'");
                        return;
                    }
                } else if (part.equals("VALIDATE")) {
                    validateData = true;
                } else {
                    out.println("Error: Unknown option '" + parts[i] + "'");
                    out.println("Usage: RESTORE <path> [FORMAT sql|csv|jsonl|avro] [ON-CONFLICT error|replace|skip] [CREATE-TABLES] [BATCH-SIZE N] [VALIDATE]");
                    return;
                }
            }
            
            // Get database instance
            Database db = database;
            if (db == null) {
                out.println("Error: No database connection available");
                return;
            }
            
            // Execute restore
            LogicalRestore restore = new LogicalRestore(db, inputPath, format, conflictStrategy, createTables, batchSize, validateData);
            restore.execute();
            out.println("Restore completed successfully from: " + inputPath);
            
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
        }
    }

    private void printHelp() {
        out.println("DieselDB CLI REPL");
        out.println("Type SQL queries and press Enter to execute them.");
        out.println("Commands:");
        out.println("  HELP                    show this help");
        out.println("  DUMP <path> [options]  dump database to file (see diesel_dump tool for full options)");
        out.println("  BACKUP <dir>           create physical backup of database (see diesel_backup tool for full options)");
        out.println("  PHYSICAL-RESTORE <backup-dir> <target-dir>  restore database from physical backup (see diesel_restore tool for full options)");
        out.println("  RESTORE <path> [opt]   restore database from file (see diesel_restore tool for full options)");
        out.println("  EXIT or QUIT            exit the REPL");
        out.println("Usage:");
        out.println("  CliRepl [host] [port]                  connect to a running server (default localhost:3306)");
        out.println("  CliRepl --local [dir]                  use a local database directly (default dir data)");
        out.println("  CliRepl --user u --password p [--db d] login on connect (prompt4 #31)");
        out.println("  without --user you are asked for login and password interactively");
        out.println("  CliRepl --reset-admin [dir]            reset admin/admin locally, requires filesystem access");
        out.println("TLS options (remote mode only):");
        out.println("  --tls                         enable TLS encryption");
        out.println("  --tls-ca <path>              CA/truststore file (PEM or PKCS12)");
        out.println("  --tls-cert <path>            client certificate file (PEM or PKCS12)");
        out.println("  --tls-key <path>             client private key file (PEM, required if --tls-cert is PEM)");
    }

    private void disconnect() {
        if (client != null) {
            client.disconnect();
        }
    }
}
