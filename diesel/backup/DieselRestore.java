package diesel.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Command-line interface for diesel_restore tool.
 * Restores DieselDB databases from physical backups.
 */
public class DieselRestore {
    
    private static final String VERSION = "1.0.0";
    
    /**
     * Runs the CLI and returns an exit code. Split from {@link #main} so tests
     * can exercise argument parsing without {@code System.exit} killing the JVM.
     */
    public static int run(String[] args) {
        if (args.length == 0) {
            printUsage();
            return 1;
        }

        try {
            if (args[0].equals("--help") || args[0].equals("-h")) {
                printUsage();
                return 0;
            }
            if (args[0].equals("--version")) {
                System.out.println("diesel_restore " + VERSION);
                return 0;
            }
            if (args[0].equals("restore")) {
                return handleRestoreCommand(args);
            }
            System.err.println("Unknown command: " + args[0]);
            printUsage();
            return 1;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    private static int handleRestoreCommand(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: diesel_restore restore <backup-dir> <target-dir> "
                    + "[--pitr <timestamp>] [--wal-dir <dir>] [--yes]");
            return 1;
        }

        String backupDirPath = args[1];
        String targetDirPath = args[2];

        String pitrTimestamp = null;
        String walDirPath = null;
        boolean assumeYes = false;
        for (int i = 3; i < args.length; i++) {
            switch (args[i]) {
                case "--pitr" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("Error: --pitr requires a timestamp argument");
                        return 1;
                    }
                    pitrTimestamp = args[++i];
                }
                case "--wal-dir" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("Error: --wal-dir requires a directory argument");
                        return 1;
                    }
                    walDirPath = args[++i];
                }
                case "--yes" -> assumeYes = true;
                default -> {
                    System.err.println("Unknown restore option: " + args[i]);
                    printUsage();
                    return 1;
                }
            }
        }

        Path backupDir = Paths.get(backupDirPath);
        Path targetDir = Paths.get(targetDirPath);
        
        // Validate backup directory
        if (!PhysicalRestore.validateBackupDirectory(backupDir)) {
            System.err.println("Error: Invalid backup directory: " + backupDirPath);
            System.err.println("The backup directory must contain:");
            System.err.println("  - backup_manifest.json");
            System.err.println("  - Data files (.table, .csv, etc.)");
            System.err.println("  - WAL files (.log)");
            return 1;
        }
        
        // Check if target directory is safe
        PhysicalRestore restore = new PhysicalRestore(backupDir, targetDir);
        if (!restore.isTargetSafe()) {
            System.err.println("Error: Target directory is not empty: " + targetDirPath);
            System.err.println("Restore requires an empty target directory.");
            System.err.println("Use a different directory or empty it first.");
            return 1;
        }
        
        System.out.println("DieselDB Physical Restore Tool");
        System.out.println("===============================");
        System.out.println("Backup: " + backupDirPath);
        System.out.println("Target: " + targetDirPath);
        if (pitrTimestamp != null) {
            System.out.println("PITR target: " + pitrTimestamp);
        }
        System.out.println();
        
        // Display backup information
        restore.displayBackupInfo();
        System.out.println();
        
        // Confirm restore (skipped with --yes for non-interactive PITR runs)
        if (!assumeYes) {
            System.out.print("Proceed with restore? (yes/no): ");
            java.util.Scanner scanner = new java.util.Scanner(System.in);
            String response = scanner.nextLine().trim().toLowerCase();
            scanner.close();

            if (!response.equals("yes") && !response.equals("y")) {
                System.out.println("Restore cancelled.");
                return 0;
            }
        }
        
        try {
            if (pitrTimestamp != null) {
                // Point-in-time restore: chain-restore, then replay the staged
                // WAL up to the requested timestamp (prompt 4 #43).
                long targetMs = PitrManager.parseTimestamp(pitrTimestamp);
                Path walDir = walDirPath != null
                        ? Paths.get(walDirPath)
                        : backupDir;
                diesel.recovery.PitrResult result = PitrManager.recoverToTimestamp(
                        backupDir, java.util.List.of(), walDir, targetDir, targetMs);
                System.out.println();
                System.out.println("Point-in-time restore completed successfully!");
                System.out.println(result);
            } else {
                // Perform restore
                restore.restore();

                System.out.println();
                System.out.println("Restore completed successfully!");
            }

            System.out.println();
            System.out.println("Next steps:");
            System.out.println("1. Start the DieselDB server with the restored database:");
            System.out.println("   diesel_server " + targetDirPath);
            System.out.println("2. Verify the database integrity:");
            System.out.println("   diesel_cli --database " + targetDirPath + " \"SELECT COUNT(*) FROM information_schema.tables\"");

        } catch (Exception e) {
            System.err.println("Restore failed: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
        return 0;
    }
    
    private static void printUsage() {
        System.out.println("DieselDB Physical Restore Tool v" + VERSION);
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  diesel_restore restore <backup-dir> <target-dir> [--pitr <timestamp>] [--wal-dir <dir>] [--yes]");
        System.out.println("  diesel_restore --help");
        System.out.println("  diesel_restore --version");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  restore     Restore a database from a physical backup");
        System.out.println("  --help      Show this help message");
        System.out.println("  --version   Show version information");
        System.out.println();
        System.out.println("Restore options:");
        System.out.println("  --pitr <timestamp>  Point-in-time restore (prompt4 #43): replay the WAL up to");
        System.out.println("                       <timestamp> (yyyy-MM-dd HH:mm:ss). Requires a valid WAL");
        System.out.println("                       directory (defaults to the backup dir, override with --wal-dir).");
        System.out.println("  --wal-dir <dir>     Directory holding the WAL segments to replay for --pitr.");
        System.out.println("  --yes               Skip the interactive confirmation (for scripted PITR runs).");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  diesel_restore restore /path/to/backup /path/to/restored_database");
        System.out.println("  diesel_restore restore /path/to/backup /path/to/restored \\");
        System.out.println("      --pitr '2026-10-05 14:30:00' --wal-dir /path/to/wal --yes");
        System.out.println();
        System.out.println("Environment Variables:");
        System.out.println("  DIESEL_HOME    Path to DieselDB installation directory");
        System.out.println();
        System.out.println("The restore process:");
        System.out.println("1. Copies all backup files to the target directory");
        System.out.println("2. Replays WAL segments to bring database to consistent state");
        System.out.println("3. Validates the restored database integrity");
        System.out.println();
        System.out.println("Important:");
        System.out.println("- The target directory must be empty");
        System.out.println("- The backup directory must be a valid DieselDB backup");
        System.out.println("- This will completely replace any existing database at the target location");
    }
}