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
    
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }
        
        try {
            if (args[0].equals("--help") || args[0].equals("-h")) {
                printUsage();
                System.exit(0);
            } else if (args[0].equals("--version")) {
                System.out.println("diesel_restore " + VERSION);
                System.exit(0);
            } else if (args[0].equals("restore")) {
                handleRestoreCommand(args);
            } else {
                System.err.println("Unknown command: " + args[0]);
                printUsage();
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void handleRestoreCommand(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: diesel_restore restore <backup-dir> <target-dir>");
            System.exit(1);
        }
        
        String backupDirPath = args[1];
        String targetDirPath = args[2];
        
        Path backupDir = Paths.get(backupDirPath);
        Path targetDir = Paths.get(targetDirPath);
        
        // Validate backup directory
        if (!PhysicalRestore.validateBackupDirectory(backupDir)) {
            System.err.println("Error: Invalid backup directory: " + backupDirPath);
            System.err.println("The backup directory must contain:");
            System.err.println("  - backup_manifest.json");
            System.err.println("  - Data files (.table, .csv, etc.)");
            System.err.println("  - WAL files (.log)");
            System.exit(1);
        }
        
        // Check if target directory is safe
        PhysicalRestore restore = new PhysicalRestore(backupDir, targetDir);
        if (!restore.isTargetSafe()) {
            System.err.println("Error: Target directory is not empty: " + targetDirPath);
            System.err.println("Restore requires an empty target directory.");
            System.err.println("Use a different directory or empty it first.");
            System.exit(1);
        }
        
        System.out.println("DieselDB Physical Restore Tool");
        System.out.println("===============================");
        System.out.println("Backup: " + backupDirPath);
        System.out.println("Target: " + targetDirPath);
        System.out.println();
        
        // Display backup information
        restore.displayBackupInfo();
        System.out.println();
        
        // Confirm restore
        System.out.print("Proceed with restore? (yes/no): ");
        java.util.Scanner scanner = new java.util.Scanner(System.in);
        String response = scanner.nextLine().trim().toLowerCase();
        
        if (!response.equals("yes") && !response.equals("y")) {
            System.out.println("Restore cancelled.");
            System.exit(0);
        }
        
        try {
            // Perform restore
            restore.restore();
            
            System.out.println();
            System.out.println("Restore completed successfully!");
            System.out.println();
            System.out.println("Next steps:");
            System.out.println("1. Start the DieselDB server with the restored database:");
            System.out.println("   diesel_server " + targetDirPath);
            System.out.println("2. Verify the database integrity:");
            System.out.println("   diesel_cli --database " + targetDirPath + " \"SELECT COUNT(*) FROM information_schema.tables\"");
            
        } catch (Exception e) {
            System.err.println("Restore failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            scanner.close();
        }
    }
    
    private static void printUsage() {
        System.out.println("DieselDB Physical Restore Tool v" + VERSION);
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  diesel_restore restore <backup-dir> <target-dir>");
        System.out.println("  diesel_restore --help");
        System.out.println("  diesel_restore --version");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  restore   Restore a database from a physical backup");
        System.out.println("  --help    Show this help message");
        System.out.println("  --version Show version information");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  diesel_restore restore /path/to/backup /path/to/restored_database");
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