package diesel.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Command-line interface for diesel_backup tool.
 * Creates physical backups of DieselDB databases.
 */
public class DieselBackup {
    
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
                System.out.println("diesel_backup " + VERSION);
                System.exit(0);
            } else if (args[0].equals("backup")) {
                handleBackupCommand(args);
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
    
    private static void handleBackupCommand(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: diesel_backup backup <database-dir> <backup-dir>");
            System.exit(1);
        }
        
        String databaseDirPath = args[1];
        String backupDirPath = args[2];
        
        Path databaseDir = Paths.get(databaseDirPath);
        Path backupDir = Paths.get(backupDirPath);
        
        // Validate database directory
        if (!databaseDir.toFile().exists()) {
            System.err.println("Error: Database directory does not exist: " + databaseDirPath);
            System.exit(1);
        }
        
        if (!databaseDir.toFile().isDirectory()) {
            System.err.println("Error: Database path is not a directory: " + databaseDirPath);
            System.exit(1);
        }
        
        // Check if backup directory already exists
        if (backupDir.toFile().exists()) {
            if (!backupDir.toFile().isDirectory()) {
                System.err.println("Error: Backup path exists but is not a directory: " + backupDirPath);
                System.exit(1);
            }
            
            if (backupDir.toFile().list().length > 0) {
                System.err.println("Error: Backup directory is not empty: " + backupDirPath);
                System.err.println("Use a different directory or empty it first.");
                System.exit(1);
            }
        }
        
        System.out.println("DieselDB Physical Backup Tool");
        System.out.println("==============================");
        System.out.println("Database: " + databaseDirPath);
        System.out.println("Backup: " + backupDirPath);
        System.out.println();
        
        try {
            // Create physical backup
            PhysicalBackup backup = new PhysicalBackup(databaseDir, backupDir);
            backup.backup();
            
            System.out.println();
            System.out.println("Backup completed successfully!");
            
        } catch (Exception e) {
            System.err.println("Backup failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void printUsage() {
        System.out.println("DieselDB Physical Backup Tool v" + VERSION);
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  diesel_backup backup <database-dir> <backup-dir>");
        System.out.println("  diesel_backup --help");
        System.out.println("  diesel_backup --version");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  backup    Create a physical backup of the database");
        System.out.println("  --help    Show this help message");
        System.out.println("  --version Show version information");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  diesel_backup backup /path/to/database /path/to/backup");
        System.out.println();
        System.out.println("Environment Variables:");
        System.out.println("  DIESEL_HOME    Path to DieselDB installation directory");
        System.out.println();
        System.out.println("The backup creates a consistent snapshot by:");
        System.out.println("1. Triggering a checkpoint to create a consistent snapshot");
        System.out.println("2. Copying all data pages (frozen snapshot)");
        System.out.println("3. Copying WAL segments from checkpoint to current");
        System.out.println("4. Creating a backup manifest with checksums");
    }
}