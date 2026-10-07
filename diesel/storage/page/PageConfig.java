package diesel.storage.page;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.ErrorMessages;

/**
 * Configuration for page-based storage (R3-002).
 * 
 * <p>Resolves page size from system property or config.properties, with fallback.
 * Supports both raw bytes (8192, 16384, 65536) and human-readable formats (8K, 16K, 64K, 8KB, etc.).
 * 
 * <p>Package-private (like StorageConfig), but exposes a public constant for the key.
 */
final class PageConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PageConfig.class);

    // Allowed page sizes (bytes)
    public static final int PAGE_SIZE_8K = 8192;
    public static final int PAGE_SIZE_16K = 16384;
    public static final int PAGE_SIZE_64K = 65536;
    public static final int[] ALLOWED_SIZES = {PAGE_SIZE_8K, PAGE_SIZE_16K, PAGE_SIZE_64K};

    // Config key
    public static final String PAGE_SIZE_KEY = "page.size";

    private static final int DEFAULT_PAGE_SIZE = PAGE_SIZE_8K;

    private PageConfig() {
        throw new AssertionError("No instances");
    }

    /**
     * Returns the configured page size in bytes.
     * Resolution order: system property → config.properties → default (8192).
     * 
     * @return page size in bytes (one of ALLOWED_SIZES)
     */
    static int getPageSize() {
        String raw = System.getProperty(PAGE_SIZE_KEY);
        if (raw == null) {
            raw = loadRootProps().getProperty(PAGE_SIZE_KEY, String.valueOf(DEFAULT_PAGE_SIZE));
        }
        return parsePageSize(raw);
    }

    /**
     * Parses a page size string into bytes.
     * 
     * @param rawSize string representation (e.g., "8192", "8K", "16KB", "64k")
     * @return page size in bytes (falls back to 8192 if invalid)
     */
    static int parsePageSize(String rawSize) {
        if (rawSize == null || rawSize.trim().isEmpty()) {
            LOGGER.warn("Empty page.size, using default {}", DEFAULT_PAGE_SIZE);
            return DEFAULT_PAGE_SIZE;
        }

        String trimmed = rawSize.trim().toUpperCase();
        
        // Check for KB suffix (must check before K to avoid partial match)
        if (trimmed.endsWith("KB")) {
            String numStr = trimmed.substring(0, trimmed.length() - 2);
            if (numStr.isEmpty()) {
                LOGGER.warn("Invalid page.size format '{}', using default {}", rawSize, DEFAULT_PAGE_SIZE);
                return DEFAULT_PAGE_SIZE;
            }
            try {
                int kilos = Integer.parseInt(numStr);
                int bytes = kilos * 1024;
                if (!isAllowedSize(bytes)) {
                    LOGGER.warn("Invalid page.size '{}': {}K not in allowed sizes, using default {}", 
                            rawSize, kilos, DEFAULT_PAGE_SIZE);
                    return DEFAULT_PAGE_SIZE;
                }
                return bytes;
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid page.size format '{}': {}, using default {}", 
                        rawSize, e.getMessage(), DEFAULT_PAGE_SIZE);
                return DEFAULT_PAGE_SIZE;
            }
        }
        
        // Check for K suffix
        if (trimmed.endsWith("K")) {
            String numStr = trimmed.substring(0, trimmed.length() - 1);
            if (numStr.isEmpty()) {
                LOGGER.warn("Invalid page.size format '{}', using default {}", rawSize, DEFAULT_PAGE_SIZE);
                return DEFAULT_PAGE_SIZE;
            }
            try {
                int kilos = Integer.parseInt(numStr);
                int bytes = kilos * 1024;
                if (!isAllowedSize(bytes)) {
                    LOGGER.warn("Invalid page.size '{}': {}K not in allowed sizes, using default {}", 
                            rawSize, kilos, DEFAULT_PAGE_SIZE);
                    return DEFAULT_PAGE_SIZE;
                }
                return bytes;
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid page.size format '{}': {}, using default {}", 
                        rawSize, e.getMessage(), DEFAULT_PAGE_SIZE);
                return DEFAULT_PAGE_SIZE;
            }
        }
        
        // Try raw integer
        try {
            int bytes = Integer.parseInt(trimmed);
            if (!isAllowedSize(bytes)) {
                LOGGER.warn("Invalid page.size '{}': not in allowed sizes, using default {}", 
                        rawSize, DEFAULT_PAGE_SIZE);
                return DEFAULT_PAGE_SIZE;
            }
            return bytes;
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid page.size format '{}': {}, using default {}", 
                    rawSize, e.getMessage(), DEFAULT_PAGE_SIZE);
            return DEFAULT_PAGE_SIZE;
        }
    }

    /**
     * Returns true if the given size is one of the allowed page sizes.
     */
    static boolean isAllowedSize(int size) {
        for (int allowed : ALLOWED_SIZES) {
            if (allowed == size) {
                return true;
            }
        }
        return false;
    }

    /**
     * Loads root properties from config.properties (same pattern as StorageConfig).
     */
    private static Properties loadRootProps() {
        Properties props = new Properties();
        try {
            java.io.File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                try (java.io.FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
            }
        } catch (IOException ignored) {
            LOGGER.debug("Config error, using defaults: {}", ignored.getMessage());
        }
        return props;
    }
}