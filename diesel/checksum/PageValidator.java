package diesel.checksum;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageCorruptedException;

/**
 * Page validator for CRC32C checksum verification (Prompt4 #52).
 * 
 * <p>Provides comprehensive page validation including checksum verification
 * and corruption detection. Integrates with PageManager for read-time validation.
 */
public final class PageValidator {
    
    /**
     * Validates a page and throws exception if corrupted.
     * 
     * @param page the page to validate
     * @throws PageCorruptedException if page is corrupted
     */
    public static void validate(Page page) throws PageCorruptedException {
        ChecksummedPage.validatePage(page);
    }
    
    /**
     * Validates a page and attempts recovery if corrupted.
     * 
     * @param page the page to validate/recover
     * @param walLsn the LSN to recover from (0 if no WAL available)
     * @return true if page is valid or recovery succeeded, false if still corrupted
     * @throws PageCorruptedException if recovery fails
     */
    public static boolean validateOrRecover(Page page, long walLsn) throws PageCorruptedException {
        try {
            validate(page);
            return true; // Page is valid
        } catch (PageCorruptedException e) {
            // Attempt recovery from WAL
            boolean recovered = ChecksummedPage.recoverFromWal(page, walLsn);
            if (recovered) {
                validate(page); // Verify recovery succeeded
                return true;
            }
            throw e; // Recovery failed, re-throw original exception
        }
    }
    
    /**
     * Checks if a page is valid without throwing exceptions.
     * 
     * @param page the page to check
     * @return true if page is valid, false if corrupted
     */
    public static boolean isValid(Page page) {
        try {
            validate(page);
            return true;
        } catch (PageCorruptedException e) {
            return false;
        }
    }
    
    /**
     * Gets detailed validation information about a page.
     * 
     * @param page the page to analyze
     * @return ValidationResult containing validation status and details
     */
    public static ValidationResult getValidationResult(Page page) {
        try {
            validate(page);
            return ValidationResult.valid(page);
        } catch (PageCorruptedException e) {
            return ValidationResult.corrupted(page, e);
        }
    }
    
    /**
     * Result of page validation with detailed information.
     */
    public static final class ValidationResult {
        private final Page page;
        private final boolean valid;
        private final PageCorruptedException corruptionException;
        private final String details;
        
        private ValidationResult(Page page, boolean valid, PageCorruptedException exception, String details) {
            this.page = page;
            this.valid = valid;
            this.corruptionException = exception;
            this.details = details;
        }
        
        /**
         * Creates a valid page result.
         */
        public static ValidationResult valid(Page page) {
            return new ValidationResult(page, true, null, "Page checksum is valid");
        }
        
        /**
         * Creates a corrupted page result.
         */
        public static ValidationResult corrupted(Page page, PageCorruptedException exception) {
            return new ValidationResult(page, false, exception, 
                "Page corrupted: " + exception.getMessage());
        }
        
        // Getters
        public Page getPage() { return page; }
        public boolean isValid() { return valid; }
        public PageCorruptedException getCorruptionException() { return corruptionException; }
        public String getDetails() { return details; }
    }
}