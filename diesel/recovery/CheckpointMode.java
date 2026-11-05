package diesel.recovery;

/**
 * Checkpoint strategy modes (Prompt4 #53).
 * 
 * <p>Controls checkpoint behavior:
 * - FUZZY: non-blocking checkpoint (flushes unpinned dirty pages only, no write gate)
 * - SHARP: blocking checkpoint (flushes all dirty pages, holds write gate for duration)
 */
public enum CheckpointMode {
    
    /** Fuzzy checkpoint: non-blocking, flushes unpinned dirty pages only (default) */
    FUZZY,
    
    /** Sharp checkpoint: blocking, flushes all dirty pages with write gate held */
    SHARP;
    
    /**
     * Parses a string representation into CheckpointMode.
     * Case-insensitive, trims whitespace.
     * 
     * @param mode string representation ("fuzzy", "sharp", etc.)
     * @return parsed mode, or FUZZY if null/blank/invalid
     */
    public static CheckpointMode parse(String mode) {
        if (mode == null || mode.trim().isEmpty()) {
            return FUZZY; // Default
        }
        
        String normalized = mode.trim().toLowerCase();
        switch (normalized) {
            case "fuzzy":
                return FUZZY;
            case "sharp":
                return SHARP;
            default:
                // Unknown string -> default to FUZZY with warning
                System.err.println("Warning: unknown checkpoint mode '" + mode + 
                        "', defaulting to FUZZY");
                return FUZZY;
        }
    }
    
    /**
     * Returns the string representation (lowercase).
     */
    public String toLowerCase() {
        return name().toLowerCase();
    }
}