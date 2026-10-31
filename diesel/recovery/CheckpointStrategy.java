package diesel.recovery;

import java.io.IOException;

/**
 * Checkpoint strategy interface (Prompt4 #53).
 * 
 * <p>Defines how to perform a database checkpoint.
 * Different implementations provide different tradeoffs between
 * blocking behavior and consistency guarantees.
 */
public interface CheckpointStrategy {
    
    /**
     * Performs one checkpoint operation.
     * 
     * @return the LSN of the checkpoint record written
     * @throws IOException if checkpoint fails
     */
    long checkpoint() throws IOException;
    
    /**
     * Returns the name of this strategy.
     * 
     * @return "fuzzy" or "sharp"
     */
    String name();
}