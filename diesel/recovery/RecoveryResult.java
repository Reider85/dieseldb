package diesel.recovery;

/**
 * Combined result of a full ARIES recovery run (prompt 4 #19): the analysis,
 * redo and undo phase results of one {@link ARIESAlgorithm#recover} call.
 */
public final class RecoveryResult {

    private final AnalysisResult analysis;
    private final RedoResult redo;
    private final UndoResult undo;

    /**
     * Creates a combined recovery result.
     *
     * @param analysis the analysis phase result
     * @param redo     the redo phase result
     * @param undo     the undo phase result
     */
    public RecoveryResult(AnalysisResult analysis, RedoResult redo, UndoResult undo) {
        this.analysis = java.util.Objects.requireNonNull(analysis, "analysis");
        this.redo = java.util.Objects.requireNonNull(redo, "redo");
        this.undo = java.util.Objects.requireNonNull(undo, "undo");
    }

    /**
     * Returns the analysis phase result (committed/active sets).
     */
    public AnalysisResult getAnalysis() {
        return analysis;
    }

    /**
     * Returns the redo phase result (page images applied, commits replayed).
     */
    public RedoResult getRedo() {
        return redo;
    }

    /**
     * Returns the undo phase result (logical undos of active transactions).
     */
    public UndoResult getUndo() {
        return undo;
    }

    /**
     * Returns the number of transactions still active after analysis
     * (the set the undo phase reversed).
     */
    public int getActiveTxidCount() {
        return analysis.getActiveTxidCount();
    }

    /**
     * Returns the number of transactions committed after analysis.
     */
    public int getCommittedTxidCount() {
        return analysis.getCommittedTxidCount();
    }

    @Override
    public String toString() {
        return "RecoveryResult{committed=" + analysis.getCommittedTxidCount()
                + ", active=" + analysis.getActiveTxidCount()
                + ", redo=" + redo
                + ", undo=" + undo + '}';
    }
}
