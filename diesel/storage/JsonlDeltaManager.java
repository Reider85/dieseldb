package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks deltas between loads and saves for JSONL append mode (prompt 49).
 *
 * <p>In append mode, new rows are physically appended to the base
 * {@code .jsonl} file at save time; deletions are recorded in a sidecar
 * {@code .jsonl.delta} file. On load, the base file is read first and the
 * recorded base lines are skipped.
 *
 * <p>The delta file format is a single JSON header line:
 * <pre>
 * {"baseLineCount":N,"deletions":[d1,d2,...]}
 * </pre>
 * where {@code N} is the number of base-file lines at the moment the
 * deletions were recorded (i.e. before that save's append). New rows never
 * appear in the delta file - they are appended to the base file so a plain
 * read of the base reconstructs them.
 *
 * <p>Compaction (full atomic rewrite via {@link AtomicFileWriter} + delta
 * reset) happens when the ratio of (deleted base lines + pending new rows)
 * to the total row count exceeds the configured {@code jsonl.compaction.threshold}.
 */
public class JsonlDeltaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonlDeltaManager.class);

    /** Delta file extension. */
    public static final String DELTA_FILE_SUFFIX = ".jsonl.delta";

    private int baseFileLineCount;
    private final List<Object[]> baseRows;
    private final Set<Integer> deletedBaseLines;
    private final List<Object[]> pendingNewRows;
    private final List<boolean[]> pendingNewPresence;
    private final double compactionThreshold;

    public JsonlDeltaManager(double compactionThreshold) {
        this.baseFileLineCount = 0;
        this.baseRows = new ArrayList<>();
        this.deletedBaseLines = new HashSet<>();
        this.pendingNewRows = new ArrayList<>();
        this.pendingNewPresence = new ArrayList<>();
        this.compactionThreshold = compactionThreshold;
    }

    /**
     * Called after a successful load from the base file. Records the base
     * rows and resets all pending deltas (the delta is consumed by the load).
     */
    public void onLoad(List<Object[]> loadedRows, List<boolean[]> loadedPresence, int lineCount) {
        baseRows.clear();
        baseRows.addAll(loadedRows);
        baseFileLineCount = lineCount;
        deletedBaseLines.clear();
        pendingNewRows.clear();
        pendingNewPresence.clear();
        LOGGER.debug("Delta manager initialized: baseLineCount={}, baseRows={}",
                baseFileLineCount, baseRows.size());
    }

    /**
     * Called when a new row is inserted (not yet persisted to the base file).
     */
    public void onInsert(Object[] row, boolean[] present) {
        pendingNewRows.add(row);
        pendingNewPresence.add(present);
    }

    /**
     * Called when a row is about to be deleted from the in-memory list.
     * Determines if the row is from the base file (and marks its line as
     * deleted) or if it is a pending new row that was never persisted (and
     * removes it from the pending list).
     *
     * @param rowIndex    the index in the current rows list of the row being deleted
     * @param currentRows the current rows list (the row at rowIndex is still present)
     */
    public void onDelete(int rowIndex, List<Object[]> currentRows) {
        Object[] row = currentRows.get(rowIndex);
        int pendingOffset = baseRows.size() - deletedBaseLines.size();
        if (rowIndex >= pendingOffset && rowIndex - pendingOffset < pendingNewRows.size()) {
            int pendingIdx = rowIndex - pendingOffset;
            pendingNewRows.remove(pendingIdx);
            pendingNewPresence.remove(pendingIdx);
            LOGGER.debug("Removed pending new row at index {}", pendingIdx);
            return;
        }
        // This is a base row - find its line number by content.
        int lineNum = findBaseLineForValue(row);
        if (lineNum >= 0) {
            deletedBaseLines.add(lineNum);
            LOGGER.debug("Marked base line {} as deleted (row value match)", lineNum);
        } else {
            LOGGER.warn("Could not find base line for deleted row at index {} - delta may be incomplete", rowIndex);
        }
    }

    /**
     * Searches the base rows for a row matching the given values. Returns the
     * line number (0-based) of the first non-deleted match, or -1 if not found.
     */
    private int findBaseLineForValue(Object[] target) {
        for (int i = 0; i < baseRows.size(); i++) {
            if (deletedBaseLines.contains(i)) {
                continue;
            }
            if (Arrays.equals(baseRows.get(i), target)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Called after a successful append save. The pending new rows have been
     * written to the base file, so they move into the base rows and the
     * pending list is cleared. Deleted base lines are kept - they are still
     * needed until the delta is consumed by a reload or a compaction.
     */
    public void onSaveComplete() {
        for (int i = 0; i < pendingNewRows.size(); i++) {
            baseRows.add(pendingNewRows.get(i));
        }
        baseFileLineCount += pendingNewRows.size();
        pendingNewRows.clear();
        pendingNewPresence.clear();
        LOGGER.debug("Save complete: baseLineCount now {}, baseRows={}",
                baseFileLineCount, baseRows.size());
    }

    /**
     * Returns true if the number of new rows plus deleted base lines exceeds
     * the compaction threshold relative to the total live rows.
     */
    public boolean shouldCompact() {
        int total = getTotalRowCount();
        if (total == 0) {
            return false;
        }
        int deltaCount = getDeltaRowCount();
        boolean compact = (double) deltaCount / total >= compactionThreshold;
        if (compact) {
            LOGGER.info("Compaction threshold reached: {}/{} = {} >= {}",
                    deltaCount, total, (double) deltaCount / total, compactionThreshold);
        }
        return compact;
    }

    /** Returns the number of delta operations (deleted base lines + pending new rows). */
    public int getDeltaRowCount() {
        return deletedBaseLines.size() + pendingNewRows.size();
    }

    /** Returns the total live row count after applying all deltas. */
    public int getTotalRowCount() {
        return baseFileLineCount - deletedBaseLines.size() + pendingNewRows.size();
    }

    /** Returns the set of deleted base line numbers (0-based). */
    public Set<Integer> getDeletedBaseLines() {
        return deletedBaseLines;
    }

    /** Returns the list of new rows pending save. */
    public List<Object[]> getPendingNewRows() {
        return pendingNewRows;
    }

    /** Returns the presence flags for new rows pending save. */
    public List<boolean[]> getPendingNewPresence() {
        return pendingNewPresence;
    }

    /** Returns the base-file line count at the last load/save. */
    public int getBaseFileLineCount() {
        return baseFileLineCount;
    }

    /** Returns the base rows loaded from the file. */
    public List<Object[]> getBaseRows() {
        return baseRows;
    }

    /** Returns the compaction threshold. */
    public double getCompactionThreshold() {
        return compactionThreshold;
    }

    /**
     * Resets all delta state (after a full rewrite or compaction).
     */
    public void reset() {
        baseRows.clear();
        deletedBaseLines.clear();
        pendingNewRows.clear();
        pendingNewPresence.clear();
        baseFileLineCount = 0;
        LOGGER.debug("Delta manager reset");
    }

    /**
     * Reads a delta file and populates the deleted-base-lines set.
     *
     * @param deltaPath the path to the .jsonl.delta file
     * @return the number of deletions read, or -1 on a corrupt file
     */
    public int readDeltaFile(String deltaPath) {
        File deltaFile = new File(deltaPath);
        if (!deltaFile.exists()) {
            return 0;
        }
        try (BufferedReader br = Files.newBufferedReader(deltaFile.toPath(),
                StandardCharsets.UTF_8)) {
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.isBlank()) {
                LOGGER.warn("Empty delta file: {}", deltaPath);
                return -1;
            }
            int bcIdx = headerLine.indexOf("\"baseLineCount\"");
            if (bcIdx < 0) {
                LOGGER.warn("Malformed delta header (no baseLineCount): {}", deltaPath);
                return -1;
            }
            int colonIdx = headerLine.indexOf(':', bcIdx);
            int commaIdx = headerLine.indexOf(',', colonIdx);
            if (commaIdx < 0) {
                commaIdx = headerLine.indexOf('}');
            }
            baseFileLineCount = Integer.parseInt(headerLine.substring(colonIdx + 1, commaIdx).trim());

            int delIdx = headerLine.indexOf("\"deletions\"");
            if (delIdx >= 0) {
                int arrStart = headerLine.indexOf('[', delIdx);
                int arrEnd = headerLine.indexOf(']', arrStart);
                if (arrStart >= 0 && arrEnd >= 0) {
                    String arr = headerLine.substring(arrStart + 1, arrEnd).trim();
                    if (!arr.isEmpty()) {
                        for (String s : arr.split(",")) {
                            deletedBaseLines.add(Integer.parseInt(s.trim()));
                        }
                    }
                }
            }
            LOGGER.info("Read delta file: baseLineCount={}, deletions={}",
                    baseFileLineCount, deletedBaseLines.size());
            return deletedBaseLines.size();
        } catch (Exception e) {
            LOGGER.warn("Failed to read delta file {}: {}", deltaPath, e.getMessage());
            return -1;
        }
    }

    /**
     * Writes the delta file atomically via {@link AtomicFileWriter}. If there
     * are no deletions the delta file is removed.
     *
     * @param deltaPath the path to the .jsonl.delta file
     */
    public void writeDeltaFile(String deltaPath) throws IOException {
        if (deletedBaseLines.isEmpty()) {
            Files.deleteIfExists(new File(deltaPath).toPath());
            return;
        }
        StringBuilder header = new StringBuilder("{\"baseLineCount\":").append(baseFileLineCount);
        header.append(",\"deletions\":[");
        TreeSet<Integer> sorted = new TreeSet<>(deletedBaseLines);
        boolean first = true;
        for (int d : sorted) {
            if (!first) {
                header.append(',');
            }
            header.append(d);
            first = false;
        }
        header.append("]}\n");
        try (AtomicFileWriter afw = AtomicFileWriter.openText(new File(deltaPath))) {
            afw.bufferedWriter().write(header.toString());
            afw.commit();
        }
        LOGGER.info("Wrote delta file: baseLineCount={}, deletions={}",
                baseFileLineCount, deletedBaseLines.size());
    }
}