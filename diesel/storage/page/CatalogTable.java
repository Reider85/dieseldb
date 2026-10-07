package diesel.storage.page;

import diesel.storage.StorageType;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * System table holding the schema of every user table (prompt4 #9, R3-002).
 *
 * <p>The whole catalog is serialised as one JSON array into the dedicated
 * catalog page ({@code pageId = 0}) of the page file, so DDL survives a
 * restart. Writes are synchronous: {@link #updateTableSchema},
 * {@link #createTableSchema} and {@link #dropTableSchema} flush the page
 * before they return, so the client only gets an ACK once the schema is on
 * disk.
 *
 * <p>When no {@link PageManager} is wired (plain in-memory engine) the
 * catalog still works, it just lives in the heap.
 *
 * <p>Multi-page catalogs are out of scope here - a catalog that no longer fits
 * one page raises a {@link PageFullException} (prompt 10, tablespace step,
 * lifts that limit).
 */
public final class CatalogTable implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(CatalogTable.class.getName());

    /** Every tablespace keeps its catalog in page 0 (prompt4 #9). */
    static final PageId CATALOG_PAGE_ID = new PageId(0, 1, 0);

    private final PageManager pageManager;
    private final List<CatalogSchema> schemas = new ArrayList<>();
    private final Object lock = new Object();

    /**
     * @param pageManager page file backing the catalog, or {@code null} for an
     *                    in-memory-only catalog
     */
    public CatalogTable(PageManager pageManager) {
        this.pageManager = pageManager;
        loadCatalog();
    }

    /**
     * Reads page 0 and rebuilds the in-memory schema list (prompt4 #9, task 4).
     * A page file that has never been written yields an empty catalog; a page
     * that exists but cannot be parsed raises {@link CatalogCorruptedException}.
     */
    public void loadCatalog() {
        if (pageManager == null) {
            return;
        }
        synchronized (lock) {
            schemas.clear();
            byte[] json;
            try {
                json = readCatalogBytes();
            } catch (IOException e) {
                throw new CatalogCorruptedException("page 0 could not be read: " + e.getMessage(), e);
            }
            if (json == null || json.length == 0) {
                return;
            }
            String text = new String(json, StandardCharsets.UTF_8);
            try (JsonStreamParser parser = JsonStreams.createParser(new StringReader(text), config())) {
                schemas.addAll(CatalogSchema.readArray(parser));
            } catch (IOException | RuntimeException e) {
                throw new CatalogCorruptedException(e.getMessage(), e);
            }
            LOGGER.log(Level.INFO, "Loaded {0} table schema(s) from catalog page 0", schemas.size());
        }
    }

    /** Adds or replaces the schema of a table and flushes the catalog page. */
    public void updateTableSchema(CatalogSchema schema) {
        if (schema == null || schema.getTableName() == null) {
            throw new IllegalArgumentException("catalog schema requires a table name");
        }
        synchronized (lock) {
            schemas.removeIf(existing -> schema.getTableName().equals(existing.getTableName()));
            schemas.add(schema);
            flush();
        }
    }

    /** Adds a table schema (same as {@link #updateTableSchema}). */
    public void createTableSchema(String tableName, List<CatalogSchema.ColumnSchema> columns,
                                  String primaryKey, StorageType storageType,
                                  List<String> sequences, List<CatalogSchema.IndexSchema> indices) {
        updateTableSchema(new CatalogSchema(tableName, columns, primaryKey, storageType, sequences, indices));
    }

    /** Updates an existing table schema; unknown tables are rejected. */
    public void updateTableSchema(String tableName, List<CatalogSchema.ColumnSchema> columns,
                                  String primaryKey, StorageType storageType,
                                  List<String> sequences, List<CatalogSchema.IndexSchema> indices) {
        if (getTableSchema(tableName) == null) {
            throw new IllegalArgumentException("Table " + tableName + " not found in catalog");
        }
        updateTableSchema(new CatalogSchema(tableName, columns, primaryKey, storageType, sequences, indices));
    }

    /** Removes a table schema and flushes the catalog page. */
    public void dropTableSchema(String tableName) {
        synchronized (lock) {
            if (schemas.removeIf(existing -> tableName.equals(existing.getTableName()))) {
                flush();
            }
        }
    }

    /**
     * Records an index on an existing table and flushes the catalog page.
     *
     * @return {@code true} when the table was found and the index recorded
     */
    public boolean addIndex(String tableName, String indexName, List<String> columns, boolean unique) {
        synchronized (lock) {
            CatalogSchema schema = getTableSchema(tableName);
            if (schema == null) {
                return false;
            }
            schema.addIndex(new CatalogSchema.IndexSchema(indexName, columns, unique));
            flush();
            return true;
        }
    }

    public CatalogSchema getTableSchema(String tableName) {
        synchronized (lock) {
            return schemas.stream()
                    .filter(schema -> schema.getTableName().equals(tableName))
                    .findFirst()
                    .orElse(null);
        }
    }

    public List<CatalogSchema> getAllSchemas() {
        synchronized (lock) {
            return new ArrayList<>(schemas);
        }
    }

    public boolean tableExists(String tableName) {
        return getTableSchema(tableName) != null;
    }

    public int getTableCount() {
        synchronized (lock) {
            return schemas.size();
        }
    }

    /** Serialises the in-memory schemas into page 0 and forces a flush. */
    private void flush() {
        if (pageManager == null) {
            return;
        }
        byte[] json = serialize();
        try {
            ensureCatalogPageAllocated();
            // A fresh page avoids growing the slot directory on every DDL.
            Page page = new Page(CATALOG_PAGE_ID, pageManager.pageSize(), PageType.CATALOG.getPageTypeValue());
            page.insert(json);
            pageManager.writePage(page);
            pageManager.flush();
        } catch (PageFullException e) {
            throw new PageFullException("Catalog page 0 is full after " + schemas.size()
                    + " table schema(s); split the catalog across pages (prompt 10)", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist catalog page 0: " + e.getMessage(), e);
        }
    }

    /** Reserves page 0 in a brand-new page file so no later allocation reuses it. */
    private void ensureCatalogPageAllocated() throws IOException {
        if (pageManager.getFileSize() >= pageManager.pageSize()) {
            return;
        }
        PageId allocated = pageManager.allocatePage(CATALOG_PAGE_ID.tablespaceId());
        if (!CATALOG_PAGE_ID.equals(allocated)) {
            LOGGER.log(Level.WARNING, "Catalog page was allocated at {0}, expected {1}",
                    new Object[]{allocated, CATALOG_PAGE_ID});
        }
    }

    /** @return the raw JSON bytes of the catalog, or {@code null} when absent */
    private byte[] readCatalogBytes() throws IOException {
        if (pageManager.getFileSize() < pageManager.pageSize()) {
            return null;
        }
        try (PinnedPage pinned = pageManager.readPage(CATALOG_PAGE_ID)) {
            Page page = pinned.getPage();
            for (int slot = 0; slot < page.getSlotCount(); slot++) {
                byte[] tuple = page.get(slot);
                if (tuple != null) {
                    return tuple;
                }
            }
            return null;
        }
    }

    private byte[] serialize() {
        StringWriter writer = new StringWriter();
        try (JsonStreamGenerator generator = JsonStreams.createGenerator(writer, config())) {
            CatalogSchema.writeArray(generator, schemas);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialise catalog: " + e.getMessage(), e);
        }
        return writer.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static JsonParserConfig config() {
        return JsonParserConfig.defaults();
    }

    /** Closes the backing page manager, if this catalog owns one. */
    @Override
    public void close() {
        if (pageManager != null) {
            try {
                pageManager.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close catalog page manager: {0}", e.getMessage());
            }
        }
    }
}
