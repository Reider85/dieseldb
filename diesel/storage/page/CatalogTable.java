package diesel.storage.page;

import diesel.storage.StorageType;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System table holding the schema of every user table (prompt4 #9, R3-002).
 *
 * <p>The whole catalog is serialized as a JSON array. For small catalogs, it fits entirely
 * in page 0 (legacy v1 format). For large catalogs, page 0 contains a v2 header pointing
 * to chunk pages holding the JSON data (multi-page v2 format). DDL survives a restart.
 * Writes are synchronous: {@link #updateTableSchema}, {@link #createTableSchema} and 
 * {@link #dropTableSchema} flush the page before they return, so the client only gets 
 * an ACK once the schema is on disk.
 *
 * <p>When no {@link PageManager} is wired (plain in-memory engine) the catalog still works,
 * it just lives in the heap.
 *
 * <p>Multi-page catalogs support catalogs larger than a single page, lifting the 
 * ~40-table limit from prompt 10.
 */
public final class CatalogTable implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogTable.class);

    /** Every tablespace keeps its catalog in page 0 (prompt4 #9). */
    static final PageId CATALOG_PAGE_ID = new PageId(0, 1, 0);

    private final PageManager pageManager;
    
    /**
     * List of catalog-owned chunk page numbers (for v2 multi-page format).
     * Tracks all allocated chunk pages for reuse across flushes.
     */
    private final List<Long> chunkPages = new ArrayList<>();
    private final Object lock = new Object();

    /**
     * Name index of the catalog (O2 Buffer-1/2/3): every schema is keyed by its
     * table name so lookup/replace/drop are O(1) instead of an O(N) list scan.
     * LinkedHashMap preserves insertion order for serialisation and
     * getAllSchemas(); replace does remove+put so an updated table moves to the
     * end exactly like the old removeIf+add. Invariant: the key must equal
     * {@code schema.getTableName()} at insert time — {@link CatalogSchema} names
     * are never renamed after insertion ({@code setTableName} has no callers).
     */
    private final LinkedHashMap<String, CatalogSchema> schemasByName = new LinkedHashMap<>();

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
            schemasByName.clear();
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
                // putIfAbsent keeps first-wins lookup semantics of the old list scan
                for (CatalogSchema schema : CatalogSchema.readArray(parser)) {
                    schemasByName.putIfAbsent(schema.getTableName(), schema);
                }
            } catch (IOException | RuntimeException e) {
                throw new CatalogCorruptedException(e.getMessage(), e);
            }
            LOGGER.info("Loaded {} table schema(s) from catalog page 0", schemasByName.size());
        }
    }

    /** Adds or replaces the schema of a table and flushes the catalog page. */
    public void updateTableSchema(CatalogSchema schema) {
        if (schema == null || schema.getTableName() == null) {
            throw new IllegalArgumentException("catalog schema requires a table name");
        }
        synchronized (lock) {
            // remove+put: moves the entry to the end, same as the old removeIf+add
            schemasByName.remove(schema.getTableName());
            schemasByName.put(schema.getTableName(), schema);
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
            if (schemasByName.remove(tableName) != null) {
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
            return schemasByName.get(tableName);
        }
    }

    public List<CatalogSchema> getAllSchemas() {
        synchronized (lock) {
            return new ArrayList<>(schemasByName.values());
        }
    }

    public boolean tableExists(String tableName) {
        return getTableSchema(tableName) != null;
    }

    public int getTableCount() {
        synchronized (lock) {
            return schemasByName.size();
        }
    }
    
    /**
     * Sets the schema version for a table.
     * 
     * @param tableName Table name
     * @param version New schema version
     * @throws IllegalArgumentException if table doesn't exist
     */
    public void setSchemaVersion(String tableName, long version) {
        synchronized (lock) {
            CatalogSchema schema = getTableSchema(tableName);
            if (schema == null) {
                throw new IllegalArgumentException("Table does not exist: " + tableName);
            }
            schema.setSchemaVersion(version);
            flush();
        }
    }
    
    /**
     * Gets the schema version for a table.
     * Returns 1 if table exists but has no schemaVersion field (backward compatibility).
     * 
     * @param tableName Table name
     * @return Schema version, or 1 if table doesn't exist or has no version
     */
    public long getSchemaVersion(String tableName) {
        synchronized (lock) {
            CatalogSchema schema = getTableSchema(tableName);
            return schema != null ? schema.getSchemaVersion() : 1;
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
            
            // Check if we can use v1 format (single page)
            int maxTupleSize = maxTupleSize(pageManager.pageSize());
            if (json.length <= maxTupleSize) {
                // v1 format: write entire JSON to page 0
                Page page = new Page(CATALOG_PAGE_ID, pageManager.pageSize(), PageType.CATALOG.getPageTypeValue());
                page.insert(json);
                pageManager.writePage(page);
            } else {
                // v2 format: write chunks and header
                flushMultiPage(json);
            }
            
            pageManager.flush();
        } catch (PageFullException e) {
            throw new PageFullException("Catalog page 0 is full after " + schemasByName.size()
                    + " table schema(s); split the catalog across pages (prompt 10)", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist catalog page 0: " + e.getMessage(), e);
        }
    }
    
    /**
     * Flushes catalog in v2 multi-page format: writes chunks to separate pages,
     * then writes header to page 0.
     */
    private void flushMultiPage(byte[] json) throws IOException {
        int pageSize = pageManager.pageSize();
        int chunkCapacity = chunkCapacity(pageSize);
        int neededChunks = (json.length + chunkCapacity - 1) / chunkCapacity;
        
        // Grow chunk page list if needed
        while (chunkPages.size() < neededChunks) {
            PageId newPage = pageManager.allocatePage(CATALOG_PAGE_ID.tablespaceId());
            chunkPages.add(newPage.pageNum());
        }
        
        // Write chunk pages
        for (int i = 0; i < neededChunks; i++) {
            int chunkOffset = i * chunkCapacity;
            int chunkLength = Math.min(chunkCapacity, json.length - chunkOffset);
            byte[] chunk = Arrays.copyOfRange(json, chunkOffset, chunkOffset + chunkLength);
            
            PageId chunkPageId = new PageId(CATALOG_PAGE_ID.tablespaceId(), 1, chunkPages.get(i));
            Page chunkPage = new Page(chunkPageId, pageSize, PageType.CATALOG.getPageTypeValue());
            chunkPage.insert(chunk);
            pageManager.writePage(chunkPage);
        }
        
        // Write v2 header to page 0 (commit point)
        byte[] header = buildV2Header(neededChunks, json.length, json);
        Page page0 = new Page(CATALOG_PAGE_ID, pageSize, PageType.CATALOG.getPageTypeValue());
        page0.insert(header);
        pageManager.writePage(page0);
    }
    
    /**
     * Builds v2 binary header with magic, chunk count, payload length, CRC, and page numbers.
     */
    private byte[] buildV2Header(int chunkCount, long payloadLen, byte[] payload) {
        ByteBuffer bb = ByteBuffer.allocate(V2_HEADER_SIZE + chunkCount * 8)
                                   .order(ByteOrder.BIG_ENDIAN);
        
        // Magic and metadata
        bb.put(V2_MAGIC);
        bb.putInt(chunkCount);
        bb.putLong(payloadLen);
        
        // CRC of payload
        CRC32C crc = new CRC32C();
        crc.update(payload);
        bb.putInt((int) crc.getValue());
        
        // Page numbers for all allocated chunk pages
        for (int i = 0; i < chunkPages.size(); i++) {
            bb.putLong(chunkPages.get(i));
        }
        
        return bb.array();
    }

    /** Reserves page 0 in a brand-new page file so no later allocation reuses it. */
    private void ensureCatalogPageAllocated() throws IOException {
        if (pageManager.getFileSize() >= pageManager.pageSize()) {
            return;
        }
        PageId allocated = pageManager.allocatePage(CATALOG_PAGE_ID.tablespaceId());
        if (!CATALOG_PAGE_ID.equals(allocated)) {
            LOGGER.warn("Catalog page was allocated at {}, expected {}", allocated, CATALOG_PAGE_ID);
        }
    }

    // Multi-page catalog format constants
    private static final byte[] V2_MAGIC = {'D', 'S', 'C', '2'};
    private static final int V2_HEADER_SIZE = 20; // magic(4) + chunkCount(4) + payloadLen(8) + crc(4)
    
    /**
     * @return the raw JSON bytes of the catalog, or {@code null} when absent.
     * Handles both v1 (single-page) and v2 (multi-page) formats.
     */
    private byte[] readCatalogBytes() throws IOException {
        if (pageManager.getFileSize() < pageManager.pageSize()) {
            return null;
        }
        try (PinnedPage pinned = pageManager.readPage(CATALOG_PAGE_ID)) {
            Page page = pinned.getPage();
            byte[] firstTuple = null;
            for (int slot = 0; slot < page.getSlotCount(); slot++) {
                byte[] tuple = page.get(slot);
                if (tuple != null) {
                    firstTuple = tuple;
                    break;
                }
            }
            if (firstTuple == null || firstTuple.length == 0) {
                return null;
            }
            
            // v1 format: JSON array starts with '['
            if (firstTuple[0] == '[') {
                return firstTuple;
            }
            
            // v2 format: binary header starts with 'D','S','C','2'
            return readV2Payload(firstTuple);
        }
    }
    
    /**
     * Reads v2 multi-page catalog payload from header and chunk pages.
     */
    private byte[] readV2Payload(byte[] header) throws IOException {
        if (header.length < V2_HEADER_SIZE || 
            header[0] != V2_MAGIC[0] || header[1] != V2_MAGIC[1] || 
            header[2] != V2_MAGIC[2] || header[3] != V2_MAGIC[3]) {
            throw new CatalogCorruptedException("Unrecognized catalog header format");
        }
        
        ByteBuffer bb = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        bb.getInt(); // magic
        int chunkCount = bb.getInt();
        long payloadLen = bb.getLong();
        int expectedCrc = bb.getInt();
        
        if (chunkCount < 0 || payloadLen < 0 || header.length < V2_HEADER_SIZE + chunkCount * 8L) {
            throw new CatalogCorruptedException("Invalid v2 catalog header fields");
        }
        
        if (payloadLen > Integer.MAX_VALUE) {
            throw new CatalogCorruptedException("Catalog payload too large: " + payloadLen);
        }
        
        // Calculate chunk capacity and number of chunks needed
        int chunkCapacity = chunkCapacity(pageManager.pageSize());
        int neededChunks = (int) ((payloadLen + chunkCapacity - 1) / chunkCapacity);
        
        if (chunkCount < neededChunks) {
            throw new CatalogCorruptedException("Catalog header claims " + chunkCount + 
                " chunks but payload requires " + neededChunks);
        }
        
        byte[] payload = new byte[(int) payloadLen];
        int payloadOffset = 0;
        
        // Read chunk pages
        for (int i = 0; i < neededChunks; i++) {
            if (bb.position() + 8 > header.length) {
                throw new CatalogCorruptedException("Incomplete page numbers in v2 header");
            }
            long pageNum = bb.getLong();
            PageId chunkPageId = new PageId(CATALOG_PAGE_ID.tablespaceId(), 1, pageNum);
            
            try (PinnedPage pinned = pageManager.readPage(chunkPageId)) {
                Page chunkPage = pinned.getPage();
                byte[] chunk = chunkPage.get(0); // Each chunk page has exactly one tuple
                if (chunk == null) {
                    throw new CatalogCorruptedException("Chunk page " + pageNum + " is empty");
                }
                
                int copyLen = Math.min(chunk.length, payload.length - payloadOffset);
                System.arraycopy(chunk, 0, payload, payloadOffset, copyLen);
                payloadOffset += copyLen;
            } catch (IOException e) {
                throw new CatalogCorruptedException("Failed to read chunk page " + pageNum + 
                    ": " + e.getMessage(), e);
            }
        }
        
        if (payloadOffset != payload.length) {
            throw new CatalogCorruptedException("Payload length mismatch: expected " + 
                payload.length + ", got " + payloadOffset);
        }
        
        // Verify CRC
        CRC32C crc = new CRC32C();
        crc.update(payload);
        if ((int) crc.getValue() != expectedCrc) {
            throw new CatalogCorruptedException("Catalog CRC mismatch - restore from backup");
        }
        
        return payload;
    }

    private byte[] serialize() {
        StringWriter writer = new StringWriter();
        try (JsonStreamGenerator generator = JsonStreams.createGenerator(writer, config())) {
            CatalogSchema.writeArray(generator, new ArrayList<>(schemasByName.values()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialise catalog: " + e.getMessage(), e);
        }
        return writer.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static JsonParserConfig config() {
        return JsonParserConfig.defaults();
    }
    
    /**
     * Calculates maximum tuple size that fits in a single page.
     * Based on page header size + slot entry size + small margin.
     */
    private static int maxTupleSize(int pageSize) {
        return pageSize - PageHeader.HEADER_SIZE - SlottedPageLayout.SLOT_ENTRY_SIZE - 8;
    }
    
    /**
     * Calculates chunk capacity for v2 format.
     * Each chunk page has exactly one tuple, so we need space for tuple + slot entry.
     */
    private static int chunkCapacity(int pageSize) {
        return pageSize - PageHeader.HEADER_SIZE - SlottedPageLayout.SLOT_ENTRY_SIZE - 8;
    }

    /** Closes the backing page manager, if this catalog owns one. */
    @Override
    public void close() {
        if (pageManager != null) {
            try {
                pageManager.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close catalog page manager: {}", e.getMessage());
            }
        }
    }
}
