package diesel.storage.page;

/**
 * Enumeration of page types for the page-based storage layer.
 */
public enum PageType {
    NORMAL(PageHeader.PAGE_TYPE_NORMAL),
    CATALOG(PageHeader.PAGE_TYPE_CATALOG);
    
    private final byte pageTypeValue;
    
    PageType(byte pageTypeValue) {
        this.pageTypeValue = pageTypeValue;
    }
    
    public byte getPageTypeValue() {
        return pageTypeValue;
    }
    
    public static PageType fromByte(byte value) {
        return switch (value) {
            case PageHeader.PAGE_TYPE_NORMAL -> NORMAL;
            case PageHeader.PAGE_TYPE_CATALOG -> CATALOG;
            default -> throw new IllegalArgumentException("Unknown page type: " + value);
        };
    }
}