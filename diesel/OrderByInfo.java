package diesel;

class OrderByInfo {
    String column;
    boolean ascending;

    OrderByInfo(String column, boolean ascending) {
        this.column = column;
        this.ascending = ascending;
    }

    @Override
    public String toString() {
        return column + (ascending ? " ASC" : " DESC");
    }
}