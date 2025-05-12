package diesel;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Splitter {

    private static final Logger LOGGER = Logger.getLogger(Splitter.class.getName());
    public static List<String> splitInValues(String valuesPart) {
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < valuesPart.length(); i++) {
            char c = valuesPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentValue.append(c);
                continue;
            }
            if (!inQuotes && c == ',') {
                String value = currentValue.toString().trim();
                if (!value.isEmpty()) {
                    values.add(value);
                }
                currentValue = new StringBuilder();
                continue;
            }
            currentValue.append(c);
        }

        String lastValue = currentValue.toString().trim();
        if (!lastValue.isEmpty()) {
            values.add(lastValue);
        }

        return values;
    }

    public static List<String> splitColumnDefinitions(String columnsPart) {
        List<String> columnDefs = new ArrayList<>();
        StringBuilder currentDef = new StringBuilder();
        int parenDepth = 0;
        boolean inQuotes = false;

        for (int i = 0; i < columnsPart.length(); i++) {
            char c = columnsPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentDef.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                } else if (c == ',' && parenDepth == 0) {
                    String def = currentDef.toString().trim();
                    if (!def.isEmpty()) {
                        columnDefs.add(def);
                    }
                    currentDef = new StringBuilder();
                    continue;
                }
            }
            currentDef.append(c);
        }

        String lastDef = currentDef.toString().trim();
        if (!lastDef.isEmpty()) {
            columnDefs.add(lastDef);
        }

        return columnDefs;
    }

    public static List<String> splitSelectItems(String selectPart) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        for (int i = 0; i < selectPart.length(); i++) {
            char c = selectPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentItem.append(c);
                continue;
            }
            if (inQuotes) {
                currentItem.append(c);
                continue;
            }
            if (c == '(') {
                parenDepth++;
                currentItem.append(c);
                continue;
            }
            if (c == ')') {
                parenDepth--;
                currentItem.append(c);
                continue;
            }
            if (c == ',' && parenDepth == 0) {
                String item = currentItem.toString().trim();
                if (!item.isEmpty()) {
                    items.add(item);
                }
                currentItem = new StringBuilder();
                continue;
            }
            currentItem.append(c);
        }

        String lastItem = currentItem.toString().trim();
        if (!lastItem.isEmpty()) {
            items.add(lastItem);
        }

        LOGGER.log(Level.FINE, "Split select items: {0}", items);
        return items;
    }

}