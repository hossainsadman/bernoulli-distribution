package app_kvServer;

import java.util.*;

public class SQLTable {
    public String name;
    private List<String> cols;
    private Map<String, Class<?>> colTypes;
    private Map<Object, Map<String, Object>> rows;

    public SQLTable() {
        this.cols = new ArrayList<>();
        this.colTypes = new HashMap<>();
        this.rows = new HashMap<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String col : cols) {
            sb.append(col).append("\t");
        }
        sb.append("ID\n");
        for (Map.Entry<Object, Map<String, Object>> entry : rows.entrySet()) {
            Object id = entry.getKey();
            Map<String, Object> row = entry.getValue();
            for (String col : cols) {
                sb.append(row.get(col)).append("\t");
            }
            sb.append(id).append("\n");
        }
        return sb.toString();
    }

    public void addCol(String colName, Class<?> colType) {
        if (cols.contains(colName)) {
            throw new IllegalArgumentException("Column " + colName + " already exists.");
        }
        cols.add(colName);
        colTypes.put(colName, colType);
        for (Map<String, Object> row : rows.values()) {
            row.put(colName, null);
        }
    }
    
    public void removeCol(String colName) {
        if (!cols.contains(colName)) {
            throw new IllegalArgumentException("Column " + colName + " does not exist.");
        }
        cols.remove(colName);
        colTypes.remove(colName);
        for (Map<String, Object> row : rows.values()) {
            row.remove(colName);
        }
    }

    public void addRow(Object rowId, Map<String, Object> row) {
        if (!(rowId instanceof Integer || rowId instanceof String)) {
            throw new IllegalArgumentException("Row ID must be an integer or a string");
        }
        if (rows.containsKey(rowId)) {
            throw new IllegalArgumentException("Row ID must be unique");
        }
        for (String col : cols) {
            if (!row.containsKey(col)) {
                row.put(col, null);
            } else {
                Class<?> colType = colTypes.get(col);
                if (!colType.isInstance(row.get(col))) {
                    throw new IllegalArgumentException("Incompatible type for column " + col);
                }
            }
        }
        rows.put(rowId, row);
    }

    public void removeRow(Object rowId) {
        rows.remove(rowId);
    }

    public enum Comparison {
        GREATER_THAN,
        LESS_THAN,
        EQUALS,
        NONE
    }

    public static class Condition {
        public String col;
        public Object value;
        public Comparison operator;

        public Condition(String col, Object value, Comparison operator) {
            this.col = col;
            this.value = value;
            this.operator = operator;
        }

        public Condition(String col) {
            this.col = col;
            this.value = null;
            this.operator = Comparison.NONE;
        }
    }

    public SQLTable select(List<String> cols) {
        SQLTable derivedTable = new SQLTable();
        derivedTable.cols = new ArrayList<>(cols);
        for (String col : cols) {
            derivedTable.colTypes.put(col, this.colTypes.get(col));
        }
        for (Map.Entry<Object, Map<String, Object>> entry : this.rows.entrySet()) {
            Object id = entry.getKey();
            Map<String, Object> row = entry.getValue();
            Map<String, Object> newRow = new HashMap<>();
            for (String col : cols) {
                newRow.put(col, row.get(col));
            }
            derivedTable.addRow(id, newRow);
        }
        return derivedTable;
    }

    public SQLTable where(List<Condition> conditions) {
        List<Map.Entry<Object, Map<String, Object>>> result = new ArrayList<>();
        for (Map.Entry<Object, Map<String, Object>> entry : this.rows.entrySet()) {
            Object id = entry.getKey();
            Map<String, Object> row = entry.getValue();
            boolean match = true;
            for (Condition condition : conditions) {
                if (condition.operator == Comparison.NONE) {
                    continue;
                }
                Object value = row.get(condition.col);
                if (value == null || !compare(value, condition.value, condition.operator)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                result.add(new AbstractMap.SimpleEntry<>(id, new HashMap<>(row)));
            }
        }
    
        SQLTable derivedTable = new SQLTable();
        derivedTable.cols = new ArrayList<>(this.cols);
        derivedTable.colTypes = new HashMap<>(this.colTypes);
    
        for (Map.Entry<Object, Map<String, Object>> entry : result) {
            derivedTable.addRow(entry.getKey(), entry.getValue());
        }
    
        return derivedTable;
    }
    
    private boolean compare(Object value1, Object value2, Comparison operator) {
        int comparison;
        if (value1 instanceof String && value2 instanceof String) {
            comparison = ((String) value1).compareTo((String) value2);
        } else if (value1 instanceof Integer && value2 instanceof Integer) {
            comparison = ((Integer) value1).compareTo((Integer) value2);
        } else {
            throw new IllegalArgumentException("Incompatible types for comparison");
        }
    
        switch (operator) {
            case GREATER_THAN:
                return comparison > 0;
            case LESS_THAN:
                return comparison < 0;
            case EQUALS:
                return comparison == 0;
            default:
                return false;
        }
    }

    public static void main(String[] args) {
        SQLTable table = new SQLTable();
    
        table.addCol("col1", Integer.class);
        table.addCol("col2", Integer.class);
        table.addCol("col3", Integer.class);
        table.addCol("col4", String.class);
    
        Map<String, Object> row1 = new HashMap<>();
        row1.put("col1", 1);
        row1.put("col2", 2);
        row1.put("col3", 3);
        row1.put("col4", "a");
        table.addRow(1, row1);
    
        Map<String, Object> row2 = new HashMap<>();
        row2.put("col1", 4);
        row2.put("col2", 5);
        row2.put("col3", 6);
        row2.put("col4", "b");
        table.addRow(2, row2);
    
        System.out.println(table);
    
        table.removeCol("col2");
        System.out.println(table);
    
        List<SQLTable.Condition> conditions = new ArrayList<>();
        conditions.add(new SQLTable.Condition("col1", 0, SQLTable.Comparison.GREATER_THAN));
        conditions.add(new SQLTable.Condition("col3", 3, SQLTable.Comparison.EQUALS));
        conditions.add(new SQLTable.Condition("col4", "a", SQLTable.Comparison.EQUALS));
    
        SQLTable selectedTable = table.where(conditions);
        System.out.println(selectedTable);
    
        List<String> cols = new ArrayList<>();
        cols.add("col1");
        table = table.select(cols);
        System.out.println(table);
    }
}