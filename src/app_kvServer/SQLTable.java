package app_kvServer;

import java.util.*;

public class SQLTable {
    public String name;
    private List<String> cols;
    private List<Map<String, Object>> rows;

    public SQLTable() {
        this.cols = new ArrayList<>();
        this.rows = new ArrayList<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String col : cols) {
            sb.append(col).append("\t");
        }
        sb.append("\n");
        for (Map<String, Object> row : rows) {
            for (String col : cols) {
                sb.append(row.get(col)).append("\t");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public void addCol(String colName) {
        cols.add(colName);
        for (Map<String, Object> row : rows) {
            row.put(colName, null);
        }
    }

    public void removeCol(String colName) {
        cols.remove(colName);
        for (Map<String, Object> row : rows) {
            row.remove(colName);
        }
    }

    public void addRow(Map<String, Object> row) {
        for (String col : cols) {
            if (!row.containsKey(col)) {
                row.put(col, null);
            }
        }
        rows.add(row);
    }

    public void removeRow(int rowIndex) {
        if (rowIndex >= 0 && rowIndex < rows.size()) {
            rows.remove(rowIndex);
        }
    }

    public enum Comparison {
        GREATER_THAN,
        LESS_THAN,
        EQUALS
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
    }

    public SQLTable select(List<Condition> conditions) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            boolean match = true;
            for (Condition condition : conditions) {
                Object value = row.get(condition.col);
                if (value == null || !compare(value, condition.value, condition.operator)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                result.add(row);
            }
        }

        SQLTable derivedTable = new SQLTable();
        derivedTable.cols = new ArrayList<>(this.cols);

        for (Map<String, Object> row : result) {
            derivedTable.addRow(new HashMap<>(row));
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
    
        table.addCol("col1");
        table.addCol("col2");
        table.addCol("col3");
        table.addCol("col4"); // Added column for strings
    
        Map<String, Object> row1 = new HashMap<>();
        row1.put("col1", 1);
        row1.put("col2", 2);
        row1.put("col3", 3);
        row1.put("col4", "a"); // Added string value
        table.addRow(row1);
    
        Map<String, Object> row2 = new HashMap<>();
        row2.put("col1", 4);
        row2.put("col2", 5);
        row2.put("col3", 6);
        row2.put("col4", "b"); // Added string value
        table.addRow(row2);
    
        System.out.println(table);
    
        table.removeCol("col2");
        System.out.println(table);
    
        List<SQLTable.Condition> conditions = new ArrayList<>();
        conditions.add(new SQLTable.Condition("col1", 0, SQLTable.Comparison.GREATER_THAN));
        conditions.add(new SQLTable.Condition("col3", 3, SQLTable.Comparison.EQUALS));
        SQLTable selectedTable = table.select(conditions);
        System.out.println(selectedTable);
    }
}