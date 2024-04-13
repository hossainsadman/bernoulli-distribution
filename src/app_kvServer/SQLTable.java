package app_kvServer;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class SQLTable {
    public String name;
    private String primaryKey;
    public List<String> cols;
    public Map<String, Class<?>> colTypes;
    private Map<Object, Map<String, Object>> rows;

    public SQLTable(String name, String primaryKey) {
        this.name = name;
        this.primaryKey = primaryKey;
        this.cols = new ArrayList<>();
        this.colTypes = new HashMap<>();
        this.rows = new HashMap<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("| TABLE: ").append(name).append(" | ");
        sb.append("PRIMARY KEY: ").append(primaryKey).append(" |\n");
        for (String col : cols) {
            sb.append(col).append("\t");
        }
        sb.append("\n");
        for (Map.Entry<Object, Map<String, Object>> entry : rows.entrySet()) {
            Map<String, Object> row = entry.getValue();
            for (String col : cols) {
                sb.append(row.get(col)).append("\t");
            }
            sb.append("\n");
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

    public void addRow(Map<String, Object> row) {
        Object key = row.get(primaryKey);
        if (!(key instanceof Integer || key instanceof String)) {
            throw new IllegalArgumentException("Row ID must be an integer or a string");
        }
        if (rows.containsKey(key)) {
            throw new IllegalArgumentException("A row with the same primary key already exists");
        }
        for (String col : cols) {
            if (!row.containsKey(col)) {
                row.put(col, null);
            } else {
                // Class<?> colType = colTypes.get(col);
                // if (!colType.isInstance(row.get(col))) {
                //     throw new IllegalArgumentException("Incompatible type for column " + col);
                // }
            }
        }
        rows.put(key, row);
    }

    public void deleteRow(Object primaryKey) {
        rows.remove(primaryKey);
    }

    public void deleteRows(List<Condition> conditions) {
        Iterator<Map.Entry<Object, Map<String, Object>>> iterator = rows.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Object, Map<String, Object>> entry = iterator.next();
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
                iterator.remove();
            }
        }
    }

    public void updateRow(Object primaryKey, Map<String, Object> newRow) {
        Map<String, Object> row = rows.get(primaryKey);
        if (row == null) {
            throw new IllegalArgumentException("No row with the given primary key exists");
        }
        for (String col : cols) {
            if (newRow.containsKey(col)) {
                Class<?> colType = colTypes.get(col);
                Object newValue = newRow.get(col);
                if (!colType.isInstance(newValue)) {
                    throw new IllegalArgumentException("Incompatible type for column " + col);
                }
                row.put(col, newValue);
            }
        }
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

    public SQLTable selectCols(List<String> cols) {
        SQLTable derivedTable = new SQLTable(this.name, this.primaryKey);
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
            derivedTable.addRow(newRow);
        }
        return derivedTable;
    }

    public SQLTable selectRows(List<Condition> conditions) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<Object, Map<String, Object>> entry : this.rows.entrySet()) {
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
                result.add(new HashMap<>(row));
            }
        }
    
        SQLTable derivedTable = new SQLTable(this.name, this.primaryKey);
        derivedTable.cols = new ArrayList<>(this.cols);
        derivedTable.colTypes = new HashMap<>(this.colTypes);
    
        for (Map<String, Object> row : result) {
            derivedTable.addRow(row);
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

    // public static void main(String[] args) {
    //     SQLTable table = new SQLTable("table1", "col1");

    //     table.addCol("col1", Integer.class);
    //     table.addCol("col2", Integer.class);
    //     table.addCol("col3", Integer.class);
    //     table.addCol("col4", String.class);

    //     Map<String, Object> row1 = new HashMap<>();
    //     row1.put("col1", 1);
    //     row1.put("col2", 2);
    //     row1.put("col3", 3);
    //     row1.put("col4", "a");
    //     table.addRow(row1);

    //     Map<String, Object> row2 = new HashMap<>();
    //     row2.put("col1", 4);
    //     row2.put("col2", 5);
    //     row2.put("col3", 6);
    //     row2.put("col4", "b");
    //     table.addRow(row2);
    
    //     System.out.println(table);
    
    //     table.removeCol("col2");
    //     System.out.println(table);
    
    //     List<SQLTable.Condition> conditions = new ArrayList<>();
    //     conditions.add(new SQLTable.Condition("col1", 0, SQLTable.Comparison.GREATER_THAN));
    //     conditions.add(new SQLTable.Condition("col3", 3, SQLTable.Comparison.EQUALS));
    //     conditions.add(new SQLTable.Condition("col4", "a", SQLTable.Comparison.EQUALS));
    
    //     SQLTable selectedTable = table.selectRows(conditions);
    //     System.out.println(selectedTable);
    
    //     List<String> cols = new ArrayList<>();
    //     cols.add("col1");
    //     selectedTable = selectedTable.selectCols(cols);
    //     System.out.println(selectedTable);

    //     System.out.println("TEST: updateRow");
    //     Map<String, Object> updatedRow = new HashMap<>();
    //     updatedRow.put("col1", 1);
    //     updatedRow.put("col2", 20);
    //     updatedRow.put("col3", 30);
    //     updatedRow.put("col4", "updated");
    //     table.updateRow(1, updatedRow);
    //     System.out.println(table);
        
    //     System.out.println("TEST: deleteRows");
    //     List<SQLTable.Condition> deleteConditions = new ArrayList<>();
    //     deleteConditions.add(new SQLTable.Condition("col1", 0, SQLTable.Comparison.GREATER_THAN));
    //     table.deleteRows(deleteConditions);
    //     System.out.println(table);
    // }

}