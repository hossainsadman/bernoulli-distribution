package app_kvServer;

import java.util.*;

public class SQLTable<T extends Comparable<T>> {
    public String name;
    private List<String> cols;
    private List<Map<String, T>> rows;

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
        for (Map<String, T> row : rows) {
            for (String col : cols) {
                sb.append(row.get(col)).append("\t");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public void addCol(String colName) {
        cols.add(colName);
        for (Map<String, T> row : rows) {
            row.put(colName, null);
        }
    }

    public void removeCol(String colName) {
        cols.remove(colName);
        for (Map<String, T> row : rows) {
            row.remove(colName);
        }
    }

    public void addRow(Map<String, T> row) {
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

    public static class Condition<T extends Comparable<T>> {
        public String col;
        public T value;
        public Comparison operator;
    
        public Condition(String col, T value, Comparison operator) {
            this.col = col;
            this.value = value;
            this.operator = operator;
        }
    }

    public SQLTable<T> select(List<Condition<T>> conditions) {
        List<Map<String, T>> result = new ArrayList<>();
        for (Map<String, T> row : rows) {
            boolean match = true;
            for (Condition<T> condition : conditions) {
                T value = row.get(condition.col);
                if (value == null || !compare(value, condition.value, condition.operator)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                result.add(row);
            }
        }
    
        SQLTable<T> derivedTable = new SQLTable<>();
        derivedTable.cols = new ArrayList<>(this.cols);
    
        for (Map<String, T> row : result) {
            derivedTable.addRow(new HashMap<>(row));
        }
    
        return derivedTable;
    }
    
    private <T extends Comparable<T>> boolean compare(T value1, T value2, Comparison operator) {
        int comparison = value1.compareTo(value2);
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
        SQLTable<Integer> table = new SQLTable<>();
    
        table.addCol("col1");
        table.addCol("col2");
        table.addCol("col3");
    
        Map<String, Integer> row1 = new HashMap<>();
        row1.put("col1", 1);
        row1.put("col2", 2);
        row1.put("col3", 3);
        table.addRow(row1);
    
        Map<String, Integer> row2 = new HashMap<>();
        row2.put("col1", 4);
        row2.put("col2", 5);
        row2.put("col3", 6);
        table.addRow(row2);
    
        System.out.println(table);
    
        table.removeCol("col2");
        System.out.println(table);
    
        List<Condition<Integer>> conditions = new ArrayList<>();
        conditions.add(new Condition<>("col1", 0, Comparison.GREATER_THAN));
        conditions.add(new Condition<>("col3", 3, Comparison.EQUALS));
        SQLTable<Integer> selectedTable = table.select(conditions);
        System.out.println(selectedTable);
    }
}