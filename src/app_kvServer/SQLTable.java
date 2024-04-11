package app_kvServer;

import java.util.*;

public class SQLTable {
    public String name;
    private List<String> cols;
    private List<Map<String, Integer>> rows;

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
        for (Map<String, Integer> row : rows) {
            for (String col : cols) {
                sb.append(row.get(col)).append("\t");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public void addCol(String colName) {
        cols.add(colName);
        for (Map<String, Integer> row : rows) {
            row.put(colName, null);
        }
    }

    public void removeCol(String colName) {
        cols.remove(colName);
        for (Map<String, Integer> row : rows) {
            row.remove(colName);
        }
    }

    public void addRow(Map<String, Integer> row) {
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

    public List<Integer> select(String col, String conditionCol, int conditionValue) {
        List<Integer> result = new ArrayList<>();
        for (Map<String, Integer> row : rows) {
            if (row.get(conditionCol) > conditionValue) {
                result.add(row.get(col));
            }
        }
        return result;
    }
    public SQLTable selectCols(List<String> selectedCols) {
        SQLTable derivedTable = new SQLTable();
        for (String col : selectedCols) {
            if (cols.contains(col)) {
                derivedTable.addCol(col);
            }
        }
        for (Map<String, Integer> row : rows) {
            Map<String, Integer> derivedRow = new HashMap<>();
            for (String col : selectedCols) {
                derivedRow.put(col, row.get(col));
            }
            derivedTable.addRow(derivedRow);
        }
        return derivedTable;
    }

    public static void main(String[] args) {
        // Create a new SQLTable
        SQLTable table = new SQLTable();

        // Add columns
        table.addCol("col1");
        table.addCol("col2");
        table.addCol("col3");

        // Add rows
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

        // print table
        System.out.println(table);

        // Remove a column
        table.removeCol("col2");
        System.out.println(table);
        
        // Select columns
        List<String> selectedCols = Arrays.asList("col1", "col3");
        SQLTable derivedTable = table.selectCols(selectedCols);
        System.out.println(derivedTable);

        // Select rows based on a condition
        List<Integer> selectedRows = table.select("col1", "col3", 2);

        // Remove a row
        table.removeRow(0);
        System.out.println(table);
    }
}