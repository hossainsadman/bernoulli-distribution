package app_kvServer;

import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.io.Serializable;

import com.google.gson.Gson;

public class SQLTable implements Serializable {
    public String name;
    private String primaryKey;
    public List<String> cols;
    public Map<String, String> colTypes;
    private Map<String, Map<String, String>> rows;

    private static final long serialVersionUID = 1L;

    public SQLTable(String name, String primaryKey) {
        this.name = name;
        this.primaryKey = primaryKey;
        this.cols = new ArrayList<>();
        this.colTypes = new HashMap<>();
        this.rows = new HashMap<>();
    }

    public int getSize() {
        return rows.size();
    }

    public Map<String, String> getRow(String key) {
        return rows.get(key);
    }

    public List<Map<String, String>> getRows() {
        return new ArrayList<>(rows.values());
    }

    public List<String> getCols() {
        return new ArrayList<>(cols);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public boolean containsCol(String col) {
        return cols.contains(col);
    }

    public boolean containsRow(String key) {
        return rows.containsKey(key);
    }

    public Map<String, String> getColTypes() {
        return new HashMap<>(colTypes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n| TABLE: ").append(name).append(" | ");
        sb.append("PRIMARY KEY: ").append(primaryKey).append(" |\n");
        for (String col : cols) {
            sb.append(col).append(" (").append(colTypes.get(col)).append(")").append("\t");
        }
        sb.append("\n");
        for (Map.Entry<String, Map<String, String>> entry : rows.entrySet()) {
            Map<String, String> row = entry.getValue();
            for (String col : cols) {
                sb.append(row.get(col)).append("\t");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public String toStringForTransfer() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static SQLTable fromString(String str) {
        Gson gson = new Gson();
        return gson.fromJson(str, SQLTable.class);
    }

    public String toStringTable() {
        StringBuilder sb = new StringBuilder();
        int[] maxLengths = new int[cols.size()];
        List<List<String>> table = new ArrayList<>();

        // Prepare table and find max lengths
        for (Map.Entry<String, Map<String, String>> entry : rows.entrySet()) {
            Map<String, String> row = entry.getValue();
            List<String> rowList = new ArrayList<>();
            int i = 0;
            for (String col : cols) {
                String cell = row.get(col);
                rowList.add(cell);
                maxLengths[i] = Math.max(maxLengths[i], cell.length());
                i++;
            }
            table.add(rowList);
        }

        // Prepare header and find max lengths
        List<String> headerList = new ArrayList<>();
        int i = 0;
        for (String col : cols) {
            String cell = col + " (" + colTypes.get(col) + ")";
            headerList.add(cell);
            maxLengths[i] = Math.max(maxLengths[i], cell.length());
            i++;
        }

        // Build string
        sb.append("| TABLE: ").append(name).append(" | ");
        sb.append("PRIMARY KEY: ").append(primaryKey).append(" |\n");
        sb.append("| ").append(String.join(" | ", headerList)).append(" |\n");
        sb.append("| ").append(Arrays.stream(maxLengths).mapToObj(n -> "-".repeat(n)).collect(Collectors.joining(" | "))).append(" |\n");
        for (List<String> rowList : table) {
            sb.append("| ");
            for (int j = 0; j < rowList.size(); j++) {
                sb.append(String.format("%-" + maxLengths[j] + "s", rowList.get(j)));
                if (j < rowList.size() - 1) {
                    sb.append(" | ");
                }
            }
            sb.append(" |\n");
        }

        return sb.toString();
    }

    public void addCol(String colName, String colType) {
        if (cols.contains(colName)) {
            throw new IllegalArgumentException("Column " + colName + " already exists.");
        }
        cols.add(colName);
        colTypes.put(colName, colType);
        for (Map<String, String> row : rows.values()) {
            row.put(colName, null);
        }
    }
    
    public void removeCol(String colName) {
        if (!cols.contains(colName)) {
            throw new IllegalArgumentException("Column " + colName + " does not exist.");
        }
        cols.remove(colName);
        colTypes.remove(colName);
        for (Map<String, String> row : rows.values()) {
            row.remove(colName);
        }
    }

    public void addRow(Map<String, String> row) {
        String key = row.get(primaryKey);
        if (rows.containsKey(key)) {
            throw new IllegalArgumentException("A row with the same primary key already exists");
        }
        for (String col : cols) {
            if (!row.containsKey(col)) {
                row.put(col, null);
            } else {
                String value = row.get(col);
                String colType = colTypes.get(col);
                if (colType.equals("int")) {
                    try {
                        Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Value for column " + col + " must be an integer");
                    }
                }
            }
        }
        rows.put(key, row);
    }

    public void deleteRow(String primaryKey) {
        rows.remove(primaryKey);
    }

    public void deleteRows(List<Condition> conditions) {
        Iterator<Map.Entry<String, Map<String, String>>> iterator = rows.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, String>> entry = iterator.next();
            Map<String, String> row = entry.getValue();
            boolean match = true;
            for (Condition condition : conditions) {
                if (condition.operator == Comparison.NONE) {
                    continue;
                }
                String value = row.get(condition.col);
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

    public void updateRow(Map<String, String> newRow) {
        String primaryKeyValue = newRow.get(primaryKey);
        Map<String, String> row = rows.get(primaryKeyValue);
        if (row == null) {
            throw new IllegalArgumentException("No row with the given primary key exists");
        }
        for (String col : cols) {
            if (newRow.containsKey(col)) {
                String newValue = newRow.get(col);
                String colType = colTypes.get(col);
                if (colType.equals("int")) {
                    try {
                        Integer.parseInt(newValue);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Value for column " + col + " must be an integer");
                    }
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
        public String value;
        public Comparison operator;

        public Condition(String col, String value, Comparison operator) {
            this.col = col;
            this.value = value;
            this.operator = operator;
        }

        public Condition(String col) {
            this.col = col;
            this.value = null;
            this.operator = Comparison.NONE;
        }

        @Override
        public String toString() {
            return "Condition{" +
                    "col='" + col + '\'' +
                    ", value='" + value + '\'' +
                    ", operator=" + operator +
                    '}';
        }
    }

    public SQLTable selectCols(List<String> cols) {
        SQLTable derivedTable = new SQLTable(this.name, this.primaryKey);
        derivedTable.cols = new ArrayList<>(cols);
        for (String col : cols) {
            derivedTable.colTypes.put(col, this.colTypes.get(col));
        }
        for (Map.Entry<String, Map<String, String>> entry : this.rows.entrySet()) {
            String id = entry.getKey();
            Map<String, String> row = entry.getValue();
            Map<String, String> newRow = new HashMap<>();
            for (String col : cols) {
                newRow.put(col, row.get(col));
            }
            derivedTable.addRow(newRow);
        }
        return derivedTable;
    }

    public SQLTable selectRows(List<Condition> conditions) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> entry : this.rows.entrySet()) {
            Map<String, String> row = entry.getValue();
            boolean match = true;
            for (Condition condition : conditions) {
                if (condition.operator == Comparison.NONE) {
                    continue;
                }
                String value = row.get(condition.col);
                if (value == null) {
                    match = false;
                    break;
                }
                String colType = this.colTypes.get(condition.col);
                if ("int".equals(colType)) {
                    if (!compare(Integer.parseInt(value), Integer.parseInt(condition.value), condition.operator)) {
                        match = false;
                        break;
                    }
                } else {
                    if (!compare(value, condition.value, condition.operator)) {
                        match = false;
                        break;
                    }
                }
            }
            if (match) {
                result.add(new HashMap<>(row));
            }
        }
    
        SQLTable derivedTable = new SQLTable(this.name, this.primaryKey);
        derivedTable.cols = new ArrayList<>(this.cols);
        derivedTable.colTypes = new HashMap<>(this.colTypes);
    
        for (Map<String, String> row : result) {
            derivedTable.addRow(row);
        }
    
        return derivedTable;
    }
    
    private boolean compare(String value1, String value2, Comparison operator) {
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
    
    private boolean compare(Integer value1, Integer value2, Comparison operator) {
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

    public static Comparison getComparisonOperator(String operator) {
        switch (operator) {
            case ">":
                return Comparison.GREATER_THAN;
            case "<":
                return Comparison.LESS_THAN;
            case "=":
                return Comparison.EQUALS;
            default:
                return Comparison.NONE;
        }
    }

    // public static void main(String[] args) {
    //     SQLTable table = new SQLTable("table1", "col1");
    
    //     table.addCol("col1", "int");
    //     table.addCol("col2", "int");
    //     table.addCol("col3", "int");
    //     table.addCol("col4", "string");
    
    //     Map<String, String> row1 = new HashMap<>();
    //     row1.put("col1", "1");
    //     row1.put("col2", "2");
    //     row1.put("col3", "3");
    //     row1.put("col4", "a");
    //     table.addRow(row1);
    
    //     Map<String, String> row2 = new HashMap<>();
    //     row2.put("col1", "4");
    //     row2.put("col2", "5");
    //     row2.put("col3", "6");
    //     row2.put("col4", "b");
    //     table.addRow(row2);
    
    //     System.out.println(table.toStringTable());
    
    //     table.removeCol("col2");
    //     System.out.println(table.toStringTable());
    
    //     List<SQLTable.Condition> conditions = new ArrayList<>();
    //     conditions.add(new SQLTable.Condition("col1", "0", SQLTable.Comparison.GREATER_THAN));
    //     conditions.add(new SQLTable.Condition("col3", "3", SQLTable.Comparison.EQUALS));
    //     conditions.add(new SQLTable.Condition("col4", "a", SQLTable.Comparison.EQUALS));
    
    //     SQLTable selectedTable = table.selectRows(conditions);
    //     System.out.println(selectedTable);
    
    //     List<String> cols = new ArrayList<>();
    //     cols.add("col1");
    //     selectedTable = selectedTable.selectCols(cols);
    //     System.out.println(selectedTable);
    
    //     System.out.println("TEST: updateRow");
    //     Map<String, String> updatedRow = new HashMap<>();
    //     updatedRow.put("col1", "1");
    //     updatedRow.put("col2", "20");
    //     updatedRow.put("col3", "30");
    //     updatedRow.put("col4", "updated");
    //     table.updateRow(updatedRow);
    //     System.out.println(table.toStringTable());
        
    //     System.out.println("TEST: deleteRows");
    //     List<SQLTable.Condition> deleteConditions = new ArrayList<>();
    //     deleteConditions.add(new SQLTable.Condition("col1", "2", SQLTable.Comparison.GREATER_THAN));
    //     table.deleteRows(deleteConditions);
    //     System.out.println(table.toStringTable());
    // }

}