package hbase;

import java.util.List;

public interface IHBase {
    void createTable(String tableName, String[] fields);
    void addRecord(String tableName, String row, String[] fields, String[] values);
    List<String> scanColumn(String tableName, String column);
    void modifyData(String tableName, String row, String column, String newValue);
    void deleteRow(String tableName, String row);
}
