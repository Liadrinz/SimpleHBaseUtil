package hbase;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtil implements IHBase {
    private final Configuration conf;
    private final Connection conn;
    public HBaseUtil() {
        conf = new Configuration();
        conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new Error("Connection failed");
        }
    }

    public void createTable(String tableName, String[] fields) {
        try {
            Admin admin = conn.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String field : fields) {
                hTableDescriptor.addFamily(new HColumnDescriptor(field));
            }
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addRecord(String tableName, String row, String[] fields, String[] values) {
        try {
            Put put = new Put(row.getBytes());
            for (int i = 0; i < fields.length; ++i) {
                String[] familyCol = fields[i].split(":");
                put.addColumn(familyCol[0].getBytes(), familyCol[1].getBytes(), values[i].getBytes());
            }
            new HTablePool().getTable(tableName).put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> scanColumn(String tableName, String column) {
        try {
            List<String> results = new ArrayList<String>();
            Table table = new HTablePool().getTable(tableName);
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                String[] familyCol = column.split(":");
                List<Cell> cells = result.getColumnCells(familyCol[0].getBytes(), familyCol[1].getBytes());
                if (cells.size() == 0) return null;
                StringBuilder rowResult = new StringBuilder();
                for (Cell cell : cells) {
                    rowResult.append(Bytes.toString(cell.getValue()));
                }
                results.add(rowResult.toString());
            }
            return results;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void modifyData(String tableName, String row, String column, String newValue) {
        try {
            Put put = new Put(row.getBytes());
            String[] familyCol = column.split(":");
            put.addColumn(familyCol[0].getBytes(), familyCol[1].getBytes(), newValue.getBytes());
            new HTablePool().getTable(tableName).put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteRow(String tableName, String row) {
        try {
            Delete delete = new Delete(row.getBytes());
            new HTablePool().getTable(tableName).delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
