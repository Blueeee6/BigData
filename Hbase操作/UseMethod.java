import kotlin.collections.ArrayDeque;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.checkerframework.checker.units.qual.A;


public class UseMethod {
    public static long ts;
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    //连接HBase数据库
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //关闭数据库连接
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void createTable(String tableName, String[] fields) throws IOException {
            init();
            TableName tablename = TableName.valueOf(tableName);
            if (admin.tableExists(tablename)) {
                System.out.println("table is exists!");
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            }
            List<ColumnFamilyDescriptor> columnfamilieslist = new ArrayList<>();
            TableDescriptorBuilder table = TableDescriptorBuilder.newBuilder(tablename);
            for(String col:fields) {
                ColumnFamilyDescriptor columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(col.getBytes()).build();
                columnfamilieslist.add(columnFamily);
            }
            TableDescriptor tableret = table.setColumnFamilies(columnfamilieslist).build();
            admin.createTable(tableret);
            close();
    }
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        String element = fields[0];
        if(element.contains(":"))
        {
            for(int i=0;i<fields.length;i++)
            {
                Put put = new Put(row.getBytes());
                String temp[] = fields[i].split(":");
                put.addColumn(temp[0].getBytes(),temp[1].getBytes(),values[i].getBytes());
                table.put(put);
            }
        }
        else {
            for(int i=0;i<fields.length;i++)
            {
                Put put = new Put(row.getBytes());
                put.addColumn(fields[i].getBytes(),null,values[i].getBytes());
                table.put(put);
            }
        }
        table.close();
        close();
    }
    public static void scanColumn(String tableName, String column) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(column.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        boolean flag = false;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            flag=true;
            Cell cells[] = result.rawCells();
            for(Cell cell:cells) {
                System.out.println("行键："+ Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("时间戳："+cell.getTimestamp());
                System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
                if(cell.getQualifierLength()>0) {
                System.out.println("列限定符："+Bytes.toString(CellUtil.cloneQualifier(cell)));
                }
                System.out.println("值："+ Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        if(flag==false) {
            System.out.println("null");
            return;
        }
        table.close();
        close();
    }

    public static void modifyData(String tableName, String row, String column, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(row.getBytes());
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        if(column.contains(":")) {
            put.addColumn(column.split(":")[0].getBytes(),column.split(":")[1].getBytes(),val.getBytes());

        }
        else {
            put.addColumn(column.getBytes(),null,val.getBytes());
        }
        table.put(put);
        table.close();
        close();
    }

    public static void deleteRow(String tableName, String row) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete=new Delete(row.getBytes());
        table.delete(delete);
        table.close();
        close();
    }
    public static void main(String[] srgs) throws IOException {
//        //创建表函数示例
//        String createExample[] = new String[2];
//        createExample[0]="test_one";createExample[1]="test_two";
//        createTable("test",createExample);
//        //添加数据示例,以test2为例
//        String addrecord[] = new String[3];
//        addrecord[0]="score:math";addrecord[1]="score:computer";addrecord[2]="score:english";
//        String addrecorddata[] = new String[3];
//        addrecorddata[0]="50";addrecorddata[1]="60";addrecorddata[2]="70";
//        addRecord("test_score","2",addrecord,addrecorddata);
//        //读取数据示例
//        scanColumn("test_score","score");
//         //modify example
//         modifyData("test_score","1","score:english","90");
//        //delete example
//        deleteRow("test_score","2");
    };


}

