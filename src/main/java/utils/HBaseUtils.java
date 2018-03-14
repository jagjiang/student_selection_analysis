package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Create by jag on 2018/2/26
 */
public class HBaseUtils {
    Connection connection = null;

    private HBaseUtils(){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop:2181");
        conf.set("hbase.rootdir","hdfs://192.168.220.210:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance(){
        if(null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }
    //获取表实例
    public Table getTable(String tableName){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     *添加一条记录到HBase表中
     * @param tableName
     * @param rowkey
     * @param cf
     * @param column
     * @param value
     */
    public void put(String tableName, String rowkey, String cf, String column, String value) {
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public Map<String, Long> query(String tableName, String condition) throws Exception {

        Map<String, Long> map = new HashMap<String, Long>();

        Table table = getTable(tableName);
        String cf = "info";
        String qualifier = "click_count";

        Scan scan = new Scan();

        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs) {
            String row = Bytes.toString(result.getRow());
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            map.put(row, clickCount);
        }

        return  map;
    }
    public static void main(String[] args) {
        Table table = getInstance().getTable("number_of_course");

        try {
            ResultScanner results =table.getScanner(Bytes.toBytes("info"));
            for(Result result : results) {
                String row = Bytes.toString(result.getRow());
                System.out.println(row);
            }
            HTableDescriptor desc = table.getTableDescriptor();
            HColumnDescriptor[] columnFamilies = desc.getColumnFamilies();
            System.out.println(results.next());
        } catch (IOException e) {
            e.printStackTrace();
        }
////        utils.HBaseUtils.getInstance().createOrOverwriteTable("number_of_course");
//        String tableNames =utils.HBaseUtils.getInstance().getTable("number_of_course").getName().getNameAsString();
//        String tableName = "number_of_course" ;
//        String rowkey = "20171111_88";
//        String cf = "info" ;
//        String column = "count";
//        String value = "2";
//        utils.HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
//        System.out.println(tableNames);
    }

}
