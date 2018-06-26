package project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HBase操作工具类
 */
public class HBaseUtils {


    HBaseAdmin admin = null;
    Configuration conf = null;


    /**
     * 私有构造方法：加载一些必要的参数
     */
    private HBaseUtils() {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.187.116");
        conf.set("hbase.rootdir", "hdfs://192.168.187.116:9000/hbase");

        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取到HTable实例
     */
    public HTable getTable(String tableName) {
        HTable table = null;

        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加一条记录到HBase表
     * @param tableName HBase表名
     * @param rowkey rowkey
     * @param cf    columnfamily
     * @param column 表的列
     * @param value 写入表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value) {
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表名和输入条件获取HBase的记录数
     */
    public Map<String, Long> query(String tableName, String condition) throws Exception {

        Map<String, Long> map = new HashMap<>();

        HTable table = getTable(tableName);
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


    public static void main(String[] args) throws Exception {
/*        Map<String, Long> map = HBaseUtils.getInstance().query("imooc_course_clickcount" , "20181111_2");

        for(Map.Entry<String, Long> entry: map.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }*/


/*        HTable table = HBaseUtils.getInstance().getTable("imooc_course_clickcount");
        System.out.println(table.getName().getNameAsString());*/

        String tableName = "imooc_course_clickcount";
        // 时间_课程编号
        String rowkey = "20181111_2";
        String cf = "info";
        String column = "click_count";
        String value = "20";

        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
    }

}
