package wjc.bigdata.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import wjc.bigdata.hbase.common.HBaseHelper;

import java.io.IOException;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-06-05 22:17
 **/
public class PutExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        try (
                HBaseHelper helper = HBaseHelper.getHelper(conf);
        ) {

            helper.dropTable("testtable");
            helper.createTable("testtable", "colfam1");
            Table table = helper.getTable("testtable");

            Put put = new Put(Bytes.toBytes("row1"));

            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("val2"));

            table.put(put);
        }
    }
}
