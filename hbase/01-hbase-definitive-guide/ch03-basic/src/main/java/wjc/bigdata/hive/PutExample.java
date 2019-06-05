package wjc.bigdata.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-06-05 22:17
 **/
public class PutExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181,localhost:2182,localhost:2183");

        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(TableName.valueOf("testtable"));
        ) {
            Put put = new Put(Bytes.toBytes("row1"));

            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("val2"));

            table.put(put);
        }
    }
}
