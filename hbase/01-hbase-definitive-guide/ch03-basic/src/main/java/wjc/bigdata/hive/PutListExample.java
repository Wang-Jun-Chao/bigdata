package wjc.bigdata.hive;

// cc PutListExample Example inserting data into HBase using a list

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import wjc.bigdata.hbase.common.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutListExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181,localhost:2182,localhost:2183");

        try (HBaseHelper helper = HBaseHelper.getHelper(conf);) {
            helper.dropTable("testtable");
            helper.createTable("testtable", "colfam1");
            Table table = helper.getTable("testtable");

            // vv PutListExample
            // co PutListExample-1-CreateList Create a list that holds the Put instances.
            List<Put> puts = new ArrayList<>();

            Put put1 = new Put(Bytes.toBytes("row1"));
            put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            // co PutListExample-2-AddPut1 Add put to list.
            puts.add(put1);

            Put put2 = new Put(Bytes.toBytes("row2"));
            put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val2"));
            // co PutListExample-3-AddPut2 Add another put to list.
            puts.add(put2);

            Put put3 = new Put(Bytes.toBytes("row2"));
            put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("val3"));
            // co PutListExample-4-AddPut3 Add third put to list.
            puts.add(put3);

            // co PutListExample-5-DoPut Store multiple rows with columns into HBase.
            table.put(puts);
        }
    }
}
