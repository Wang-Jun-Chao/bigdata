package wjc.bigdata.hive;

// cc PutListErrorExample1 Example inserting a faulty column family into HBase

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import wjc.bigdata.hbase.common.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutListErrorExample1 {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        try (HBaseHelper helper = HBaseHelper.getHelper(conf);) {
            helper.dropTable("testtable");
            helper.createTable("testtable", "colfam1");
            Table table = helper.getTable(TableName.valueOf("testtable"));

            List<Put> puts = new ArrayList<Put>();

            // vv PutListErrorExample1
            Put put1 = new Put(Bytes.toBytes("row1"));
            put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            puts.add(put1);
            Put put2 = new Put(Bytes.toBytes("row2"));

            // co PutListErrorExample1-1-AddErrorPut Add put with non existent family to list.
            put2.addColumn(Bytes.toBytes("BOGUS"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val2"));
            puts.add(put2);
            Put put3 = new Put(Bytes.toBytes("row2"));
            put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("val3"));
            puts.add(put3);

            // co PutListErrorExample1-2-DoPut Store multiple rows with columns into HBase.
            table.put(puts);
        }

    }
}
