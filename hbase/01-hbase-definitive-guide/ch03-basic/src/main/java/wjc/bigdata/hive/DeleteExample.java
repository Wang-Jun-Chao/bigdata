package wjc.bigdata.hive;

// cc DeleteExample Example application deleting data from HBase

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import wjc.bigdata.hbase.common.HBaseHelper;

import java.io.IOException;

public class DeleteExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        try (
                HBaseHelper helper = HBaseHelper.getHelper(conf);
        ) {
            helper.dropTable("testtable");
            helper.createTable("testtable", 100, "colfam1", "colfam2");
            helper.put("testtable",
                    new String[]{"row1"},
                    new String[]{"colfam1", "colfam2"},
                    new String[]{"qual1", "qual1", "qual2", "qual2", "qual3", "qual3"},
                    new long[]{1, 2, 3, 4, 5, 6},
                    new String[]{"val1", "val1", "val2", "val2", "val3", "val3"});

            System.out.println("Before delete call...");
            helper.dump("testtable", new String[]{"row1"}, null, null);

            Table table = helper.getTable(TableName.valueOf("testtable"));

            // vv DeleteExample
            // co DeleteExample-1-NewDel Create delete with specific row.
            Delete delete = new Delete(Bytes.toBytes("row1"));
            // co DeleteExample-2-SetTS Set timestamp for row deletes.
            delete.setTimestamp(1);

            // co DeleteExample-3-DelColNoTS Delete the latest version only in one column.
            delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
            // co DeleteExample-4-DelColTS Delete specific version in one column.
            delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 3);
            // co DeleteExample-5-DelColsNoTS Delete all versions in one column.
            delete.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
            // co DeleteExample-6-DelColsTS Delete the given and all older versions in one column.
            delete.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 2);
            // co DeleteExample-7-AddCol Delete entire family, all columns and versions.

            delete.addFamily(Bytes.toBytes("colfam1"));
            // co DeleteExample-8-AddCol Delete the given and all older versions in the entire column family,
            // i.e., from all columns therein.
            delete.addFamily(Bytes.toBytes("colfam1"), 3);
            // co DeleteExample-9-DoDel Delete the data from the HBase table.
            table.delete(delete);

            // ^^ DeleteExample
            table.close();
            System.out.println("After delete call...");
            helper.dump("testtable", new String[]{"row1"}, null, null);
        }

    }
}
