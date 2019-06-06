package wjc.bigdata.hive;

// cc PutListErrorExample3 Special error handling with lists of puts

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import wjc.bigdata.hbase.common.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutListErrorExample3 {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        try (HBaseHelper helper = HBaseHelper.getHelper(conf)) {

            helper.dropTable("testtable");
            helper.createTable("testtable", "colfam1");
            Table table = helper.getTable(TableName.valueOf("testtable"));
            List<Put> puts = new ArrayList<Put>();

            Put put1 = new Put(Bytes.toBytes("row1"));
            put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            puts.add(put1);
            Put put2 = new Put(Bytes.toBytes("row2"));
            put2.addColumn(Bytes.toBytes("BOGUS"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val2"));
            puts.add(put2);
            Put put3 = new Put(Bytes.toBytes("row2"));
            put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("val3"));
            puts.add(put3);

            // vv PutListErrorExample3
            try {
                table.put(puts); // co PutListErrorExample3-1-DoPut Store multiple rows with columns into HBase.
            } catch (RetriesExhaustedWithDetailsException e) {
                int numErrors = e.getNumExceptions(); // co PutListErrorExample3-2-Error Handle failed operations.
                System.out.println("Number of exceptions: " + numErrors);
                for (int n = 0; n < numErrors; n++) {
                    System.out.println("Cause[" + n + "]: " + e.getCause(n));
                    System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
                    System.out.println("Row[" + n + "]: " + e.getRow(n)); // co PutListErrorExample3-3-ErrorPut Gain access to the failed operation.
                }
                System.out.println("Cluster issues: " + e.mayHaveClusterIssues());
                System.out.println("Description: " + e.getExhaustiveDescription());
            }
            // ^^ PutListErrorExample3
        }
    }
}
