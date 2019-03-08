/**
 * Illustrates loading data from Hive with Spark SQL
 */
package wjc.bigdata.spark.allexamples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class LoadJsonWithSparkSQL {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage LoadJsonWithSparkSQL sparkMaster jsonFile");
        }
        String master = args[0];
        String jsonFile = args[1];

        JavaSparkContext sc = new JavaSparkContext(
                master, "loadJsonwithsparksql");
        SQLContext sqlCtx = new SQLContext(sc);
        Dataset<Row> input = sqlCtx.jsonFile(jsonFile);
        input.printSchema();
    }
}
