/**
 * Illustrates loading data from Hive with Spark SQL
 */
package wjc.bigdata.spark.allexamples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class LoadHive {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new Exception("Usage LoadHive sparkMaster tbl");
        }
        String master = args[0];
        String tbl = args[1];

        JavaSparkContext sc = new JavaSparkContext(
                master, "loadhive", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        SQLContext sqlCtx = new SQLContext(sc);
        Dataset<Row> rdd = sqlCtx.sql("SELECT key, value FROM src");
        JavaRDD<Integer> squaredKeys = rdd.toJavaRDD().map(new SquareKey());
        List<Integer> result = squaredKeys.collect();
        for (Integer elem : result) {
            System.out.println(elem);
        }
    }

    public static class SquareKey implements Function<Row, Integer> {
        @Override
        public Integer call(Row row) throws Exception {
            return row.getInt(0) * row.getInt(0);
        }
    }
}
