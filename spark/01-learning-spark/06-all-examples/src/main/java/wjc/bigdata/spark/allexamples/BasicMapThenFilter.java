/**
 * Illustrates a simple map then filter in Java
 */
package wjc.bigdata.spark.allexamples;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class BasicMapThenFilter {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(
                master, "basic-map-filter",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> squared = rdd.map((Function<Integer, Integer>) x -> x * x);
        JavaRDD<Integer> result = squared.filter((Function<Integer, Boolean>) x -> x != 1);
        System.out.println(StringUtils.join(result.collect(), ","));
    }
}
