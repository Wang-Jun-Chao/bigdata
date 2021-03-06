/**
 * Illustrates remove outliers in Java using summary Stats
 */
package wjc.bigdata.spark.allexamples;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import java.util.Arrays;

public class RemoveOutliers {
    public static void main(String[] args) {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(
                master,
                "basic-map",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));
        JavaDoubleRDD input = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 1000.0));
        JavaDoubleRDD result = removeOutliers(input);
        System.out.println(StringUtils.join(result.collect(), ","));
    }

    private static JavaDoubleRDD removeOutliers(JavaDoubleRDD rdd) {
        final StatCounter summaryStats = rdd.stats();
        final double stddev = Math.sqrt(summaryStats.variance());
        System.out.println("stddev: " + stddev + ", mean: " + summaryStats.mean());
        return rdd.filter((Function<Double, Boolean>) x -> (Math.abs(x - summaryStats.mean()) < 2 * stddev));
    }
}
