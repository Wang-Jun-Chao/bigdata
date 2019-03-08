/**
 * Illustrates a simple map in Java
 */
package wjc.bigdata.spark.allexamples;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class BasicMap {
    public static void main(String[] args) throws Exception {
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
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer x) {
                        return x * x;
                    }
                });
        System.out.println(StringUtils.join(result.collect(), ","));
    }
}
