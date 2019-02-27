package wjc.bigdata.spark.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-17 11:26
 **/
public class BasicSum {
    private final static Logger logger = LoggerFactory.getLogger(BasicSum.class);

    public static void main(String[] args) {
        String master = "local";
        if (args != null && args.length > 0) {
            master = args[0];
        }

        JavaSparkContext ctx = new JavaSparkContext(
                master,
                "basic-sum",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));

        JavaRDD<Integer> rdd = ctx.parallelize(Arrays.asList(1, 2, 3, 4));

        Integer result = rdd.fold(0, (Function2<Integer, Integer, Integer>) (x, y) -> x + y);
        System.out.println(result);
        ctx.stop();
    }

}
