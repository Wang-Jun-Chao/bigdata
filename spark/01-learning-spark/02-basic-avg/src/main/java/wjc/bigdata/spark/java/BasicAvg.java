package wjc.bigdata.spark.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-17 11:26
 **/
public class BasicAvg {
    private final static Logger logger = LoggerFactory.getLogger(BasicAvg.class);

    public static void main(String[] args) {
        String master = "local";
        if (args != null && args.length > 0) {
            master = args[0];
        }

        JavaSparkContext ctx = new JavaSparkContext(
                master,
                "basic-avg",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));

        JavaRDD<Integer> rdd = ctx.parallelize(Arrays.asList(1, 2, 3, 4));

        AvgCount initial = new AvgCount(0, 0);
        Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {

            @Override
            public AvgCount call(AvgCount a, Integer x) throws Exception {
                a.total += x;
                a.num += 1;
                return a;
            }
        };

        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) throws Exception {
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };
        AvgCount result = rdd.aggregate(initial, addAndCount, combine);

        System.out.println(result.avg());
        ctx.stop();;
    }

    public static class AvgCount implements Serializable {
        int total;
        int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        private double avg() {
            return 1.0 * total / num;
        }
    }
}
