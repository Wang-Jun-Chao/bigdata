/**
 * Illustrates a simple map in Java
 */
package wjc.bigdata.spark.allexamples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public final class PerKeyAvg {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        JavaSparkContext sc = new JavaSparkContext(
                master, "per-key-avg",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));
        List<Tuple2<String, Integer>> input = new ArrayList<>();
        input.add(new Tuple2<>("coffee", 1));
        input.add(new Tuple2<>("coffee", 2));
        input.add(new Tuple2<>("pandas", 3));

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);

        Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
            @Override
            public AvgCount call(Integer x) {
                return new AvgCount(x, 1);
            }
        };

        Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, Integer x) {
                a.total += x;
                a.num += 1;
                return a;
            }
        };

        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) {
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };
        AvgCount initial = new AvgCount(0, 0);
        JavaPairRDD<String, AvgCount> avgCounts = rdd.combineByKey(createAcc, addAndCount, combine);
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Entry<String, AvgCount> entry : countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }

    public static class AvgCount implements java.io.Serializable {
        public int total;
        public int num;
        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public float avg() {
            return total / (float) num;
        }
    }
}
