/**
 * Illustrates a simple map partitions in Java to compute the average
 */
package wjc.bigdata.spark.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public final class BasicAvgMapPartitions implements Serializable {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        BasicAvgMapPartitions bamp = new BasicAvgMapPartitions();
        bamp.run(master);
    }

    public void run(String master) {
        JavaSparkContext sc = new JavaSparkContext(
                master, "basic-avg-map-partitions",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));

        JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(1, 2, 3, 4, 5));
        FlatMapFunction<Iterator<Integer>, AvgCount> setup = new FlatMapFunction<Iterator<Integer>, AvgCount>() {
            @Override
            public Iterator<AvgCount> call(Iterator<Integer> input) {
                AvgCount a = new AvgCount(0, 0);
                while (input.hasNext()) {
                    a.total += input.next();
                    a.num += 1;
                }
                ArrayList<AvgCount> ret = new ArrayList<AvgCount>();
                ret.add(a);
                return ret.iterator();
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

        AvgCount result = rdd.mapPartitions(setup).reduce(combine);
        System.out.println(result.avg());
    }

    public static class AvgCount implements Serializable {
        public Integer total;
        public Integer num;

        public AvgCount() {
            total = 0;
            num = 0;
        }

        public AvgCount(Integer total, Integer num) {
            this.total = total;
            this.num = num;
        }

        public AvgCount merge(Iterable<Integer> input) {
            for (Integer elem : input) {
                num += 1;
                total += elem;
            }
            return this;
        }

        public float avg() {
            return total / (float) num;
        }
    }
}
