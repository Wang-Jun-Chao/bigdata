package wjc.bigdata.spark.rddconcurrency;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-10 15:38
 **/
public class SparkDriver implements Serializable {
    private final static Logger logger = LoggerFactory.getLogger(SparkDriver.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("rdd-concurrency-spark-driver-2");
        conf.setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(32, 34, 2, 3, 4, 54, 3));
        rdd1.foreachAsync(x -> System.out.println("Items in the list: " + x));
//        JavaFutureAction<Void> futureAction = rdd1.foreachAsync(x -> System.out.println("Items in the list: " + x));

//        if (futureAction.isDone() || futureAction.isCancelled()) {
//            sc.stop();
//        }

//        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(434, 3, 2, 43, 45, 3, 2));
//
//
//        rdd2.foreachAsync(x -> System.out.println("Number of items in the list: " + x));


    }
}
