package wjc.bigdata.spark.rddconcurrency;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-10 15:38
 **/
public class SparkDriver implements Serializable {
    private final static Logger logger = LoggerFactory.getLogger(SparkDriver.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        SparkConf conf = new SparkConf();
        conf.setAppName("rdd-concurrency-spark-driver-2");
        conf.setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.default.parallelism", "500");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(32, 34, 2, 3, 4, 54, 3));

//        rdd1.foreachAsync(x -> System.out.println("Items in the list: " + x));

        new Thread() {
            @Override
            public void run() {
                JavaFutureAction<Void> futureAction1 = rdd1.foreachAsync(
                        x -> {
                            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date())
                                    + " Double: " + (x * 2));
                            TimeUnit.MILLISECONDS.sleep(100);
                        });
                try {
                    futureAction1.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();


        new Thread() {
            @Override
            public void run() {
                JavaFutureAction<Void> futureAction2 = rdd1.foreachAsync(x -> {
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date())
                            + " Half: " + (x / 2));
                    TimeUnit.MILLISECONDS.sleep(50);
                });
                try {
                    futureAction2.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();


//        if (futureAction.isDone() || futureAction.isCancelled()) {
//            sc.stop();
//        }

//        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(434, 3, 2, 43, 45, 3, 2));
//
//
//        rdd2.foreachAsync(x -> System.out.println("Number of items in the list: " + x));


    }
}
