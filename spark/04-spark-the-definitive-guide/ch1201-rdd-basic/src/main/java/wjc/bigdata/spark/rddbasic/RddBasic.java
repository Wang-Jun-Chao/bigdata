package wjc.bigdata.spark.rddbasic;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;
import scala.util.Random;
import wjc.bigdata.spark.util.PathUtils;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-30 06:24
 **/
public class RddBasic implements Serializable {
    private final static Logger logger = LoggerFactory.getLogger(RddBasic.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("ch1201-rdd-basic")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<Integer> javaRDD = context.parallelize(Lists.newArrayList(1, 2, 3, 4, 5));
        javaRDD.map((Function<Integer, Long>) Integer::longValue);

        String[] splits = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ");

        JavaRDD<String> words = context.parallelize(Lists.newArrayList(splits), 2);
        words.setName("myWords");
        long count = words.distinct().count();
        System.out.println(count);

        List<String> list = words.filter((Function<String, Boolean>) v1 -> v1.startsWith("S")).collect();
        System.out.println(list);

        JavaRDD<Tuple3<String, Character, Boolean>> words2 = words.map((Function<String, Tuple3<String, Character, Boolean>>)
                v1 -> new Tuple3<>(v1, v1.charAt(0), v1.startsWith("S")));
        List<Tuple3<String, Character, Boolean>> take = words2.filter((Function<Tuple3<String, Character, Boolean>, Boolean>) Tuple3::_3)
                .take(5);
        System.out.println(take);
        list = words.sortBy(
                (Function<String, Integer>) v1 -> v1.length() * -1,
                true,
                1)
                .take(2);
        System.out.println(list);

        words.randomSplit(new double[]{0.5, 0.5});

        Integer reduce = context.parallelize(Lists.newArrayList(10, 20, 30, 40, 50))
                .reduce((Function2<Integer, Integer, Integer>) Integer::sum);
        System.out.println(reduce);
        context.parallelize(Lists.newArrayList(10, 20, 30, 40, 50))
                .max(new IntComparator());
        context.parallelize(Lists.newArrayList(10, 20, 30, 40, 50))
                .min(new IntComparator());


        words.reduce((Function2<String, String, String>) (v1, v2) -> v1.length() > v2.length() ? v1 : v2);
        words.count();

        double confidence = 0.95;
        int timeoutMilliseconds = 400;
        words.countApprox(timeoutMilliseconds, confidence);
        words.countApproxDistinct(0.05);
        words.countByValue();
        words.countByValueApprox(1000, 0.95);
        words.first();

        words.take(5);
        words.takeOrdered(5);
        words.top(5);
        boolean withReplacement = true;
        int numberToTake = 6;
        long randomSeed = 100L;
        words.takeSample(withReplacement, numberToTake, randomSeed);
        PathUtils.removeDir("/tmp/bookTitle");
        words.saveAsTextFile("file:/tmp/bookTitle");
        PathUtils.removeDir("/tmp/bookTitleCompressed");
        words.saveAsTextFile("file:/tmp/bookTitleCompressed", BZip2Codec.class);

        words.cache();
        words.getStorageLevel();
        PathUtils.removeDir("/tmp/some/path/for/checkpointing");
        context.setCheckpointDir("file:/tmp/some/path/for/checkpointing");
        words.checkpoint();

        words.pipe("wc -l").collect();

        words.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<String> stringIterator) throws Exception {
                return Lists.newArrayList(1).iterator();
            }
        });


        words.mapPartitionsWithIndex((Function2<Integer, Iterator<String>, Iterator<String>>) (partitionIndex, withinPartIterator) -> {
            List<String> objects = new ArrayList<>();
            withinPartIterator.forEachRemaining(item -> objects.add(
                    MessageFormat.format("Partition: {0} => {1}", partitionIndex, item)
            ));

            return objects.iterator();
        }, false).collect();


        words.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> iter) throws Exception {
                int randomFileName = new Random().nextInt();
                PrintWriter pw = new PrintWriter(new File("/tmp/random-file-" + randomFileName + ".txt"));
                while (iter.hasNext()) {
                    pw.write(iter.next());
                }
                pw.close();
            }
        });

        context.textFile("/some/path/withTextFiles");
        context.wholeTextFiles("/some/path/withTextFiles");
    }


    public static class IntComparator implements Comparator<Integer>, Serializable {

        @Override
        public int compare(Integer x, Integer y) {
            return x.compareTo(y);
        }
    }
}
