package wjc.bigdata.spark.advancedrdd;

import com.google.common.collect.Lists;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.util.Random;
import wjc.bigdata.spark.util.PathUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-30 20:17
 **/
public class AdvancedRdd {
    private final static Logger logger = LoggerFactory.getLogger(AdvancedRdd.class);

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch1301-advanced-rdd")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
        String[] myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
                .split(" ");
        JavaRDD<String> words = context.parallelize(Lists.newArrayList(myCollection), 2);

        words.map(word -> new Tuple2<>(word.toLowerCase(), 1));
        JavaPairRDD<String, String> keyword = words.keyBy(word -> word.substring(0, 1));

        keyword.mapValues(String::toUpperCase).collect();
        keyword.flatMapValues(word -> Lists.newArrayList(word.toLowerCase())).collect();

        keyword.keys().collect();
        keyword.values().collect();

        List<String> distinctChars = words
                .flatMap(word -> Lists.newArrayList(word.toLowerCase()).iterator())
                .distinct()
                .collect();
        Map<String, Double> sampleMap = new HashMap<String, Double>();
        distinctChars.forEach(c -> sampleMap.put(c, new Random().nextDouble()));
        words.map(word -> new Tuple2<>(word.toLowerCase(), word))
                .sample(true, 6L)
                .collect();


        JavaRDD<String> chars = words.flatMap(word -> Lists.newArrayList(word.toLowerCase()).iterator());
        JavaPairRDD<String, Integer> kvCharacters = chars.mapToPair(letter -> new Tuple2<>(letter, 1));
        JavaRDD<Integer> nums = context.parallelize(
                Lists.newArrayList(
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                        11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30),
                5);

        long timeout = 1000L; //milliseconds
        double confidence = 0.95;
        kvCharacters.countByKey();
        kvCharacters.countByKeyApprox(timeout, confidence);

        kvCharacters.groupByKey().map(row -> {
            return new Tuple2<>(
                    row._1(),
                    Lists.newArrayList(row._2())
                            .stream()
                            .reduce(Integer::sum)
                            .orElse(null));
        }).collect();

        kvCharacters.reduceByKey(Integer::sum).collect();

        nums.aggregate(0, Math::max, Integer::sum);

        int depth = 3;
        nums.treeAggregate(0, Math::max, Integer::sum, depth);

        int outputPartitions = 6;
        kvCharacters
                .combineByKey(
                        (Function<Integer, List<Integer>>) Lists::newArrayList,
                        (Function2<List<Integer>, Integer, List<Integer>>) (ints, v) -> {
                            ints.add(v);
                            return ints;
                        },
                        (Function2<List<Integer>, List<Integer>, List<Integer>>) (x, y) -> {
                            x.addAll(y);
                            return x;
                        },
                        outputPartitions)
                .collect();


        kvCharacters.foldByKey(0, Integer::sum).collect();

        JavaRDD<String> distinctCharsRdd = words
                .flatMap(
                        word -> Lists.newArrayList(word.toLowerCase()).iterator())
                .distinct();

        JavaPairRDD<String, Integer> charRDD = distinctCharsRdd.mapToPair(c -> new Tuple2<>(c, new Random().nextInt()));
        JavaPairRDD<String, Integer> charRDD2 = distinctCharsRdd.mapToPair(c -> new Tuple2<>(c, new Random().nextInt()));
        JavaPairRDD<String, Integer> charRDD3 = distinctCharsRdd.mapToPair(c -> new Tuple2<>(c, new Random().nextInt()));
        charRDD.cogroup(charRDD2, charRDD3).take(5);

        JavaPairRDD<String, Double> keyedChars = distinctCharsRdd.mapToPair(c -> new Tuple2<>(c, new Random().nextDouble()));
        outputPartitions = 10;
        kvCharacters.join(keyedChars).count();
        kvCharacters.join(keyedChars, outputPartitions).count();

        JavaRDD<Integer> numRange = context.parallelize(
                Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9),
                2);
        words.zip(numRange).collect();

        words.coalesce(1).getNumPartitions();

        words.repartition(10);


        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(PathUtils.workDir("../../../data/retail-data/all/"));
        JavaRDD<Row> rdd = df.coalesce(10).toJavaRDD();
        df.printSchema();

        rdd.map(r -> r.getString(6)).take(5).forEach(System.out::println);
        JavaPairRDD<Double, Row> keyedRDD = rdd.keyBy(row -> Double.parseDouble(row.getString(6)));
        keyedRDD.partitionBy(new HashPartitioner(10)).take(10);

        keyedRDD
                .partitionBy(new DomainPartitioner())
                .map(Tuple2::_1)
                .glom()
                .map(List::size)
                .take(5);

        context.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9))
                .map(num -> new SomeClass().setSomeValue(num));

    }

    static class SomeClass implements Serializable {
        int someValue = 0;

        public SomeClass setSomeValue(int i) {
            this.someValue = i;
            return this;
        }
    }

    static class DomainPartitioner extends Partitioner {
        int numPartitions = 3;

        @Override
        public int getPartition(Object key) {

            int customerId = ((Double) key).intValue();
            if (customerId == 17850 || customerId == 12583) {
                return 0;
            } else {
                return new java.util.Random().nextInt(2) + 1;
            }
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }
    }
}
