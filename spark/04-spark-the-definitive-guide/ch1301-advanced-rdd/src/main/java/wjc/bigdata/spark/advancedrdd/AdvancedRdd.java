package wjc.bigdata.spark.advancedrdd;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.util.Random;

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
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("ch1301-advanced-rdd")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext context = new JavaSparkContext(conf);
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
    }

}
