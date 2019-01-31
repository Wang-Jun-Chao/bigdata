package wjc.bigdata.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-16 16:59
 **/
public class WordCount {
    private final static Logger logger = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        // 创建一个Java版本的Spark Context
        SparkConf conf = new SparkConf().setAppName("word-count");

        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取我们的输入数据
        JavaRDD<String> input = sc.textFile(inputFile);
        // 切分为单词
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String x) {
                        return Arrays.asList(x.split(" ")).iterator();
                    }
                });
        // 转换为键值对并计数
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2<>(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });

        // 将统计出来的单词总数存入一个文本文件，引发求值
        counts.saveAsTextFile(outputFile);
    }
}
