/**
 * Illustrates loading a sequence file of people and how many pandas they have seen
 */
package wjc.bigdata.spark.allexamples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class BasicLoadSequenceFile {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage BasicLoadSequenceFile [sparkMaster] [input]");
        }
        String master = args[0];
        String fileName = args[1];

        JavaSparkContext sc = new JavaSparkContext(
                master,
                "basic-load-sequence-file",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));
        JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(fileName, Text.class, IntWritable.class);
        JavaPairRDD<String, Integer> result = input.mapToPair(new ConvertToNativeTypes());
        List<Tuple2<String, Integer>> resultList = result.collect();
        for (Tuple2<String, Integer> record : resultList) {
            System.out.println(record);
        }
    }

    public static class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {
        @Override
        public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) {
            return new Tuple2<>(record._1.toString(), record._2.get());
        }
    }
}
