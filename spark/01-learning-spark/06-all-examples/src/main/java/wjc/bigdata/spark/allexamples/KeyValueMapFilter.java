/**
 * Illustrates how to make a PairRDD then do a basic filter
 */
package wjc.bigdata.spark.allexamples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;
import java.util.Map.Entry;

public final class KeyValueMapFilter {

    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
//            throw new Exception("Usage KeyValueMapFilter sparkMaster inputFile");
//        }
//        String master = args[0];
//        String inputFile = args[1];


        String master = "local";
        String inputFile = PathUtils.workDir("key-value-map-filter.txt");
        JavaSparkContext sc = new JavaSparkContext(
                master,
                "key-value-map-filter",
                System.getenv("SPARK_HOME"),
                System.getenv("JARS"));

        JavaRDD<String> input = sc.textFile(inputFile);
        PairFunction<String, String, String> keyData = (PairFunction<String, String, String>) x -> new Tuple2<>(x.split(" ")[0], x);
        Function<Tuple2<String, String>, Boolean> longWordFilter = (Function<Tuple2<String, String>, Boolean>) input1 -> (input1._2().length() < 20);

        JavaPairRDD<String, String> rdd = input.mapToPair(keyData);
        JavaPairRDD<String, String> result = rdd.filter(longWordFilter);

        Map<String, String> resultMap = result.collectAsMap();
        for (Entry<String, String> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }
}
