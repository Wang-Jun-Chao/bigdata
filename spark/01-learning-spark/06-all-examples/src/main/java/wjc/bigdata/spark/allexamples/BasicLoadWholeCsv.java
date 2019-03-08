/**
 * Illustrates joining two csv files
 */
package wjc.bigdata.spark.allexamples;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.StringReader;
import java.util.Iterator;

public class BasicLoadWholeCsv {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new Exception("Usage BasicLoadCsv sparkMaster csvInputFile csvOutputFile key");
        }
        String master = args[0];
        String csvInput = args[1];
        String outputFile = args[2];
        final String key = args[3];

        JavaSparkContext sc = new JavaSparkContext(
                master, "load-whole-csv", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaPairRDD<String, String> csvData = sc.wholeTextFiles(csvInput);
        JavaRDD<String[]> keyedRDD = csvData.flatMap(new ParseLine());
        JavaRDD<String[]> result =
                keyedRDD.filter(new Function<String[], Boolean>() {
                    @Override
                    public Boolean call(String[] input) {
                        return input[0].equals(key);
                    }
                });

        result.saveAsTextFile(outputFile);
    }

    public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
        @Override
        public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(file._2()));
            return reader.readAll().iterator();
        }
    }
}
