/**
 * Illustrates a simple map then filter in Java
 */
package wjc.bigdata.spark.allexamples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingLogInput {
    public static void main(String[] args) throws Exception {
        String master = args[0];
        JavaSparkContext sc = new JavaSparkContext(master, "StreamingLogInput");
        // Create a StreamingContext with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));
        // Create a DStream from all the input on port 7777
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
        // Filter our DStream for lines with "error"
        JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String line) {
                return line.contains("error");
            }
        });
        // Print out the lines with errors, which causes this DStream to be evaluated
        errorLines.print();
        // start our streaming context and wait for it to "finish"
        jssc.start();
        // Wait for 10 seconds then exit. To run forever call without a timeout
        jssc.awaitTermination(10000);
        // Stop the streaming context
        jssc.stop();
    }
}
