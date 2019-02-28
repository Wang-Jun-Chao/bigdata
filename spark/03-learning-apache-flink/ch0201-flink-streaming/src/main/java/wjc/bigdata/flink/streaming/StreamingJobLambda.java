package wjc.bigdata.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class StreamingJobLambda {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(
                Base.PRODUCT_TOPIC,
                new SimpleStringSchema(),
                Base.KAFKA_PROPS2);
        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());


        DataStream<Tuple2<String, Double>> keyedStream = env
                .addSource(myConsumer)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                // TODO 怎么解决lambda问题？
                .apply((WindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple, TimeWindow>) (key, window, input, out) -> {
                    double sum = 0L;
                    int count = 0;
                    for (Tuple2<String, Double> record : input) {
                        sum += record.f1;
                        count++;
                    }

                    // TODO 这里用来做什么
                    Tuple2<String, Double> result = input.iterator().next();
                    result.f1 = (sum / count);
                    out.collect(result);

                });

        keyedStream.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

}
