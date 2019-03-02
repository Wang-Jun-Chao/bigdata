package wjc.bigdata.flink.complexeventprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaDataApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        DataStream<TemperatureEvent> inputEventStream = env.addSource(
                new FlinkKafkaConsumer09<TemperatureEvent>("test", new EventDeserializationSchema(), properties));

        Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent>begin("first")
                .subtype(TemperatureEvent.class).where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) throws Exception {
                        return value.getTemperature() >= 26.0;
                    }
                }).within(Time.seconds(10));

        DataStream<Alert> patternStream = CEP.pattern(inputEventStream, warningPattern)
                .select(new PatternSelectFunction<TemperatureEvent, Alert>() {
                    @Override
                    public Alert select(Map<String, List<TemperatureEvent>> pattern) throws Exception {
                        return new Alert("Temperature Rise Detected:" + pattern);
                    }


                });

        patternStream.print();
        env.execute("CEP on Temperature Sensor");
    }
}
