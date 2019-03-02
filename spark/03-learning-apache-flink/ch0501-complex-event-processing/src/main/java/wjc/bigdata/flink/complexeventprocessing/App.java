package wjc.bigdata.flink.complexeventprocessing;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TemperatureEvent> inputEventStream = env.fromElements(
                new TemperatureEvent("xyz", 22.0),
                new TemperatureEvent("xyz", 20.1),
                new TemperatureEvent("xyz", 21.1),
                new TemperatureEvent("xyz", 22.2),
                new TemperatureEvent("xyz", 29.1),
                new TemperatureEvent("xyz", 22.3),
                new TemperatureEvent("xyz", 22.1),
                new TemperatureEvent("xyz", 22.4),
                new TemperatureEvent("xyz", 22.7),
                new TemperatureEvent("xyz", 27.0));

        Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent>begin("first")
                .subtype(TemperatureEvent.class).where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) {
                        return value.getTemperature() >= 26.0;
                    }
                }).within(Time.seconds(10));

        DataStream<Alert> patternStream = CEP.pattern(inputEventStream, warningPattern)
                .select(new PatternSelectFunction<TemperatureEvent, Alert>() {
                    @Override
                    public Alert select(Map<String, List<TemperatureEvent>> pattern) throws Exception {
                        return new Alert("Temperature Rise Detected");
                    }
                });

        patternStream.print();
        env.execute("CEP on Temperature Sensor");
    }
}
