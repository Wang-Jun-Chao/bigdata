package wjc.bigdata.flink.streaming;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-12 15:57
 **/
public class Base {
    public final static Map<String, Object> KAFKA_PROPS   = new HashMap<>();
    public final static String              BROKERS       = "localhost:9091,localhost:9092,localhost:9093";
    public final static String              PRODUCT_TOPIC = "flink-kafka-streaming";
    public final static String              CONSUME_TOPIC = "flink-kafka-streaming-output";
    public final static String              GROUP_ID      = "flink";
    public final static Properties          KAFKA_PROPS2  = new Properties();


    static {
        KAFKA_PROPS.put("bootstrap.servers", BROKERS);
        KAFKA_PROPS.put("group.id", GROUP_ID);
        KAFKA_PROPS.put("key.serializer", StringSerializer.class.getName());
        KAFKA_PROPS.put("key.deserializer", StringDeserializer.class.getName());
        KAFKA_PROPS.put("value.serializer", StringSerializer.class.getName());
        KAFKA_PROPS.put("value.deserializer", StringDeserializer.class.getName());

        KAFKA_PROPS.forEach(KAFKA_PROPS2::put);
    }


}
