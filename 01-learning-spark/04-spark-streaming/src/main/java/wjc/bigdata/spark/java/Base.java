package wjc.bigdata.spark.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import wjc.bigdata.spark.java.register.CustomKryoRegistrator;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-12 15:57
 **/
public class Base {
    protected final static Map<String, Object> KAFKA_PROPS = new HashMap<>();
    protected final static ObjectMapper        MAPPER      = new ObjectMapper();
    protected final static String              BROKERS     = "kslave2:9092,kslave3:9092,kslave4:9092";
    protected final static String              TOPIC       = "kafka-streaming";
//    protected final static String              TOPIC       = "kafka-streaming-" + new SimpleDateFormat("yyyy_MM_dd").format(new Date());

    protected final static String              GROUP_ID    = "spark";
    protected final static SparkConf           SPARK_CONF  = new SparkConf();

    static {
        KAFKA_PROPS.put("bootstrap.servers", BROKERS);
        KAFKA_PROPS.put("group.id", GROUP_ID);
        KAFKA_PROPS.put("key.serializer", StringSerializer.class.getName());
        KAFKA_PROPS.put("key.deserializer", StringDeserializer.class.getName());
        KAFKA_PROPS.put("value.serializer", StringSerializer.class.getName());
        KAFKA_PROPS.put("value.deserializer", StringDeserializer.class.getName());


        SPARK_CONF.setMaster("local");
        // 使用Kryo序列化库，如果要使用Java序列化库，需要把该行屏蔽掉
        SPARK_CONF.set("spark.serializer", KryoSerializer.class.getName());
        // 在Kryo序列化库中注册自定义的类集合，如果要使用Java序列化库，需要把该行屏蔽掉
        SPARK_CONF.set("spark.kryo.registrator", CustomKryoRegistrator.class.getName());

    }
}
