package wjc.bigdata.spark.java.kryo;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-23 16:18
 **/
public class KafkaKyroDeserializer<T> implements Deserializer<T> {
    private final static Logger logger = LoggerFactory.getLogger(KafkaKyroDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
