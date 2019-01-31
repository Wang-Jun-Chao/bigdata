package wjc.bigdata.spark.java.kryo;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-23 16:18
 **/
public class KafkaKyroSerializer<T> implements Serializer<T> {
    private final static Logger logger = LoggerFactory.getLogger(KafkaKyroSerializer.class);

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
