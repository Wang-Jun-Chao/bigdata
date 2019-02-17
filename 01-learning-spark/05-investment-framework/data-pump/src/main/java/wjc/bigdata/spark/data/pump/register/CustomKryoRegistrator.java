package wjc.bigdata.spark.data.pump.register;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.spark.KafkaOutput;

/**
 * 注册自定义类
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-23 16:11
 **/
public class CustomKryoRegistrator implements KryoRegistrator {
    private final static Logger logger = LoggerFactory.getLogger(CustomKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(KafkaProducer.class);
        kryo.register(KafkaOutput.class);
        kryo.register(KafkaOutput.class);
        kryo.register(KafkaProducer.class, new FieldSerializer(kryo, KafkaProducer.class));
    }
}
