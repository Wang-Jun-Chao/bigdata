package wjc.bigdata.spark.data.pump.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Map;


/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-23 16:18
 **/
public class KafkaKryoDeserializer<T> implements Deserializer<T>, Serializable {
    private final static Logger logger      = LoggerFactory.getLogger(KafkaKryoDeserializer.class);
    private static final int    BUFFER_SIZE = 16 * 1024 * 1024;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing need to do
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        Output output = new Output(new ByteArrayOutputStream(), BUFFER_SIZE);
        Kryo kryo = null;
        T t = null;
        try {
            kryo = KryoObjectPool.borrow();
            Input input = new Input(data);
            t = (T) kryo.readObject(input, kryo.readClass(input).getType());
        } finally {
            if (kryo != null) {
                KryoObjectPool.release(kryo);
            }
        }
        return t;
    }

    @Override
    public void close() {
        // nothing need to do
    }
}
