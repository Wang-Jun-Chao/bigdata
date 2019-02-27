package wjc.bigdata.spark.data.pump.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Map;


/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-23 16:18
 **/
public class KafkaKryoSerializer<T> implements Serializer<T>, Serializable {
    private final static Logger logger = LoggerFactory.getLogger(KafkaKryoSerializer.class);

    /**
     * 对象缓存最大16MB
     */
    private final static int BUFFER_SIZE = 16 * 1024 * 1024;


    @Override
    public void configure(Map configs, boolean isKey) {
        // nothing need to do
    }

    @Override
    public byte[] serialize(String topic, T data) {
        Output output = new Output(new ByteArrayOutputStream(), BUFFER_SIZE);
        Kryo kryo = null;
        byte[] serialized = null;
        try {
            kryo = KryoObjectPool.borrow();
            kryo.writeObject(output, data);
            serialized = output.toBytes();
        } finally {
            if (kryo != null) {
                KryoObjectPool.release(kryo);
            }
        }

        return serialized;
    }

    @Override
    public void close() {
        // nothing need to do
    }
}
