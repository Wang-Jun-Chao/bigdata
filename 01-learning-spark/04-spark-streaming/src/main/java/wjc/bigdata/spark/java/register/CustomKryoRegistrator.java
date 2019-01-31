package wjc.bigdata.spark.java.register;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.java.User;

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
        kryo.register(User.class, new FieldSerializer(kryo, User.class));
    }
}
