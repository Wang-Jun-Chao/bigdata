package wjc.bigdata.spark.local;

import org.apache.spark.rdd.RDD;
import wjc.bigdata.spark.data.pump.connector.Input;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 19:53
 **/
public class TestReader implements Input {

    private Map<String, Object> attributes = new HashMap<>();

    @Override
    public <T> T getAttribute(String className) {
        return (T) attributes.get(className);
    }

    @Override
    public List<Object> getAttributes() {
        return null;
    }

    @Override
    public Map<String, ? extends Collection> getAttributeMap() {
        return null;
    }

    @Override
    public <T> RDD<T> read() {
        attributes.put("java.lang.String", UUID.randomUUID().toString());
        System.out.println("read data: " + getAttributes());
        return null;
    }
}
