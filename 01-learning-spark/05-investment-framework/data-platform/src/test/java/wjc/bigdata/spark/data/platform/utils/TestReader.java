package wjc.bigdata.spark.data.platform.utils;

import org.apache.spark.rdd.RDD;
import wjc.bigdata.spark.data.pump.connector.Input;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 19:53
 **/
public class TestReader implements Input {
    @Override
    public <T> T getAttribute(String className) {
        return null;
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
    public RDD<?> read() {
        System.out.println();
        return null;
    }
}
