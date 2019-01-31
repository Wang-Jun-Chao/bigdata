package wjc.bigdata.spark.data.pump.connector;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 14:41
 **/
public class BaseInput implements Input {
    private final static Logger logger = LoggerFactory.getLogger(BaseInput.class);

    private final ConcurrentMap<String, List<Object>> attributes = new ConcurrentHashMap<>(64);

    @Override
    public <T> T getAttribute(String name) {
        List<Object> list = attributes.get(name);
        return CollectionUtils.isNotEmpty(list) ? (T) list.get(0) : null;
    }

    @Override
    public List<Object> getAttributeValues(String name) {
        return attributes.get(name);
    }

    @Override
    public void setAttribute(String name, Object value) {
        List<Object> list = attributes.get(name);
        if (CollectionUtils.isEmpty(list)) {
            list = new ArrayList<>();
            attributes.put(name, list);
        }

        list.add(value);

    }

    @Override
    public void removeAttribute(String name) {
        attributes.remove(name);
    }

    @Override
    public List<Object> getAttributes() {
        List<Object> result = new ArrayList<>(attributes.size());
        attributes.values().forEach(result::addAll);

        return result;
    }

    @Override
    public Map<String, ? extends List> getAttributeMap() {
        return attributes;
    }

    @Override
    public JavaRDDLike getJavaRDD() {
        return findFirst(JavaRDDLike.class);
    }

    @Override
    public RDD getRDD() {
        return findFirst(RDD.class);
    }

    @Override
    public JavaDStreamLike getJavaDStream() {
        return findFirst(JavaDStreamLike.class);
    }

    @Override
    public DStream getDStream() {
        return findFirst(DStream.class);
    }

    @Override
    public JavaStreamingContext getJavaStreamingContext() {
        return findFirst(JavaStreamingContext.class);
    }

    @Override
    public <T> T findFirst(Class<T> type) {

        if (type == null) {
            return null;
        }

        for (List<Object> list : attributes.values()) {
            for (Object obj : list) {
                if (obj != null && type.isAssignableFrom(obj.getClass())) {
                    return (T) obj;
                }
            }
        }

        return null;
    }

    @Override
    public <T> Collection<T> findAll(Class<T> type) {
        List<T> result = new ArrayList<>();

        if (type == null) {
            return result;
        }

        for (List<Object> list : attributes.values()) {
            for (Object obj : list) {
                if (obj != null && type.isAssignableFrom(obj.getClass())) {
                    result.add((T) obj);
                }
            }
        }

        return result;
    }
}
