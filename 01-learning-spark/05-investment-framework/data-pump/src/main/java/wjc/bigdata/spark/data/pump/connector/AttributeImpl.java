package wjc.bigdata.spark.data.pump.connector;

import org.apache.commons.collections.CollectionUtils;
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
public class AttributeImpl<T> implements Attribute<T> {
    private final static Logger logger = LoggerFactory.getLogger(AttributeImpl.class);

    private final ConcurrentMap<String, List<Object>> attributes = new ConcurrentHashMap<>(64);

    @Override
    public <V> V getAttribute(String name) {
        List<Object> list = attributes.get(name);
        return CollectionUtils.isNotEmpty(list) ? (V) list.get(0) : null;
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
    public <V> V findValue(Class<V> type) {

        if (type == null) {
            return null;
        }

        for (List<Object> list : attributes.values()) {
            for (Object obj : list) {
                if (obj != null && type.isAssignableFrom(obj.getClass())) {
                    return (V) obj;
                }
            }
        }

        return null;
    }

    @Override
    public <V> Collection<V> findValues(Class<V> type) {
        List<V> result = new ArrayList<>();

        if (type == null) {
            return result;
        }

        for (List<Object> list : attributes.values()) {
            for (Object obj : list) {
                if (obj != null && type.isAssignableFrom(obj.getClass())) {
                    result.add((V) obj);
                }
            }
        }

        return result;
    }
}
