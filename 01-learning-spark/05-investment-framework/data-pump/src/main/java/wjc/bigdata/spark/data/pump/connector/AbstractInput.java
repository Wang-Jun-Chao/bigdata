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
public abstract class AbstractInput<T> extends AttributeImpl<T> implements Input<T> {
    private final static Logger logger = LoggerFactory.getLogger(AbstractInput.class);
}
