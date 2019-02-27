package wjc.bigdata.spark.data.pump.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 16:00
 **/
public abstract class AbstractOutput<T> extends AttributeImpl<T> implements Output<T> {
    private final static Logger logger = LoggerFactory.getLogger(AbstractOutput.class);
}
