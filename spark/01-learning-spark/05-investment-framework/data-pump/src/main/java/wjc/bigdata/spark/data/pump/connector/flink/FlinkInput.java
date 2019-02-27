package wjc.bigdata.spark.data.pump.connector.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.AbstractInput;

import java.util.Collection;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 16:03
 **/
public class FlinkInput<T> extends AbstractInput<T> {
    private final static Logger logger = LoggerFactory.getLogger(FlinkInput.class);

    @Override
    public T read() {
        return null;
    }

    @Override
    public Collection<T> batchRead() {
        return null;
    }
}
