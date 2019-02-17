package wjc.bigdata.spark.data.pump.connector.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.AbstractOutput;

import java.util.Collection;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 16:04
 **/
public class FlinkOutput<T> extends AbstractOutput<T> {
    private final static Logger logger = LoggerFactory.getLogger(FlinkOutput.class);

    @Override
    public void write(T data) {

    }

    @Override
    public void write(Collection<? extends T> data) {

    }
}
