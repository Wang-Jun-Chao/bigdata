package wjc.bigdata.spark.data.pump.connector.spark;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.AbstractInput;

import java.util.Collection;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-30 07:20
 **/
public class SocketInput extends SparkInput {
    private final static Logger logger = LoggerFactory.getLogger(SocketInput.class);

    @Override
    public JavaRDDLike readJavaRDD() {
        return null;
    }

    @Override
    public Collection<? extends JavaRDDLike> batchReadJavaRDD() {
        return null;
    }

    @Override
    public RDD read() {
        return null;
    }

    @Override
    public Collection<? extends RDD> batchRead() {
        return null;
    }
}
