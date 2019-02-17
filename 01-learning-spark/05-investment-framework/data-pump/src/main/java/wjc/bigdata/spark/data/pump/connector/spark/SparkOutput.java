package wjc.bigdata.spark.data.pump.connector.spark;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.AbstractOutput;

import java.util.Collection;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 16:12
 **/
public abstract class SparkOutput extends AbstractOutput<RDD> {
    private final static Logger logger = LoggerFactory.getLogger(SparkOutput.class);

    public abstract void writeJavaRDD(JavaRDDLike data);

    public abstract void writeJavaRDD(Collection<? extends JavaRDDLike> data);

}
