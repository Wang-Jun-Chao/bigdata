package wjc.bigdata.spark.data.pump.connector.spark;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import wjc.bigdata.spark.data.pump.connector.AbstractInput;

import java.util.Collection;

/**
 * 数据输入连接器
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 14:00
 **/
public abstract class SparkInput extends AbstractInput<RDD> {

    public JavaRDDLike getJavaRDD() {
        return findValue(JavaRDDLike.class);
    }

    public RDD getRDD() {
        return findValue(RDD.class);
    }

    public JavaDStreamLike getJavaDStream() {
        return findValue(JavaDStreamLike.class);
    }

    public DStream getDStream() {
        return findValue(DStream.class);
    }

    public JavaStreamingContext getJavaStreamingContext() {
        return findValue(JavaStreamingContext.class);
    }

    public abstract JavaRDDLike readJavaRDD();

    public abstract Collection<? extends JavaRDDLike> batchReadJavaRDD();
}
