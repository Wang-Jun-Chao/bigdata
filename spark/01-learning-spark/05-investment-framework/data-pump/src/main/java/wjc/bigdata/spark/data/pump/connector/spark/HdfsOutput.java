package wjc.bigdata.spark.data.pump.connector.spark;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 15:58
 **/
public class HdfsOutput extends SparkOutput {
    private final static Logger logger = LoggerFactory.getLogger(HdfsOutput.class);

    @Override
    public void writeJavaRDD(JavaRDDLike data) {

    }

    @Override
    public void writeJavaRDD(Collection<? extends JavaRDDLike> data) {

    }


    @Override
    public void write(RDD data) {

    }

    @Override
    public void write(Collection<? extends RDD> data) {

    }
}