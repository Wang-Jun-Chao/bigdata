package wjc.bigdata.spark.data.pump.connector;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 15:58
 **/
public class HdfsOutput implements Output {
    private final static Logger logger = LoggerFactory.getLogger(HdfsOutput.class);



    public HdfsOutput() {
        
    }

    @Override
    public <T> void write(RDD<T> rdd) {

    }

    @Override
    public <T> void write(JavaRDDLike<T, ? extends JavaRDDLike> rdd) {

    }
}
