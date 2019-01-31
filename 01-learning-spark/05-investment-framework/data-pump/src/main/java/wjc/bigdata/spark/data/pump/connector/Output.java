package wjc.bigdata.spark.data.pump.connector;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;

/**
 * 数据输出连接器
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 14:00
 **/
public interface Output extends Serializable {

    /**
     * 将RDD的结果进行输出，一般输出到HDFS, KAFKA, REDIS等存储系统
     *
     * @param rdd 输入的RDD数据
     */
    <T> void write(RDD<T> rdd);

    <T> void write(JavaRDDLike<T, ? extends JavaRDDLike> rdd);


}
