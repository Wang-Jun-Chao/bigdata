package wjc.bigdata.spark.local;

import org.apache.spark.rdd.RDD;
import wjc.bigdata.spark.data.pump.connector.Output;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 19:53
 **/
public class TestWriter implements Output {

    @Override
    public void write(RDD rdd) {
        System.out.println("write data: " + rdd);
    }
}
