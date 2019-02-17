package wjc.bigdata.spark.local;

import org.apache.spark.rdd.RDD;
import wjc.bigdata.spark.data.pump.connector.AbstractOutput;

import java.util.Collection;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 19:53
 **/
public class TestWriter extends AbstractOutput<RDD> {

    @Override
    public void write(RDD rdd) {
        System.out.println("write data: " + rdd);
    }

    @Override
    public void write(Collection<? extends RDD> data) {

    }
}
