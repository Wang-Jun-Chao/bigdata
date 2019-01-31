package wjc.bigdata.spark.local;

import wjc.bigdata.spark.data.pump.operation.Handler;
import wjc.bigdata.spark.data.pump.operation.HandlerChain;
import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 19:53
 **/
public class TestFirstHandler implements Handler {

    @Override
    public void handle(Input reader, Output writer, HandlerChain chain) {
        System.out.println(this.getClass().getName());
        chain.handle(reader, writer);
    }
}
