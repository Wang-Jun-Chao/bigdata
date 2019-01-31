package wjc.bigdata.spark.data.pump.operation;

import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;

/**
 * 数据处理器，将一个RDD处理成另一个RDD
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 14:02
 **/
public class KafkaHandler implements Handler {

    @Override
    public void handle(Input inputConnector, Output output, HandlerChain chain) {

    }
}
