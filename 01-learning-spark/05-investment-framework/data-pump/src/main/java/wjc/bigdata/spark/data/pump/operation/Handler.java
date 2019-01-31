package wjc.bigdata.spark.data.pump.operation;

import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;

import java.io.Serializable;

/**
 * 数据处理器，将一个RDD处理成另一个RDD
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 14:02
 **/
public interface Handler extends Serializable {

    /**
     * 执行处理操作
     *
     * @param inputConnector 输入对象
     * @param output 输出对象
     * @param chain       处理链
     */
    default void handle(Input inputConnector, Output output, HandlerChain chain) {
    }
}
