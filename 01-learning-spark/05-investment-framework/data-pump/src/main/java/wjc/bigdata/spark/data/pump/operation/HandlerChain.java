package wjc.bigdata.spark.data.pump.operation;

import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;

import java.io.Serializable;
import java.util.Optional;

/**
 * 处理链，进行数据处理
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 08:37
 **/
public interface HandlerChain extends Serializable {

    default void beforeHandle(BeforeAction beforeAction) {
        Optional.ofNullable(beforeAction).ifPresent(BeforeAction::act);
    }

    /**
     * 执行处理操作
     *
     * @param inputConnector 输入对象
     * @param output 输出对象
     */
    default void handle(Input inputConnector, Output output) {
    }

    default void afterHandle(AfterAction afterAction) {
        Optional.ofNullable(afterAction).ifPresent(AfterAction::act);
    }
}
