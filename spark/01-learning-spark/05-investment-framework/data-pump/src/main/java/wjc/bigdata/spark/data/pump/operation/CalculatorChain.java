package wjc.bigdata.spark.data.pump.operation;

import java.io.Serializable;

/**
 * 处理链，进行数据处理
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 08:37
 **/
public interface CalculatorChain extends Serializable {

    /**
     * 计算链调用前处理操作
     *
     * @param handler 调用处理器
     */
    void beforeHandle(BeforeCalculationHandler handler);

    /**
     * 执行处理操作
     *
     * @param context 计算上下文
     */
    void handle(CalculationContext context);


    /**
     * 计算链调用后处理操作
     *
     * @param handler 调用处理器
     */
    void afterHandle(AfterCalculateHandler handler);
}
