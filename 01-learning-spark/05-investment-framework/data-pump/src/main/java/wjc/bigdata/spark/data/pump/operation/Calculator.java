package wjc.bigdata.spark.data.pump.operation;

import java.io.Serializable;

/**
 * 数据处理器，将一个RDD处理成另一个RDD
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 14:02
 **/
public interface Calculator extends Serializable {

    /**
     * 执行处理操作
     *
     * @param context 计算上下文件
     * @param chain   处理链
     */
    void handle(CalculationContext context, CalculatorChain chain);
}
