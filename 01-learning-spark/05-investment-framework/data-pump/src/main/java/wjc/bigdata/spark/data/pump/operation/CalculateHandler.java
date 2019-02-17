package wjc.bigdata.spark.data.pump.operation;

import java.io.Serializable;

/**
 * 计算处理器
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 19:24
 **/
public interface CalculateHandler extends Serializable {
    /**
     * 处理操作
     */
    void handle();
}
