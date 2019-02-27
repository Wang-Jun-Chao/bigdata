package wjc.bigdata.spark.data.pump.connector;

import java.util.Collection;

/**
 * 数据输入连接器
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 14:00
 **/
public interface Input<T> extends Attribute<T> {
    /**
     * 读取数据的方法
     *
     * @return 返回读取的单个数据
     */
    T read();

    /**
     * 数据批量读取的方法
     *
     * @return 批量读取的单个数据
     */
    Collection<? extends T> batchRead();
}
