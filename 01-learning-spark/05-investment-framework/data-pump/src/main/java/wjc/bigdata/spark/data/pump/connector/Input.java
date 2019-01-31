package wjc.bigdata.spark.data.pump.connector;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 数据输入连接器
 *
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 14:00
 **/
public interface Input extends Serializable {

    /**
     * 根据类名获取对象
     *
     * @param name 属性名
     * @param <T>  泛型参数
     * @return 属性对应的对象
     */
    <T> T getAttribute(String name);

    List<Object> getAttributeValues(String name);

    /**
     * 设置属性
     *
     * @param name  属性名
     * @param value 属性值
     */
    void setAttribute(String name, Object value);

    /**
     * 删除属性
     *
     * @param name 属性名
     */
    void removeAttribute(String name);

    /**
     * 获取所属性对象
     *
     * @return 属性对象集合
     */
    List<Object> getAttributes();

    /**
     * 获取名称和属性对象
     *
     * @return 名称和属性对象
     */
    Map<String, ? extends List> getAttributeMap();

    JavaRDDLike getJavaRDD();

    RDD getRDD();

    JavaDStreamLike getJavaDStream();

    DStream getDStream();

    JavaStreamingContext getJavaStreamingContext();

    <T> T findFirst(Class<T> type);

    <T> Collection<T> findAll(Class<T> type);
}
