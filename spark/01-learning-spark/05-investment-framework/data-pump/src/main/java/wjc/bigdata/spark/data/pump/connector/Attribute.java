package wjc.bigdata.spark.data.pump.connector;

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
public interface Attribute<T> extends Serializable {

    /**
     * 根据类名获取对象
     *
     * @param name 属性名
     * @param <V>  泛型参数
     * @return 属性对应的对象
     */
    <V> V getAttribute(String name);

    /**
     * 获取所有的属性值
     *
     * @param name 属性值名称
     * @return
     */
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

    /**
     * 根据类型找出第一个值
     *
     * @param type 类型对的class对象
     * @param <V>  泛型参数
     * @return 找出的第一个值
     */
    <V> V findValue(Class<V> type);

    /**
     * 根据类型找出所有的值
     *
     * @param type 类型对的class对象
     * @param <V>  泛型参数
     * @return 找出的所有值
     */
    <V> Collection<V> findValues(Class<V> type);
}
