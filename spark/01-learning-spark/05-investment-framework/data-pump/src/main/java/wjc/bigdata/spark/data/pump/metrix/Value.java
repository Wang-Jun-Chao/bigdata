package wjc.bigdata.spark.data.pump.metrix;

import java.io.Serializable;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public interface Value<T>  extends Serializable {

    T data();

    void update(T data);

    Type type();

    int scale();
}
