package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public class Base<T> implements Value<T>{
    private T value;
    private int scale;
    private Type type;

    public Base() {
    }

    public Base(T value, int scale, Type type) {
        this.value = value;
        this.scale = scale;
        this.type = type;
    }

    @Override
    public T data() {
        return value;
    }

    @Override
    public void update(T t) {
        this.value = value;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public int scale() {
        return scale;
    }
}
