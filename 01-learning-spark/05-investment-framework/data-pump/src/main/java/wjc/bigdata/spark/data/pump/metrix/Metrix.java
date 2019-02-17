package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:23
 **/
public class Metrix<T> {
    private String name;
    private Value<T> value;

    public Metrix() {
    }

    public Metrix(String name, Value<T> value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Value<T> getValue() {
        return value;
    }

    public void setValue(Value<T> value) {
        this.value = value;
    }
}
