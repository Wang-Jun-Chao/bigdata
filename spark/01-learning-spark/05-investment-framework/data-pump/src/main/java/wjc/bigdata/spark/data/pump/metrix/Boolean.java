package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public class Boolean extends Base<java.lang.Boolean> {

    public Boolean() {
    }

    public Boolean(java.lang.Boolean value) {
        super(value, 0, Type.BOOLEAN);
    }

    public Boolean(java.lang.Boolean value, int scala) {
        super(value, scala, Type.BOOLEAN);
    }
}
