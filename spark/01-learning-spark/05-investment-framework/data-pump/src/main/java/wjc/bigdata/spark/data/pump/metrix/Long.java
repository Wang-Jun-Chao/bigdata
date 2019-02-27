package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public class Long extends Base<java.lang.Long> {

    public Long() {
    }

    public Long(java.lang.Long value) {
        super(value, 0, Type.LONG);
    }

    public Long(java.lang.Long value, int scala) {
        super(value, scala, Type.LONG);
    }
}
