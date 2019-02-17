package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public class Double extends Base<Long> {

    public Double() {
    }

    public Double(Long value) {
        super(value, 0, Type.DOUBLE);
    }

    public Double(Long value, int scala) {
        super(value, scala, Type.DOUBLE);
    }
}
