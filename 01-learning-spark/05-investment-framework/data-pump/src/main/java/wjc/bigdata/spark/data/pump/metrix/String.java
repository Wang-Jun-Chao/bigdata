package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public class String extends Base<java.lang.String> {

    public String() {
    }

    public String(java.lang.String value) {
        super(null, 0, Type.STRING);
    }

    public String(java.lang.String value, int scala) {
        super(null, scala, Type.STRING);
    }
}
