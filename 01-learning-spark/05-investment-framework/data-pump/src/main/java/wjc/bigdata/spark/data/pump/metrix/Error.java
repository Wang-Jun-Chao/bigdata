package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public class Error extends Base<String> {

    public Error() {
    }

    public Error(String value) {
        super(null, 0, Type.ERROR);
    }

    public Error(String value, int scala) {
        super(null, scala, Type.ERROR);
    }
}
