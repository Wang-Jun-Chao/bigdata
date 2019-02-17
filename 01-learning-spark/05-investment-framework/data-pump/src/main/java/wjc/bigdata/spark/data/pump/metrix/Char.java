package wjc.bigdata.spark.data.pump.metrix;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:24
 **/
public class Char extends Base<Character> {

    public Char() {
    }

    public Char(Character value) {
        super(value, 0, Type.CHAR);
    }

    public Char(Character value, int scala) {
        super(value, scala, Type.CHAR);
    }
}
