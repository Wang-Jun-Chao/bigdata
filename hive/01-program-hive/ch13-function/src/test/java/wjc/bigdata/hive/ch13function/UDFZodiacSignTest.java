package wjc.bigdata.hive.ch13function;

import org.junit.Test;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-05-31 07:59
 **/
public class UDFZodiacSignTest {
    @Test
    public void test() {

        UDFZodiacSign aa = new UDFZodiacSign();
        String str = aa.evaluate("01-10-2004");
        System.out.println(str);
    }
}