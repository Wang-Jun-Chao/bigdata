package wjc.bigdata.hive.ch13function;

import org.junit.Test;

import java.util.Arrays;

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

    @Test
    public void testSplit() {
        String s = "a|b|c|d|e|f|g";
        System.out.println(Arrays.toString(s.split("\\|")));
    }
}