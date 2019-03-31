package wjc.bigdata.spark.sprakapplication;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-31 16:04
 **/
public class SparkApplication {
    private final static Logger logger = LoggerFactory.getLogger(SparkApplication.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("ch1601-spark-applications")
                .set("some.conf", "to.some.value");
    }
}
