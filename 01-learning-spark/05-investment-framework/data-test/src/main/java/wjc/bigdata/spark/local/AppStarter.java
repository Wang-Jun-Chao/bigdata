package wjc.bigdata.spark.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.SparkApplication;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 10:07
 **/
public class AppStarter {
    private final static Logger logger = LoggerFactory.getLogger(AppStarter.class);

    public static void main(String[] args) {
        SparkApplication.run(AppStarter.class, args);
    }
}
