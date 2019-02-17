package wjc.bigdata.spark.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.SparkApplication;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-12 10:49
 **/
public class KafkaSparkRunner {
    private final static Logger logger = LoggerFactory.getLogger(KafkaSparkRunner.class);

    public static void main(String[] args) {
        SparkApplication.run(KafkaSparkRunner.class, args);
    }
}
