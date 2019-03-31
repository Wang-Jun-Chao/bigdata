package wjc.bigdata.spark.deployingsprak;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-31 16:04
 **/
public class DeployingSpark {
    private final static Logger logger = LoggerFactory.getLogger(DeployingSpark.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("mesos://HOST:5050")
                .appName("ch1701-deploying-spark")
                .config("spark.executor.uri", "<path to spark-2.2.0.tar.gz uploaded above>")
                .getOrCreate();
    }
}
