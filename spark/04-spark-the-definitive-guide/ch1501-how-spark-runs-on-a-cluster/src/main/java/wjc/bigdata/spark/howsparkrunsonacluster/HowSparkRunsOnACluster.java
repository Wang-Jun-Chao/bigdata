package wjc.bigdata.spark.howsparkrunsonacluster;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-31 10:47
 **/
public class HowSparkRunsOnACluster {
    private final static Logger logger = LoggerFactory.getLogger(HowSparkRunsOnACluster.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Databricks Spark Example")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", 50);
    }
}
