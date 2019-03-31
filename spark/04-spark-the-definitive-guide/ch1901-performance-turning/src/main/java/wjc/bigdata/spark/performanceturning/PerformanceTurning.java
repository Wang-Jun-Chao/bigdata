package wjc.bigdata.spark.performanceturning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.util.PathUtils;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-31 16:12
 **/
public class PerformanceTurning {
    private final static Logger logger = LoggerFactory.getLogger(PerformanceTurning.class);

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch1901-performance-turning")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        Dataset<Row> df1 = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(PathUtils.workDir("../../../data/flight-data/csv/2015-summary.csv"));
        Object df2 = df1.groupBy("DEST_COUNTRY_NAME").count().collect();
        Object df3 = df1.groupBy("ORIGIN_COUNTRY_NAME").count().collect();
        Object df4 = df1.groupBy("count").count().collect();

        df1.cache();
        df1.count();

        df2 = df1.groupBy("DEST_COUNTRY_NAME").count().collect();
        df3 = df1.groupBy("ORIGIN_COUNTRY_NAME").count().collect();
        df4 = df1.groupBy("count").count().collect();
    }

}
