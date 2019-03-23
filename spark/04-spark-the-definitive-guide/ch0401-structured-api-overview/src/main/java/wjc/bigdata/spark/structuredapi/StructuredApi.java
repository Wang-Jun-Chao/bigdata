package wjc.bigdata.spark.structuredapi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-23 16:52
 **/
public class StructuredApi {
    private final static Logger logger = LoggerFactory.getLogger(StructuredApi.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch0401-structured-api-overview")
                .getOrCreate();

        Dataset<Row> df = spark.range(500).toDF("number");
        df.select(df.col("number").plus(10)).show();

        Object [] dataObjects = (Object[])spark.range(2).toDF().collect();
        for(Object object: dataObjects) {
            System.out.println(object.toString());
        }
    }
}
