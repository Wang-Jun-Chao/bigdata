package wjc.bigdata.spark.basic_structure_operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.util.PathUtils;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-23 16:52
 **/
public class BasicStructuredOperations {
    private final static Logger logger = LoggerFactory.getLogger(BasicStructuredOperations.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch0501-basic-structured-operations")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("json").load(PathUtils.workDir(
                "../../../data/flight-data/json/2015-summary.json"));

    }
}
