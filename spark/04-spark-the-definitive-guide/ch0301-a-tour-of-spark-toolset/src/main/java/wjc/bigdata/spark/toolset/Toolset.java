package wjc.bigdata.spark.toolset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import wjc.bigdata.spark.util.PathUtils;

import java.util.Arrays;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-12 19:53
 **/
public class Toolset {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch0301-a-tour-of-spark-toolset")
                .getOrCreate();

        Dataset<Row> flightsDF = spark.read().parquet(PathUtils.workDir(
                "/data/flight-data/parquet/2010-summary.parquet/"));

        Dataset<Row> flights = flightsDF.as("Flight");

        Object[] take = flights.filter(
                (FilterFunction<Row>) row -> !"Canada".equalsIgnoreCase(row.getAs("ORIGIN_COUNTRY_NAME"))
        ).map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                return value;
            }
        }, Encoders.<Row>kryo(Row.class)).take(5);

        Row[] rows = (Row[]) take;
        System.out.println(take.getClass() + " " + Arrays.toString(rows));
    }
}
