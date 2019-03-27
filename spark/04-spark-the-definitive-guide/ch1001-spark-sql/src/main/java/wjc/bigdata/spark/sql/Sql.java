package wjc.bigdata.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.util.PathUtils;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-27 22:16
 **/
public class Sql {
    private final static Logger logger = LoggerFactory.getLogger(Sql.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch1001-spark-sql")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        spark.sql("SELECT 1 + 1").show();

        spark.read()
                .json(PathUtils.workDir("../../../data/flight-data/json/2015-summary.json"))
                .createOrReplaceTempView("some_sql_view");
        long count = spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) "
                + "FROM some_sql_view GROUP BY DEST_COUNTRY_NAME ")
                .where("DEST_COUNTRY_NAME like 'S%'")
                .where("`sum(count)` > 10")
                .count();
        System.out.println(count);

        Dataset<Row> flights = spark.read().format("json")
                .load(PathUtils.workDir("../../../data/flight-data/json/2015-summary.json"));
        Dataset<Row> justUsaDf = flights.where("dest_country_name = 'United States'");
        justUsaDf.selectExpr("*").explain();

        spark.udf().register("power3", new UDF1<Double, Double>() {
            @Override
            public Double call(Double o) throws Exception {
                return o == null ? null : o * o * o;
            }
        }, DataTypes.DoubleType);

    }
}
