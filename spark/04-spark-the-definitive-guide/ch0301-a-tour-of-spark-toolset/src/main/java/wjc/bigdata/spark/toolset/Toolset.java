package wjc.bigdata.spark.toolset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import wjc.bigdata.spark.util.PathUtils;

import java.util.Arrays;
//import static org.apache.spark.sql.functions.*;

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
                "../../../data/flight-data/parquet/2010-summary.parquet/"));

        Dataset<Row> flights = flightsDF.as("Flight");

        Object take = flights.filter(
                (FilterFunction<Row>) row -> !"Canada".equalsIgnoreCase(row.getAs("ORIGIN_COUNTRY_NAME"))
        ).map((MapFunction<Row, Row>) value -> value, Encoders.kryo(Row.class)).take(5);

        Row[] rows = (Row[]) take;
        System.out.println(take.getClass() + " " + Arrays.toString(rows));

        Dataset<Row> staticDataFrame = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PathUtils.workDir("../../../data/retail-data/by-day/*.csv"));


        staticDataFrame.createOrReplaceTempView("retail_data");
        StructType staticSchema = staticDataFrame.schema();
        System.out.println(staticSchema.getClass());

        staticDataFrame
                .selectExpr(
                        "CustomerId",
                        "(UnitPrice * Quantity) as total_cost",
                        "InvoiceDate")
                .groupBy(
                        functions.col("CustomerId"),
                        functions.window(functions.col("InvoiceDate"), "1 day"))
                .sum("total_cost")
                .show(5);



        staticDataFrame.createOrReplaceTempView("retail_data");
        spark.conf().set("spark.sql.shuffle.partitions", "5");
        Dataset<Row> streamingDataFrame = spark.readStream()
                .schema(staticSchema)
                .option("maxFilesPerTrigger", 1)
                .format("csv")
                .option("header", "true")
                .load(PathUtils.workDir("../../../data/retail-data/by-day/*.csv"));
        System.out.println(streamingDataFrame.isStreaming());

        Dataset<Row> purchaseByCustomerPerHour = streamingDataFrame
                .selectExpr(
                        "CustomerId",
                        "(UnitPrice * Quantity) as total_cost",
                        "InvoiceDate")
                .groupBy(
                        functions.col("CustomerId"),
                        functions.window(functions.col("InvoiceDate"), "1 day"))
                .sum("total_cost");

        purchaseByCustomerPerHour.writeStream()
                .format("memory")                   // memory = store in-memory table
                .queryName("customer_purchases")    // the name of the in-memory table
                .outputMode("complete")             // complete = all the counts should be in the table
                .start();
        spark.sql(" SELECT * "
                + " FROM customer_purchases "
                + " ORDER BY `sum(total_cost)` DESC "
        ).show(5);

        staticDataFrame.printSchema();
    }
}
