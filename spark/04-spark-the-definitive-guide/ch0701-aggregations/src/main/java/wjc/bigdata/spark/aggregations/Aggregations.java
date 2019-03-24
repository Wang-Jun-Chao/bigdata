package wjc.bigdata.spark.aggregations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.util.PathUtils;
import wjc.bigdata.spark.util.SparkUtils;

import java.util.Collections;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-12 17:38
 **/
public class Aggregations {
    private final static Logger logger = LoggerFactory.getLogger(Aggregations.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch0701-aggregations")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PathUtils.workDir("../../../data/retail-data/all/*.csv"))
                .coalesce(5);
        long count = df.count();
        System.out.println("count: " + count + " == 541909" + (count == 541909));
        df.select(functions.count("StockCode")).show(); // 541909
        df.select(functions.countDistinct("StockCode")).show(); // 4070
        df.select(functions.approx_count_distinct("StockCode", 0.1)).show(); // 3364
        df.select(functions.first("StockCode"), functions.last("StockCode")).show();
        df.select(functions.min("Quantity"), functions.max("Quantity")).show();
        df.select(functions.sum("Quantity")).show(); // 5176450
        df.select(functions.sumDistinct("Quantity")).show(); // 29310
        df.select(
                functions.count("Quantity").alias("total_transactions"),
                functions.sum("Quantity").alias("total_purchases"),
                functions.avg("Quantity").alias("avg_purchases"),
                functions.expr("mean(Quantity)").alias("mean_purchases"))
                .selectExpr(
                        "total_purchases/total_transactions",
                        "avg_purchases",
                        "mean_purchases")
                .show();

        df.select(
                functions.var_pop("Quantity"),
                functions.var_samp("Quantity"),
                functions.stddev_pop("Quantity"),
                functions.stddev_samp("Quantity"))
                .show();
        df.select(
                functions.skewness("Quantity"),
                functions.kurtosis("Quantity"))
                .show();
        df.select(
                functions.corr("InvoiceNo", "Quantity"),
                functions.covar_samp("InvoiceNo", "Quantity"),
                functions.covar_pop("InvoiceNo", "Quantity"))
                .show();
        df.agg(
                functions.collect_set("Country"),
                functions.collect_list("Country"))
                .show();
        df.groupBy("InvoiceNo", "CustomerId")
                .count()
                .show();

        df.groupBy("InvoiceNo")
                .agg(
                        functions.count("Quantity").alias("quan"),
                        functions.expr("count(Quantity)"))
                .show();

        df.groupBy("InvoiceNo").agg(
                functions.avg("Quantity"),
                functions.stddev_pop("Quantity"))
                .show();

        Dataset dfWithDate = df.withColumn(
                "date",
                functions.to_date(functions.col("InvoiceDate"),
                        "MM/d/yyyy H:mm"));
        dfWithDate.createOrReplaceTempView("dfWithDate");

        WindowSpec windowSpec = Window
                .partitionBy("CustomerId", "date")
                .orderBy(functions.col("Quantity").desc())
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());
        Column maxPurchaseQuantity = functions
                .max(functions.col("Quantity"))
                .over(windowSpec);
        System.out.println(maxPurchaseQuantity);

        Column purchaseDenseRank = functions.dense_rank().over(windowSpec);
        Column purchaseRank = functions.rank().over(windowSpec);
        System.out.println(purchaseDenseRank);
        System.out.println(purchaseRank);

        dfWithDate.where("CustomerId IS NOT NULL")
                .orderBy("CustomerId")
                .select(
                        functions.col("CustomerId"),
                        functions.col("date"),
                        functions.col("Quantity"),
                        purchaseRank.alias("quantityRank"),
                        purchaseDenseRank.alias("quantityDenseRank"),
                        maxPurchaseQuantity.alias("maxPurchaseQuantity"))
                .show();

        Dataset dfNoNull = dfWithDate.drop();
        dfNoNull.createOrReplaceTempView("dfNoNull");

        Dataset<Row> rolledUpDF = dfNoNull
                .rollup("Date", "Country")
                .agg(functions.sum("Quantity"))
                .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
                .orderBy("Date");
        rolledUpDF.show();

        rolledUpDF.where("Country IS NULL").show();
        rolledUpDF.where("Date IS NULL").show();
        dfNoNull.cube("Date", "Country")
                .agg(functions.sum(functions.col("Quantity")))
                .select("Date", "Country", "sum(Quantity)")
                .orderBy("Date")
                .show();

        dfNoNull.cube("customerId", "stockCode")
                .agg(functions.grouping_id(
                        SparkUtils.seq(Collections.EMPTY_LIST)),
                        functions.sum("Quantity"))
                .orderBy(functions.expr("grouping_id()").desc())
                .show();

        Dataset<Row> pivoted = dfWithDate.groupBy("date").pivot("Country").sum();
        pivoted.where("date > '2011-12-05'")
                .select("date", "`USA_sum(Quantity)`")
                .show();


        BoolAnd ba = new BoolAnd();
        spark.udf().register("booland", ba);
        spark.range(1)
                .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
                .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
                .select(
                        ba.apply(functions.col("t")),
                        functions.expr("booland(f)"))
                .show();

    }

    public static class BoolAnd extends UserDefinedAggregateFunction {
        @Override
        public StructType inputSchema() {
            return new StructType(new StructField[]{
                    new StructField(
                            "value",
                            DataTypes.BooleanType,
                            true,
                            Metadata.empty()
                    )
            });
        }

        @Override
        public StructType bufferSchema() {
            return new StructType(new StructField[]{
                    new StructField(
                            "result",
                            DataTypes.BooleanType,
                            true,
                            Metadata.empty()
                    )
            });
        }

        @Override
        public DataType dataType() {
            return DataTypes.BooleanType;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, true);
        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            buffer.update(0, buffer.getBoolean(0) && input.getBoolean(0));
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            buffer1.update(0, buffer1.getBoolean(0) && buffer2.getBoolean(0));
        }

        @Override
        public Object evaluate(Row buffer) {
            return buffer.get(0);
        }
    }
}
