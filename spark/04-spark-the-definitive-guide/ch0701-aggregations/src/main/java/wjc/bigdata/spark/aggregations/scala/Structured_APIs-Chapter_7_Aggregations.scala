// in Scala
val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/data/retail-data/all/*.csv")
    .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")


// COMMAND ----------

df.count() == 541909


// COMMAND ----------

// in Scala
import org.apache.spark

df.select(count("StockCode")).show() // 541909


// COMMAND ----------

// in Scala
df.select(countDistinct("StockCode")).show() // 4070


// COMMAND ----------

// in Scala
df.select(approx_count_distinct("StockCode", 0.1)).show() // 3364


// COMMAND ----------

// in Scala
df.select(first("StockCode"), last("StockCode")).show()


// COMMAND ----------

// in Scala
df.select(min("Quantity"), max("Quantity")).show()


// COMMAND ----------

// in Scala
df.select(sum("Quantity")).show() // 5176450


// COMMAND ----------

// in Scala
df.select(sumDistinct("Quantity")).show() // 29310


// COMMAND ----------

// in Scala

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))
    .selectExpr(
        "total_purchases/total_transactions",
        "avg_purchases",
        "mean_purchases").show()


// COMMAND ----------

// in Scala
df.select(var_pop("Quantity"), var_samp("Quantity"),
    stddev_pop("Quantity"), stddev_samp("Quantity")).show()


// COMMAND ----------
df.select(skewness("Quantity"), kurtosis("Quantity")).show()


// COMMAND ----------

// in Scala
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()


// COMMAND ----------

// in Scala
df.agg(collect_set("Country"), collect_list("Country")).show()


// COMMAND ----------

df.groupBy("InvoiceNo", "CustomerId").count().show()


// COMMAND ----------

// in Scala

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()


// COMMAND ----------

// in Scala
df.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show()


// COMMAND ----------

// in Scala
val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


// COMMAND ----------

// in Scala
val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)


// COMMAND ----------
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)


// COMMAND ----------

// in Scala
val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)


// COMMAND ----------

// in Scala

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
    .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


// COMMAND ----------

// in Scala
val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")


// COMMAND ----------

val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date")
rolledUpDF.show()


// COMMAND ----------

rolledUpDF.where("Country IS NULL").show()


// COMMAND ----------

rolledUpDF.where("Date IS NULL").show()


// COMMAND ----------

// in Scala
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
    .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()


// COMMAND ----------

// in Scala

dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
    .orderBy(expr("grouping_id()").desc)
    .show()


// COMMAND ----------

// in Scala
val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()


// COMMAND ----------

pivoted.where("date > '2011-12-05'").select("date", "`USA_sum(Quantity)`").show()


// COMMAND ----------

// in Scala
class BoolAnd extends UserDefinedAggregateFunction {
    def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(StructField("value", BooleanType) :: Nil)

    def bufferSchema: StructType = StructType(
        StructField("result", BooleanType) :: Nil
    )

    def dataType: DataType = BooleanType

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = true
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
    }

    def evaluate(buffer: Row): Any = {
        buffer(0)
    }
}


// COMMAND ----------

// in Scala
val ba = new BoolAnd
spark.udf.register("booland", ba)
spark.range(1)
    .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
    .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
    .select(ba(col("t")), expr("booland(f)"))
    .show()


// COMMAND ----------

