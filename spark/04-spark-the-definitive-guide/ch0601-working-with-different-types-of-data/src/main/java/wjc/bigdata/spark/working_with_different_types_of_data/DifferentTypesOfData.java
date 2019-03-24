package wjc.bigdata.spark.working_with_different_types_of_data;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.util.PathUtils;
import wjc.bigdata.spark.util.SparkUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-24 09:30
 **/
public class DifferentTypesOfData {
    private final static Logger logger = LoggerFactory.getLogger(DifferentTypesOfData.class);


    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch0601-working-with-different-types-of-data")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PathUtils.workDir("../../../data/retail-data/by-day/2010-12-01.csv"));
        df.printSchema();
        df.createOrReplaceTempView("dfTable");

        df.select(
                functions.lit(5),
                functions.lit("five"),
                functions.lit(5.0)
        ).show(5);

        df.where(functions.column("InvoiceNo").equalTo(536365))
                .select("InvoiceNo", "Description")
                .show(5, false);

        df.where("InvoiceNo = 536365")
                .show(5, false);

        df.where("InvoiceNo <> 536365")
                .show(5, false);

        Column priceFilter = functions.column("UnitPrice").$greater(600);
        Column descripFilter = functions.column("Description").contains("POSTAGE");

        df.where(functions.col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
                .show();

        Column dotCodeFilter = functions.column("StockCode").equalTo("DOT");
        df.withColumn("isExpensive", dotCodeFilter.and(priceFilter.or(descripFilter)))
                .where("isExpensive")
                .select("unitPrice", "isExpensive")
                .show(5);

        df.withColumn("isExpensive", functions.not(functions.column("UnitPrice").leq(250)))
                .filter("isExpensive")
                .select("Description", "UnitPrice")
                .show(5);
        df.withColumn("isExpensive", functions.expr("NOT UnitPrice <= 250"))
                .filter("isExpensive")
                .select("Description", "UnitPrice")
                .show(5);

        df.where(functions.column("Description").eqNullSafe("hello")).show();

        Column fabricatedQuantity = functions.pow(
                functions.column("Quantity").multiply(functions.column("UnitPrice")),
                2)
                .$plus(5);

        df.select(functions.expr("CustomerId"), fabricatedQuantity.alias("realQuantity"))
                .show(2);

        df.selectExpr(
                "CustomerId",
                "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity")
                .show(2);

        df.select(functions.round(
                functions.column("UnitPrice"), 1).alias("rounded"),
                functions.column("UnitPrice"))
                .show(5);

        df.select(
                functions.round(functions.lit("2.5")),
                functions.bround(functions.lit("2.5")))
                .show(2);

        df.stat().corr("Quantity", "UnitPrice");
        df.select(functions.corr("Quantity", "UnitPrice"))
                .show();

        df.describe().show();

        String colName = "UnitPrice";
        double[] quantileProbs = new double[]{0.5};
        double relError = 0.05;
        double[] approxQuantile = df.stat().approxQuantile(colName, quantileProbs, relError); // 2.51
        System.out.println(Arrays.toString(approxQuantile));

        df.stat().crosstab("StockCode", "Quantity")
                .show();

        df.stat().freqItems(new String[]{"StockCode", "Quantity"})
                .show();

        df.select(functions.monotonically_increasing_id())
                .show(2);

        df.select(functions.initcap(functions.column("Description")))
                .show(2, false);

        df.select(
                functions.col("Description"),
                functions.lower(functions.col("Description")),
                functions.upper(functions.lower(functions.col("Description"))))
                .show(2);

        df.select(
                functions.ltrim(functions.lit("    HELLO    ")).as("ltrim"),
                functions.rtrim(functions.lit("    HELLO    ")).as("rtrim"),
                functions.trim(functions.lit("    HELLO    ")).as("trim"),
                functions.lpad(functions.lit("HELLO"), 3, " ").as("lp"),
                functions.rpad(functions.lit("HELLO"), 10, " ").as("rp"))
                .show(2);


        String regexString = StringUtils.join("black", "white", "red", "green", "blue")
                .toUpperCase();
        // the | signifies `OR` in regular expression syntax
        df.select(
                functions.regexp_replace(
                        functions.col("Description"),
                        regexString,
                        "COLOR")
                        .alias("color_clean"),
                functions.col("Description"))
                .show(2);

        df.select(
                functions.translate(
                        functions.col("Description"),
                        "LEET", "1337"),
                functions.col("Description"))
                .show(2);

        df.select(
                functions.regexp_extract(functions.column("Description"), regexString, 1)
                        .alias("color_clean"),
                functions.column("Description"))
                .show(2);

        Column containsBlack = functions.column("Description").contains("BLACK");
        Column containsWhite = functions.column("DESCRIPTION").contains("WHITE");
        df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
                .where("hasSimpleColor")
                .select("Description")
                .show(3, false);

//        Seq<String> simpleColors = SparkUtils.seq(Arrays.asList("black", "white", "red", "green", "blue"));
//        selectedColumns = simpleColors.map(color -> {
//                col("Description").contains(color.toUpperCase).alias(s"is_$color")
//        }):+expr("*") // could also append this value
//        df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
//                .select("Description").show(3, false);

        Dataset<Row> dateDF = spark.range(10)
                .withColumn("today", functions.current_date())
                .withColumn("now", functions.current_timestamp());
        dateDF.createOrReplaceTempView("dateTable");

        dateDF.printSchema();

        dateDF.select(functions.date_sub(
                functions.col("today"), 5),
                functions.date_add(functions.column("today"), 5))
                .show(1);

        dateDF.withColumn("week_ago", functions.date_sub(functions.col("today"), 7))
                .select(functions.datediff(functions.column("week_ago"), functions.col("today")))
                .show(1);
        dateDF.select(
                functions.to_date(functions.lit("2016-01-01")).alias("start"),
                functions.to_date(functions.lit("2017-05-22")).alias("end"))
                .select(functions.months_between(functions.col("start"), functions.col("end")))
                .show(1);

        spark.range(5)
                .withColumn("date", functions.lit("2017-01-01"))
                .select(functions.to_date(functions.col("date")))
                .show(1);

        dateDF.select(functions.to_date(
                functions.lit("2016-20-12")),
                functions.to_date(functions.lit("2017-12-11")))
                .show(1);

        String dateFormat = "yyyy-dd-MM";
        Dataset cleanDateDF = spark.range(1).select(
                functions.to_date(functions.lit("2017-12-11"), dateFormat).alias("date"),
                functions.to_date(functions.lit("2017-20-12"), dateFormat).alias("date2"));
        cleanDateDF.createOrReplaceTempView("dateTable2");
        cleanDateDF.select(functions.to_timestamp(functions.col("date"), dateFormat))
                .show();

        cleanDateDF.filter(functions.col("date2").$greater(functions.lit("2017-12-12")))
                .show();
        cleanDateDF.filter(functions.col("date2").$greater("2017-12-12"))
                .show();
        df.select(functions.coalesce(functions.col("Description"), functions.col("CustomerId")))
                .show();

        df.na().drop().show(5);
        df.na().drop("any").show(5);
        df.na().drop("all").show(5);
        df.na().drop("all", SparkUtils.seq(Arrays.asList("StockCode", "InvoiceNo"))).show(5);
        df.na().fill("All Null values become this string").show(5);
        df.na().fill(5, SparkUtils.seq(Arrays.asList("StockCode", "InvoiceNo"))).show(5);

        Map<String, Object> fillColValues = new HashMap<String, Object>();
        fillColValues.put("StockCode", 5);
        fillColValues.put("Description", "No Value");
        df.na().fill(fillColValues).show(5);

        Map<String, String> map = new HashMap<>();
        map.put("", "UNKNOWN");
        df.na().replace("Description", map).show(5);
        df.selectExpr("(Description, InvoiceNo) as complex", "*").take(5);
        df.selectExpr("struct(Description, InvoiceNo) as complex", "*").take(5);

        Dataset complexDF = df.select(functions.struct("Description", "InvoiceNo").alias("complex"));
        complexDF.createOrReplaceTempView("complexDF");

        complexDF.select("complex.Description").take(5);
        complexDF.select(functions.col("complex").getField("Description"));
        complexDF.select("complex.*").take(5);
        df.select(functions.split(functions.col("Description"), " ")).show(2);
        df.select(functions.split(functions.col("Description"), " ").alias("array_col"))
                .selectExpr("array_col[0]").show(2);

        df.select(functions.size(functions.split(functions.col("Description"), " "))).show(2); // shows 5 and 3
        df.select(functions.array_contains(functions.split(functions.col("Description"), " "), "WHITE")).show(2);

        df.withColumn("splitted", functions.split(functions.col("Description"), " "))
                .withColumn("exploded", functions.explode(functions.col("splitted")))
                .select("Description", "InvoiceNo", "exploded").show(2);
        df.select(functions.map(functions.col("Description"), functions.col("InvoiceNo")).alias("complex_map")).show(2);

        df.select(functions.map(functions.col("Description"), functions.col("InvoiceNo")).alias("complex_map"))
                .selectExpr("complex_map['WHITE METAL LANTERN']").show(2);

        df.select(functions.map(functions.col("Description"), functions.col("InvoiceNo")).alias("complex_map"))
                .selectExpr("explode(complex_map)").show(2);

        Dataset jsonDF = spark.range(1).selectExpr("'{\"myJSONKey\":{\"myJSONValue\":[1,2,3]}}' as jsonString");
        jsonDF.take(5);

        jsonDF.select(
                functions.get_json_object(functions.col("jsonString"), "$.myJSONKey.myJSONValue[1]").as("column"),
                functions.json_tuple(functions.col("jsonString"), "myJSONKey")).show(2);
        jsonDF.selectExpr(
                "json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as column").show(2);
        df.selectExpr("(InvoiceNo, Description) as myStruct")
                .select(functions.to_json(functions.col("myStruct"))).show(5);

        StructType parseSchema = new StructType(new StructField[]{
                new StructField("InvoiceNo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Description", DataTypes.StringType, true, Metadata.empty())});
        df.selectExpr("(InvoiceNo, Description) as myStruct")
                .select(functions.to_json(functions.col("myStruct")).alias("newJSON"))
                .select(functions.from_json(functions.col("newJSON"), parseSchema), functions.col("newJSON")).show(2);
    }
}
