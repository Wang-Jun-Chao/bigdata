package wjc.bigdata.spark.basic_structure_operations;

import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import wjc.bigdata.spark.util.PathUtils;

import java.util.Arrays;


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
        df.printSchema();


        StructType schema = spark.read().format("json").load(PathUtils.workDir(
                "../../../data/flight-data/json/2015-summary.json")).schema();
        System.out.println(schema);

        StructType myManualSchema = new StructType(new StructField[]{
                new StructField("DEST_COUNTRY_NAME", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true, Metadata.empty()),
                new StructField("count", DataTypes.LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))}
        );

        df = spark.read().format("json").schema(myManualSchema).load(PathUtils.workDir(
                "../../../data/flight-data/json/2015-summary.json"));
        df.printSchema();

        df.first();

        String[] columns = spark.read().format("json").load(PathUtils.workDir(
                "../../../data/flight-data/json/2015-summary.json")).columns();
        System.out.println(Arrays.toString(columns));

        Row myRow = new GenericRow(new Object[]{"Hello", null, 1, false});
        System.out.println(myRow.get(0));
        System.out.println(myRow.get(0) instanceof String);
        System.out.println(myRow.getString(0));
        System.out.println(myRow.getInt(2));

        df = spark.read().format("json").load(PathUtils.workDir(
                "../../../data/flight-data/json/2015-summary.json"));
        df.createOrReplaceTempView("dfTable");
        myManualSchema = new StructType(new StructField[]{
                new StructField("some", DataTypes.StringType, true, Metadata.empty()),
                new StructField("col", DataTypes.StringType, true, Metadata.empty()),
                new StructField("names", DataTypes.LongType, false, Metadata.empty())}
        );

        Seq<GenericRow> myRows = JavaConverters
                .asScalaIteratorConverter(Arrays.asList(new GenericRow(new Object[]{"Hello", null, 1L})).iterator())
                .asScala()
                .toSeq();
        // java 泛型不支持协变所以先变成 obj 再变成目标类型
        Object obj = myRows;
        RDD<Row> myRDD = spark.sparkContext().parallelize(
                (Seq) obj,
                spark.sparkContext().defaultParallelism(),
                JavaSparkContext$.MODULE$.fakeClassTag());
        Dataset<Row> myDf = spark.createDataFrame(myRDD, myManualSchema);
        myDf.show();


        df.select("DEST_COUNTRY_NAME")
                .show(2);
        df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
                .show(2);
        df.select(
                df.col("DEST_COUNTRY_NAME"),
                functions.column("DEST_COUNTRY_NAME"),
                functions.column("DEST_COUNTRY_NAME"),
                functions.column("DEST_COUNTRY_NAME"),
                functions.expr("DEST_COUNTRY_NAME"))
                .show(2);
        df.select(functions.expr("DEST_COUNTRY_NAME AS destination"))
                .show(2);
        df.select(functions.expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))
                .show(2);
        df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME")
                .show(2);
        df.selectExpr(
                "*", // include all original columns
                "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
                .show(2);
        df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")
                .show(2);
        df.select(functions.expr("*"), functions.lit(1).as("One"))
                .show(2);
        df.withColumn("numberOne", functions.lit(1))
                .show(2);
        df.withColumn("withinCountry", functions.expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
                .show(2);

        columns = df.withColumn("Destination", functions.expr("DEST_COUNTRY_NAME")).columns();
        System.out.println(Arrays.toString(columns));
        columns = df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns();
        System.out.println(Arrays.toString(columns));

        Dataset<Row> dfWithLongColName = df.withColumn(
                "This Long Column-Name",
                functions.expr("ORIGIN_COUNTRY_NAME"));
        dfWithLongColName.show(2);
        dfWithLongColName.selectExpr(
                "`This Long Column-Name`",
                "`This Long Column-Name` as `new col`")
                .show(2);
        dfWithLongColName.createOrReplaceTempView("dfTableLong");
        columns = dfWithLongColName.select(functions.column("This Long Column-Name")).columns();
        System.out.println(Arrays.toString(columns));

        columns = df.drop("ORIGIN_COUNTRY_NAME").columns();
        System.out.println(Arrays.toString(columns));

        dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME");
        df.withColumn("count2", functions.column("count").cast("long"))
                .take(2);
        df.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row value) throws Exception {
                return (long) value.getAs("count") < 2;
            }
        }).show(2);
        df.where("count < 2").show(2);

        df.where("count < 2").where("ORIGIN_COUNTRY_NAME != \"Croatia\"")
                .show(2);
        long count = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count();
        System.out.println(count);

        count = df.select("ORIGIN_COUNTRY_NAME").distinct().count();
        System.out.println(count);

        long seed = 5;
        boolean withReplacement = false;
        double fraction = 0.5;
        count = df.sample(withReplacement, fraction, seed).count();
        System.out.println(count);

        Dataset[] dataFrames = df.randomSplit(new double[]{0.25, 0.75}, seed);
        System.out.println(dataFrames[0].count() > dataFrames[1].count());

        schema = df.schema();
        Seq<GenericRow> newRows = JavaConverters
                .asScalaIteratorConverter(
                        Arrays.asList(
                                new GenericRow(new Object[]{"New Country", "Other Country", 5L}),
                                new GenericRow(new Object[]{"New Country 2", "Other Country 3", 1L})
                        ).iterator())
                .asScala()
                .toSeq();

        obj = newRows;

        RDD<Row> parallelizedRows = spark.sparkContext().parallelize(
                (Seq) obj,
                spark.sparkContext().defaultParallelism(),
                JavaSparkContext$.MODULE$.fakeClassTag());

        Dataset newDF = spark.createDataFrame(parallelizedRows, schema);
        df.union(newDF)
                .where("count = 1")
                .where("ORIGIN_COUNTRY_NAME != \"United States\"")
                .show(); // get all of them and we'll see our new rows at the end;

        df.sort("count")
                .show(5);
        df.orderBy("count", "DEST_COUNTRY_NAME")
                .show(5);
        df.orderBy(functions.col("count"), functions.col("DEST_COUNTRY_NAME"))
                .show(5);

        df.orderBy(functions.expr("count desc"))
                .show(2);
        df.orderBy(functions.desc("count"), functions.asc("DEST_COUNTRY_NAME"))
                .show(2);

        df = spark.read().format("json").load(PathUtils.workDir(
                "../../../data/flight-data/json/*-summary.json"));
        df.limit(5).show();
        df.orderBy(functions.expr("count desc")).limit(6).show();
        System.out.println(df.rdd().getNumPartitions());

        df.repartition(5);
        df.repartition(functions.col("DEST_COUNTRY_NAME"));
        df.repartition(5, functions.col("DEST_COUNTRY_NAME"));
        df.repartition(5, functions.col("DEST_COUNTRY_NAME")).coalesce(2);

        Dataset collectDF = df.limit(10);
        collectDF.take(5); // take works with an Integer count
        collectDF.show(); // this prints it out nicely
        collectDF.show(5, false);
        collectDF.collect();

        collectDF.toLocalIterator();

    }
}
