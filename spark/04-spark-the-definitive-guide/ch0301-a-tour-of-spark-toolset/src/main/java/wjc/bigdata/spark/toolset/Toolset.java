package wjc.bigdata.spark.toolset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassManifestFactory;
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

        Dataset<Row> preppedDataFrame = staticDataFrame
                .na().fill(0)
                .withColumn("day_of_week", functions.date_format(functions.col("InvoiceDate"), "EEEE"))
                .coalesce(5);
        preppedDataFrame.printSchema();


        Dataset<Row> trainDataFrame = preppedDataFrame
                .where("InvoiceDate < '2011-07-01'");
        Dataset<Row> testDataFrame = preppedDataFrame
                .where("InvoiceDate >= '2011-07-01'");

        trainDataFrame.count();
        testDataFrame.count();


        StringIndexer indexer = new StringIndexer()
                .setInputCol("day_of_week")
                .setOutputCol("day_of_week_index");

        OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
                .setInputCols(new String[]{"day_of_week_index"})
                .setOutputCols(new String[]{"day_of_week_encoded"});


        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"UnitPrice", "Quantity", "day_of_week_encoded"})
                .setOutputCol("features");

        Pipeline transformationPipeline = new Pipeline()
                .setStages(new PipelineStage[]{indexer, encoder, vectorAssembler});

        PipelineModel fittedPipeline = transformationPipeline.fit(trainDataFrame);

        Dataset<Row> transformedTraining = fittedPipeline.transform(trainDataFrame);
        transformedTraining.cache();

        KMeans kmeans = new KMeans()
                .setK(20)
                .setSeed(1L);
        KMeansModel kmModel = kmeans.fit(transformedTraining);

        Dataset<Row> transformedTest = fittedPipeline.transform(testDataFrame);
        System.out.println(kmModel.computeCost(transformedTest));


        Seq<Integer> seq = JavaConverters
                .asScalaIteratorConverter(Arrays.asList(1, 2, 3)
                .iterator())
                .asScala()
                .toSeq();

        Object integers = spark.sparkContext().<Integer>parallelize(
                seq, 1,
                ClassManifestFactory.<Integer>classType(Integer.class))
                .take(5);
        System.out.println(Arrays.toString((Object[]) integers));

    }
}
