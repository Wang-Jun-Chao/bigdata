package wjc.bigdata.spark.basic_structure_operations;

import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
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
                .asScalaIteratorConverter(Arrays.asList(new GenericRow(new Object[]{"Hello", null, 1L}))
                        .iterator())
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
    }
}
