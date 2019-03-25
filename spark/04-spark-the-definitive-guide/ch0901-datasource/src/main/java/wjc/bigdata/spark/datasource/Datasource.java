package wjc.bigdata.spark.datasource;

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
import wjc.bigdata.spark.util.PathUtils;

import java.util.Arrays;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-25 21:55
 **/
public class Datasource {
    private final static Logger logger = LoggerFactory.getLogger(Datasource.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch0801-joins")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        StructType idStructType = new StructType(new StructField[]{
                new StructField(
                        "DEST_COUNTRY_NAME",
                        DataTypes.StringType,
                        true,
                        Metadata.empty())
        });

        List<GenericRow> rows = Arrays.asList(
                new GenericRow(new Object[]{1}),
                new GenericRow(new Object[]{2}),
                new GenericRow(new Object[]{3}));

        Object obj = rows;
        Dataset<Row> dataFrame = spark.createDataFrame(
                (List<Row>) obj,
                idStructType);
        dataFrame.createOrReplaceTempView("temp");

        dataFrame.write()
                .option("path", PathUtils.outputPath("output"));

        dataFrame.write().format("csv")
                .option("mode", "OVERWRITE")
                .option("dateFormat", "yyyy-MM-dd")
                .option("path",  PathUtils.outputPath("output2"))
                .save();

        StructType myManualSchema = new StructType(new StructField[]{
                new StructField("DEST_COUNTRY_NAME", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true, Metadata.empty()),
                new StructField("count", DataTypes.LongType, false, Metadata.empty())});

        spark.read().format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(myManualSchema)
                .load(PathUtils.workDir("../../../data/flight-data/csv/2010-summary.csv"))
                .show(5);


        myManualSchema = new StructType(new StructField[]{
                new StructField("DEST_COUNTRY_NAME", DataTypes.LongType, true, Metadata.empty()),
                new StructField("ORIGIN_COUNTRY_NAME", DataTypes.LongType, true, Metadata.empty()),
                new StructField("count", DataTypes.LongType, false, Metadata.empty())});

        Row[] rows1 = spark.read().format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(myManualSchema)
                .load(PathUtils.workDir("../../../data/flight-data/csv/2010-summary.csv"))
                .take(5);
        System.out.println(Arrays.toString(rows1));

        Dataset<Row> csvFile = spark.read().format("csv")
                .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
                .load(PathUtils.workDir("../../../data/flight-data/csv/2010-summary.csv"));

        csvFile.show(5);

        csvFile.write()
                .format("csv")
                .mode("overwrite")
                .option("sep", "\t")
                .save(PathUtils.outputPath("/tmp/my-tsv-file.tsv"));

        spark.read()
                .format("json")
                .option("mode", "FAILFAST")
                .schema(myManualSchema)
                .load(PathUtils.workDir("../../../data/flight-data/json/2010-summary.json"))
                .show(5);

        csvFile.write()
                .format("json")
                .mode("overwrite")
                .save(PathUtils.workDir("/tmp/my-json-file.json"));


    }
}
