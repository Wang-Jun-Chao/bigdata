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
                        DataTypes.IntegerType,
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

//        dataFrame.write().format("csv")
//                .option("mode", "OVERWRITE")
//                .option("dateFormat", "yyyy-MM-dd")
//                .option("path", PathUtils.outputPath("output2"))
//                .save();

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

        Object rows1 = spark.read().format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(myManualSchema)
                .load(PathUtils.workDir("../../../data/flight-data/csv/2010-summary.csv"))
                .take(5);
        System.out.println(Arrays.toString((Object[]) rows1));

        Dataset csvFile = spark.read()
                .format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(myManualSchema)
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

        spark.read()
                .format("parquet")
                .load(PathUtils.workDir("../../../data/flight-data/parquet/2010-summary.parquet"))
                .show(5);

        csvFile.write()
                .format("parquet")
                .mode("overwrite")
                .save(PathUtils.workDir("/tmp/my-parquet-file.parquet"));

        spark.read().
                format("orc")
                .load(PathUtils.workDir("../../../data/flight-data/orc/2010-summary.orc"))
                .show(5);
        csvFile.write()
                .format("orc")
                .mode("overwrite")
                .save(PathUtils.workDir("../../../tmp/my-json-file.orc"));


//        try {
//            String driver = "org.sqlite.JDBC";
//            String path = "/data/flight-data/jdbc/my-sqlite.db";
//            String url = MessageFormat.format("jdbc:sqlite:/${path}", path);
//            String tablename = "flight_info";
//            Connection connection = DriverManager.getConnection(url);
//            connection.isClosed();
//            connection.close();
//
//            Dataset<Row> dbDataFrame = spark.read()
//                    .format("jdbc")
//                    .option("url", url)
//                    .option("dbtable", tablename)
//                    .option("driver", driver)
//                    .load();
//
//            DataFrameReader pgDF = spark.read()
//                    .format("jdbc")
//                    .option("driver", "org.postgresql.Driver")
//                    .option("url", "jdbc:postgresql://database_server")
//                    .option("dbtable", "schema.tablename")
//                    .option("user", "username")
//                    .option("password", "my-secret-password");
//            dbDataFrame.select("DEST_COUNTRY_NAME")
//                    .distinct()
//                    .show(5);
//            dbDataFrame.select("DEST_COUNTRY_NAME")
//                    .distinct()
//                    .explain();
//            dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')")
//                    .explain();
//
//            String pushdownQuery = "(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info";
//            dbDataFrame = spark.read()
//                    .format("jdbc")
//                    .option("url", url)
//                    .option("dbtable", pushdownQuery)
//                    .option("driver", driver)
//                    .load();
//            dbDataFrame.explain();
//
//            dbDataFrame = spark.read()
//                    .format("jdbc")
//                    .option("url", url)
//                    .option("dbtable", tablename)
//                    .option("driver", driver)
//                    .option("numPartitions", 10)
//                    .load();
//
//            dbDataFrame.select("DEST_COUNTRY_NAME")
//                    .distinct()
//                    .show();
//
//            Properties props = new Properties();
//            props.setProperty("driver", "org.sqlite.JDBC");
//            String[] predicates = new String[]{
//                    "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
//                    "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"};
//            spark.read().jdbc(url, tablename, predicates, props).show();
//            spark.read().jdbc(url, tablename, predicates, props).rdd().getNumPartitions(); // 2
//
//
//            props = new java.util.Properties();
//            props.setProperty("driver", "org.sqlite.JDBC");
//            predicates = new String[]{
//                    "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
//                    "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"};
//            spark.read().jdbc(url, tablename, predicates, props).count(); // 510
//
//            String colName = "count";
//            long lowerBound = 0L;
//            long upperBound = 348113L; // this is the max count in our database
//            int numPartitions = 10;
//            spark.read()
//                    .jdbc(url, tablename, colName, lowerBound, upperBound, numPartitions, props)
//                    .count(); // 255
//
//            String newPath = "jdbc:sqlite://tmp/my-sqlite.db";
//            csvFile.write().mode("overwrite").jdbc(newPath, tablename, props);
//
//            spark.read().jdbc(newPath, tablename, props).count();
//            csvFile.write().mode("append").jdbc(newPath, tablename, props);
//            spark.read().jdbc(newPath, tablename, props).count(); // 765
//
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//        }

        Dataset<String> textFile = spark.read().textFile(PathUtils.inputPath("../../../data/flight-data/csv/2010-summary.csv"));
        textFile.selectExpr("split(value, ',') as rows")
                .show();
        textFile.select("DEST_COUNTRY_NAME").write().text(PathUtils.workDir("/tmp/simple-text-file.txt"));
        textFile.limit(10)
                .select("DEST_COUNTRY_NAME", "count")
                .write()
                .partitionBy("count")
                .text(PathUtils.workDir("/tmp/five-csv-files2.csv"));

        textFile.limit(10)
                .write()
                .mode("overwrite")
                .partitionBy("DEST_COUNTRY_NAME")
                .save(PathUtils.workDir("/tmp/partitioned-files.parquet"));

        int numberBuckets = 10;
        String columnToBucketBy = "count";
        textFile.write().format("parquet").mode("overwrite")
                .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles");

    }
}
