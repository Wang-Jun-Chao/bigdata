package wjc.bigdata.spark.structuredstreamingbasics;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import wjc.bigdata.spark.util.PathUtils;
import wjc.bigdata.spark.util.SparkUtils;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-04-01 22:10
 **/
public class StructuredStreamingBasics {
    public static void main(String[] args) throws StreamingQueryException, InterruptedException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch2101-structured-streaming-sasics")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        Dataset<Row> staticData = spark
                .read()
                .json(PathUtils.workDir("../../../data/activity-data/"));
        StructType dataSchema = staticData.schema();

        Dataset<Row> streaming = spark
                .readStream()
                .schema(dataSchema)
                .option("maxFilesPerTrigger", 1)
                .json(PathUtils.workDir("../../../data/activity-data"));

        Dataset<Row> activityCounts = streaming.groupBy("gt").count();

        spark.conf().set("spark.sql.shuffle.partitions", 5);

        StreamingQuery activityQuery = activityCounts
                .writeStream()
                .queryName("activity_counts")
                .format("memory")
                .outputMode("complete")
                .start();

        activityQuery.awaitTermination();

        spark.streams().active();

        for (int i = 1; i <= 5; i++) {
            spark.sql("SELECT * FROM activity_counts").show();
            Thread.sleep(1000);
        }

        StreamingQuery simpleTransform = streaming
                .withColumn("stairs", functions.expr("gt like '%stairs%'"))
                .where("stairs")
                .where("gt is not null")
                .select("gt", "model", "arrival_time", "creation_time")
                .writeStream()
                .queryName("simple_transform")
                .format("memory")
                .outputMode("append")
                .start();

        StreamingQuery deviceModelStats = streaming
                .cube("gt", "model")
                .avg()
                .drop("avg(Arrival_time)")
                .drop("avg(Creation_Time)")
                .drop("avg(Index)")
                .writeStream()
                .queryName("device_counts")
                .format("memory")
                .outputMode("complete")
                .start();

        Dataset<Row> historicalAgg = staticData
                .groupBy("gt", "model")
                .avg();

        deviceModelStats = streaming
                .drop("Arrival_Time", "Creation_Time", "Index")
                .cube("gt", "model")
                .avg()
                .join(historicalAgg, SparkUtils.seq("gt", "model"))
                .writeStream()
                .queryName("device_counts")
                .format("memory")
                .outputMode("complete")
                .start();

//        Dataset<Row> ds1 = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//                .option("subscribe", "topic1")
//                .load();
//
//        Dataset<Row> ds2 = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//                .option("subscribe", "topic1,topic2")
//                .load();
//
//        Dataset<Row> ds3 = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//                .option("subscribePattern", "topic.*")
//                .load();

//        ds1.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
//                .writeStream()
//                .format("kafka")
//                .option("checkpointLocation", "/to/HDFS-compatible/dir")
//                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//                .start();
//
//        ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//                .writeStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//                .option("checkpointLocation", "/to/HDFS-compatible/dir")
//                .option("topic", "topic1")
//                .start();

//        datasetOfString.write().foreach(new ForeachWriter<String>() {
//            @Override
//            public boolean open(long partitionId, long version) {
//                // open a database connection
//                return false;
//            }
//
//            @Override
//            public void process(String record) {
//                // write string to connection
//            }
//
//            @Override
//            public void close(Throwable errorOrNull) {
//                // close the connection
//            }
//        });

        Dataset<Row> socketDF = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

//        activityCounts.format("console").write();

        activityCounts
                .writeStream()
                .format("memory")
                .queryName("my_device_table");


        activityCounts
                .writeStream()
                .trigger(Trigger.ProcessingTime("100 seconds"))
                .format("console")
                .outputMode("complete")
                .start();

        activityCounts
                .writeStream()
                .trigger(Trigger.Once())
                .format("console")
                .outputMode("complete")
                .start();

        dataSchema = spark
                .read()
                .parquet(PathUtils.workDir("../../../data/flight-data/parquet/2010-summary.parquet/"))
                .schema();
        Dataset<Row> flightsDF = spark
                .readStream()
                .schema(dataSchema)
                .parquet(PathUtils.workDir("../../../data/flight-data/parquet/2010-summary.parquet/"));
        Dataset<Flight> flights = flightsDF.as(Encoders.bean(Flight.class));

        flights.filter((FilterFunction<Flight>) flightRow -> flightRow.getORIGIN_COUNTRY_NAME().equalsIgnoreCase(flightRow.getDEST_COUNTRY_NAME()))
                .groupByKey(
                        (MapFunction<Flight, String>) Flight::getDEST_COUNTRY_NAME,
                        Encoders.STRING())
                .count()
                .writeStream()
                .queryName("device_counts")
                .format("memory")
                .outputMode("complete")
                .start();
    }

    public static boolean originIsDestination(Flight flightRow) {
        return flightRow.getORIGIN_COUNTRY_NAME().equalsIgnoreCase(flightRow.getDEST_COUNTRY_NAME());
    }

    public static class Flight {
        public String DEST_COUNTRY_NAME;
        public String ORIGIN_COUNTRY_NAME;
        public Long   count;

        public Flight() {
        }

        public Flight(String DEST_COUNTRY_NAME, String ORIGIN_COUNTRY_NAME, Long count) {
            this.DEST_COUNTRY_NAME = DEST_COUNTRY_NAME;
            this.ORIGIN_COUNTRY_NAME = ORIGIN_COUNTRY_NAME;
            this.count = count;
        }

        public String getDEST_COUNTRY_NAME() {
            return DEST_COUNTRY_NAME;
        }

        public void setDEST_COUNTRY_NAME(String DEST_COUNTRY_NAME) {
            this.DEST_COUNTRY_NAME = DEST_COUNTRY_NAME;
        }

        public String getORIGIN_COUNTRY_NAME() {
            return ORIGIN_COUNTRY_NAME;
        }

        public void setORIGIN_COUNTRY_NAME(String ORIGIN_COUNTRY_NAME) {
            this.ORIGIN_COUNTRY_NAME = ORIGIN_COUNTRY_NAME;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }
}
