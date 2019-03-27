package wjc.bigdata.spark.dataset;


import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.util.Random;
import wjc.bigdata.spark.util.PathUtils;
import wjc.bigdata.spark.util.SparkUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-27 22:32
 **/
public class Datasets {
    private final static Logger logger = LoggerFactory.getLogger(Datasets.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch1101-dataset")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        Dataset<Row> flightsDF = spark.read()
                .parquet(PathUtils.workDir("../../../data/flight-data/parquet/2010-summary.parquet/"));
        Dataset<Row> flights = flightsDF.as("Flight");
        flights.show(2);
        Object destCountryName = flights.first().getAs("DEST_COUNTRY_NAME");
        System.out.println(destCountryName);


        Row first = flights.filter((FilterFunction<Row>) value ->
                value.getAs("ORIGIN_COUNTRY_NAME").equals(
                        value.getAs("DEST_COUNTRY_NAME"))).first();
        System.out.println(first);

        Arrays.stream((Row[])flights.collect())
                .filter(row -> row.getAs("ORIGIN_COUNTRY_NAME").equals(
                        row.getAs("DEST_COUNTRY_NAME")));

        Dataset<String> destinations = flights.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                return value.getAs("DEST_COUNTRY_NAME");
            }
        }, Encoders.STRING());

        Object localDestinations = destinations.take(5);
        System.out.println(Arrays.toString((Object[])localDestinations));

        Dataset<Row> flightsMeta = spark.range(500)
                .map(
                        (MapFunction<Long, Tuple2>) x -> new Tuple2<>(x, new Random().nextLong()),
                        Encoders.bean(Tuple2.class))
                .withColumnRenamed("_1", "count")
                .withColumnRenamed("_2", "randomData")
                .as("FlightMetadata");

        Dataset flights2 = flights
                .joinWith(flightsMeta, flights.col("count")
                        .$eq$eq$eq(flightsMeta.col("count")));
        flights2.selectExpr("_1.DEST_COUNTRY_NAME").show();

        flights2 = flights.join(flightsMeta, SparkUtils.seq("count"));
        flights2.show();

        flights.groupBy("DEST_COUNTRY_NAME").count();
        flights.show();


        flights
                .groupByKey(
                        (MapFunction<Row, String>) x -> x.getAs("DEST_COUNTRY_NAME"),
                        Encoders.STRING())
                .count()
                .explain();


        flights
                .groupByKey(
                        (MapFunction<Row, String>) x -> x.getAs("DEST_COUNTRY_NAME"),
                        Encoders.STRING())
                .flatMapGroups(new FlatMapGroupsFunction<String, Row, Tuple2>() {
                    @Override
                    public Iterator<Tuple2> call(String key, Iterator<Row> values) throws Exception {
                        ArrayList<Tuple2> objects = new ArrayList<>();
                        values.forEachRemaining(row -> {
                            if ((int) row.getAs("count") > 4) {
                                objects.add(new Tuple2<>(key, row));
                            }
                        });
                        return objects.iterator();
                    }
                }, Encoders.bean(Tuple2.class))
                .show();

        flights
                .groupByKey(
                        (MapFunction<Row, String>) x -> x.getAs("DEST_COUNTRY_NAME"),
                        Encoders.STRING())
                .mapValues(new MapFunction<Row, Integer>() {
                    @Override
                    public Integer call(Row value) throws Exception {
                        return 1;
                    }
                }, Encoders.INT())
                .count()
                .show();

        flights
                .groupByKey(
                        (MapFunction<Row, String>) x -> x.getAs("DEST_COUNTRY_NAME"),
                        Encoders.STRING())
                .reduceGroups(new ReduceFunction<Row>() {
                    @Override
                    public Row call(Row v1, Row v2) throws Exception {

                        return new GenericRow(new Object[]{
                                v1.getAs("DEST_COUNTRY_NAME"),
                                null,
                                (int) v1.getAs("count") + (int) v2.getAs("count")});
                    }
                }).show();

        flights
                .groupBy("DEST_COUNTRY_NAME")
                .count()
                .explain();

    }


    public static class Flight {
        private String DEST_COUNTRY_NAME;
        private String ORIGIN_COUNTRY_NAME;
        private long   count;

        public Flight(String DEST_COUNTRY_NAME, String ORIGIN_COUNTRY_NAME, long count) {
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

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
}
