package wjc.bigdata.spark.distributedvariables;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;
import scala.math.BigInt;
import wjc.bigdata.spark.util.PathUtils;

import java.util.HashMap;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-31 10:04
 **/
public class DistributedVariables implements Serializable {
    private final static Logger          logger = LoggerFactory.getLogger(DistributedVariables.class);
    private static       LongAccumulator accChina;
    private static       EvenAccumulator acc;

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch1401-distributed-variables")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

        String[] myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
                .split(" ");
        JavaRDD<String> words = context.parallelize(Lists.newArrayList(myCollection), 2);

        HashMap<String, Integer> supplementalData = Maps.newHashMap();
        supplementalData.put("Spark", 1000);
        supplementalData.put("Definitive", 200);
        supplementalData.put("Big", -300);
        supplementalData.put("Simple", 100);

        Broadcast<HashMap<String, Integer>> suppBroadcast = context.broadcast(supplementalData);
        suppBroadcast.getValue();

        words.map(word -> new Tuple2<>(word, suppBroadcast.getValue().getOrDefault(word, 0)))
                .sortBy(Tuple2::_2, true, 1)
                .collect();

        Dataset<Flight> flights = spark.read()
                .parquet(PathUtils.workDir("../../../data/flight-data/parquet/2010-summary.parquet"))
                .as(Encoders.bean(Flight.class));

        LongAccumulator accUnnamed = new LongAccumulator();

        spark.sparkContext().register(accUnnamed);

        accChina = new LongAccumulator();
        spark.sparkContext().longAccumulator("China");
        spark.sparkContext().register(accChina, "China");

        flights.foreach(DistributedVariables::accChinaFunc);

        System.out.println(accChina.value());

        acc = new EvenAccumulator();
        spark.sparkContext().longAccumulator("EvenAccumulator");
        spark.sparkContext().register(acc, "EvenAccumulator");
        flights.foreach(flightRow -> acc.add(BigInt.apply(flightRow.count)));
        BigInt value = acc.value();
        System.out.println(value);
    }

    public static void accChinaFunc(Flight flight_row) {
        String destination = flight_row.DEST_COUNTRY_NAME;
        String origin = flight_row.ORIGIN_COUNTRY_NAME;
        if ("China".equals(destination)) {
            accChina.add(flight_row.count);
        }
        if ("China".equals(origin)) {
            accChina.add(flight_row.count);
        }
    }


    public static class EvenAccumulator extends AccumulatorV2<BigInt, BigInt> implements Serializable {
        private BigInt num = BigInt.apply(0);

        @Override
        public void reset() {
            this.num = BigInt.apply(0);
        }

        @Override
        public void add(BigInt intValue) {
            if (intValue.intValue() % 2 == 0) {
                this.num.$plus(intValue);
            }
        }

        @Override
        public void merge(AccumulatorV2<BigInt, BigInt> other) {
            this.num.$plus(other.value());
        }

        @Override
        public BigInt value() {
            return this.num;
        }

        @Override
        public AccumulatorV2<BigInt, BigInt> copy() {
            return new EvenAccumulator();
        }

        @Override
        public boolean isZero() {
            return this.num.intValue() == 0;
        }
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
