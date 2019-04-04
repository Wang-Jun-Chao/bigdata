package wjc.bigdata.spark.eventtimeandstatefulprocessing;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function3;
import wjc.bigdata.spark.util.PathUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-04-02 21:55
 **/
public class EventTimeAndStatefulProcessing {
    private final static Logger logger = LoggerFactory.getLogger(EventTimeAndStatefulProcessing.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch2201-event-time-and-stateful-processing")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        spark.conf()
                .set("spark.sql.shuffle.partitions", 5);
        Dataset<Row> staticRdd = spark.read().json(PathUtils.workDir("../../../data/activity-data"));
        Dataset<Row> streaming = spark
                .readStream()
                .schema(staticRdd.schema())
                .option("maxFilesPerTrigger", 10)
                .json(PathUtils.workDir("../../../data/activity-data"));
        streaming.printSchema();

        Dataset<Row> withEventTime = streaming.selectExpr(
                "*",
                "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time");

        withEventTime.groupBy(functions.window(functions.col("event_time"), "10 minutes"))
                .count()
                .writeStream()
                .queryName("events_per_window")
                .format("memory")
                .outputMode("complete")
                .start();


        spark.sql("SELECT * FROM events_per_window")
                .printSchema();

        withEventTime.groupBy(functions.window(functions.col("event_time"), "10 minutes"), functions.col("User"))
                .count()
                .writeStream()
                .queryName("events_per_window")
                .format("memory")
                .outputMode("complete")
                .start();

        withEventTime.groupBy(functions.window(functions.col("event_time"), "10 minutes", "5 minutes"))
                .count()
                .writeStream()
                .queryName("events_per_window")
                .format("memory")
                .outputMode("complete")
                .start();

        withEventTime
                .withWatermark("event_time", "5 hours")
                .groupBy(functions.window(functions.col("event_time"), "10 minutes", "5 minutes"))
                .count()
                .writeStream()
                .queryName("events_per_window")
                .format("memory")
                .outputMode("complete")
                .start();

        withEventTime
                .withWatermark("event_time", "5 seconds")
                .dropDuplicates("User", "event_time")
                .groupBy("User")
                .count()
                .writeStream()
                .queryName("deduplicated")
                .format("memory")
                .outputMode("complete")
                .start();

        withEventTime
                .selectExpr("User as user",
                        "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
                .as(Encoders.bean(InputRow.class))
                .groupByKey((MapFunction<InputRow, String>) InputRow::getUser, Encoders.STRING())
                .mapGroupsWithState(GroupStateTimeout.NoTimeout(), new Function3<String, scala.collection.Iterator<InputRow>, GroupState<DeviceState>, Iterator<OutputRow>>() {
                }) (updateAcrossEvents2)
                .writeStream()
                .queryName("events_per_window")
                .format("memory")
                .outputMode("update")
                .start();
    }

    public static Iterator<OutputRow> updateAcrossEvents2(
            String device, Iterator<InputRow2> inputs,
            GroupState<DeviceState> oldState) {

        List<InputRow2> list = new ArrayList<>();
        inputs.forEachRemaining(list::add);
        list.sort(Comparator.comparing(x -> x.timestamp));

//       return list.stream().flatMap(input -> {
//            DeviceState state = oldState.exists() ? oldState.get() :
//                    new DeviceState(device, new ArrayList<>(), 0);
//
//            DeviceState newState = updateWithEvent(state, input);
//            if (newState.count >= 500) {
//                // One of our windows is complete; replace our state with an empty
//                // DeviceState and output the average for the past 500 items from
//                // the old state
//                oldState.update(new DeviceState(device, new ArrayList<>(), 0));
//                OutputRow outputRow = new OutputRow(
//                        device,
//                        newState.values.stream().reduce((double) 0, Double::sum) / newState.values.size());
//                return Lists.newArrayList(outputRow).iterator();
//            } else {
//                // Update the current DeviceState object in place and output no
//                // records
//                oldState.update(newState);
//                return Lists.newArrayList().iterator();
//            }
//        }).collect(Collectors.toList());

        return null;
    }

    public static DeviceState updateWithEvent(DeviceState state, InputRow2 input) {
        state.count += 1;
        // maintain an array of the x-axis values
        state.values.add(input.x);
        return state;
    }

    public UserState updateAcrossEvents(String user,
                                        Iterator<InputRow> inputs,
                                        GroupState<UserState> oldState) {
        UserState state = oldState.exists() ? oldState.get() :
                new UserState(user, "", new Timestamp(6284160000000L), new Timestamp(6284160L));

        // we simply specify an old date that we can compare against and
        // immediately update based on the values in our data
        UserState[] states = new UserState[]{state};
        inputs.forEachRemaining(input -> {
            states[0] = updateUserStateWithEvent(states[0], input);
            oldState.update(states[0]);
        });
        return states[0];
    }

    public UserState updateUserStateWithEvent(UserState state, InputRow input) {
        if (input.getTimestamp() == null) {
            return state;
        }
        if (Objects.equals(state.getActivity(), input.getActivity())) {

            if (input.getTimestamp().after(state.getEnd())) {
                state.setEnd(input.getTimestamp());
            }
            if (input.getTimestamp().before(state.getStart())) {
                state.setStart(input.getTimestamp());
            }
        } else {
            if (input.getTimestamp().after(state.getEnd())) {
                state.setStart(input.getTimestamp());
                state.setEnd(input.getTimestamp());
                state.setActivity(input.getActivity());
            }
        }

        return state;
    }

    public static class InputRow2 {
        private String    device;
        private Timestamp timestamp;
        private Double    x;

        public String getDevice() {
            return device;
        }

        public void setDevice(String device) {
            this.device = device;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public Double getX() {
            return x;
        }

        public void setX(Double x) {
            this.x = x;
        }
    }

    public static class DeviceState {
        private String       device;
        private List<Double> values = new ArrayList<>();
        private int          count;

        public DeviceState() {
        }

        public DeviceState(String device, List<Double> values, int count) {
            this.device = device;
            this.values = values;
            this.count = count;
        }

        public String getDevice() {
            return device;
        }

        public void setDevice(String device) {
            this.device = device;
        }

        public List<Double> getValues() {
            return values;
        }

        public void setValues(List<Double> values) {
            this.values = values;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    public static class OutputRow {
        private String device;
        private double previousAverage;

        public OutputRow() {
        }

        public OutputRow(String device, double previousAverage) {
            this.device = device;
            this.previousAverage = previousAverage;
        }

        public String getDevice() {
            return device;
        }

        public void setDevice(String device) {
            this.device = device;
        }

        public double getPreviousAverage() {
            return previousAverage;
        }

        public void setPreviousAverage(double previousAverage) {
            this.previousAverage = previousAverage;
        }
    }


    public static class InputRow {
        private String    user;
        private Timestamp timestamp;
        private String    activity;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public String getActivity() {
            return activity;
        }

        public void setActivity(String activity) {
            this.activity = activity;
        }
    }


    public static class UserState {
        private String    user;
        private String    activity;
        private Timestamp start;
        private Timestamp end;

        public UserState() {
        }

        public UserState(String user, String activity, Timestamp start, Timestamp end) {
            this.user = user;
            this.activity = activity;
            this.start = start;
            this.end = end;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getActivity() {
            return activity;
        }

        public void setActivity(String activity) {
            this.activity = activity;
        }

        public Timestamp getStart() {
            return start;
        }

        public void setStart(Timestamp start) {
            this.start = start;
        }

        public Timestamp getEnd() {
            return end;
        }

        public void setEnd(Timestamp end) {
            this.end = end;
        }
    }
}
