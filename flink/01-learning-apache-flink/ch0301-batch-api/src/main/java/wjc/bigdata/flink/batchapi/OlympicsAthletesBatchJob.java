package wjc.bigdata.flink.batchapi;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Implements the Oylympics Athletes program that gives insights about games played and medals won.
 * <p>
 * Sample input file is provided in src/main/resources/data folder
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink batch program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 */
public class OlympicsAthletesBatchJob {


    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = OlympicsAthletesBatchJob.class.getClassLoader().getResource("").getPath()
                + "/olympic-athletes.csv";
        DataSet<Record> csvInput = env.readCsvFile(filePath)
                .pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");

        DataSet<Tuple2<String, Integer>> groupedByCountry = csvInput
                .flatMap(new FlatMapFunction<Record, Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void flatMap(Record record, Collector<Tuple2<String, Integer>> out) {

                        out.collect(new Tuple2<>(record.getCountry(), 1));
                    }
                }).groupBy(0).sum(1);

        groupedByCountry.print();

        DataSet<Tuple2<String, Integer>> groupedByGame = csvInput
                .flatMap(new FlatMapFunction<Record, Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void flatMap(Record record, Collector<Tuple2<String, Integer>> out) {
                        out.collect(new Tuple2<>(record.getGame(), 1));
                    }
                }).groupBy(0).sum(1);
        groupedByGame.print();

    }

}
