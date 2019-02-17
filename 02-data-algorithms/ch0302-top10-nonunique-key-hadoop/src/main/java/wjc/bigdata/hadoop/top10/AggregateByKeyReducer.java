package wjc.bigdata.hadoop.top10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Reducer's input is: (K, List<Integer>)
 * <p>
 * Aggregate on the list of values and create a single (K,V),
 * where V is the sum of all values passed in the list.
 * <p>
 * This class, AggregateByKeyReducer, accepts (K, [2, 3]) and
 * emits (K, 5).
 *
 * @author Mahmoud Parsian
 */
public class AggregateByKeyReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));
    }
}
