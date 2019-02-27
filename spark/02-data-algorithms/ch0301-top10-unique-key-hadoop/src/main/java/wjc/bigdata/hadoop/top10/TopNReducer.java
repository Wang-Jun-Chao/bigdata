package wjc.bigdata.hadoop.top10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Reducer's input are local top N from all mappers.
 * We have a single reducer, which creates the final top N.
 *
 * @author Mahmoud Parsian
 */
public class TopNReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    // default
    private int                        N   = 10;
    private SortedMap<Long, String> top = new TreeMap<Long, String>();

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            top.put(key.get(), value.toString());
            // keep only top N
            if (top.size() > N) {
                top.remove(top.firstKey());
            }
        }
        // emit final top N
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // default is top 10
        this.N = context.getConfiguration().getInt("N", N);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Long> keys = new ArrayList<Long>(top.keySet());
        for (int i = keys.size() - 1; i >= 0; i--) {
            context.write(new LongWritable(keys.get(i)), new Text(top.get(keys.get(i))));
        }
    }
}
