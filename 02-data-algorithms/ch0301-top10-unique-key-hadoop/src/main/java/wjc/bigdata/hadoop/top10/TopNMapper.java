package wjc.bigdata.hadoop.top10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Mapper's input are read from SequenceFile and records are: (K, V)
 * where
 * K is a Text
 * V is an Integer
 *
 * @author Mahmoud Parsian
 */
public class TopNMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    // default
    private int                     N   = 10;
    private SortedMap<Long, String> top = new TreeMap<Long, String>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        String keyAsString = key.toString();
//        long frequency = value.get();
//        String compositeValue = keyAsString + "," + frequency;
//        top.put(frequency, compositeValue);

        String compositeValue = value.toString();
        int index = compositeValue.indexOf(',');
        long frequency = Long.parseLong(compositeValue.substring(0, index));
        top.put(frequency, compositeValue);
        // keep only top N
        if (top.size() > N) {
            top.remove(top.firstKey());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // default is top 10
        this.N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : top.entrySet()) {
            context.write(new LongWritable(entry.getKey()), new Text(entry.getValue()));
        }
    }

}
