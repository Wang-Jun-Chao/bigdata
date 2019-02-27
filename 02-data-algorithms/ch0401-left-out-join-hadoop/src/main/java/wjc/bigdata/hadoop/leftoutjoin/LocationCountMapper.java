package wjc.bigdata.hadoop.leftoutjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This is an identity mapper.
 * LocationCountMapper implements the map() function for counting locations.
 *
 * @author Mahmoud Parsian
 */
public class LocationCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        context.write(new Text(parts[0]), new Text(parts[1]));
    }
}
