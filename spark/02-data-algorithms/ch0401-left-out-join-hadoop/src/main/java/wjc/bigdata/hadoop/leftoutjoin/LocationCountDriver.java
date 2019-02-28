package wjc.bigdata.hadoop.leftoutjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import wjc.bigdata.algorithm.utils.HadoopPathUtils;

/**
 * LocationCountDriver is driver class for counting locations.
 *
 * @author Mahmoud Parsian
 */
public class LocationCountDriver {

    public static void main(String[] args) throws Exception {
        Path input = new Path(HadoopPathUtils.inputPath(args[0]));
        Path output = new Path(HadoopPathUtils.outputPath(args[1]));
        Configuration conf = new Configuration();

        Job job = new Job(conf);
        job.setJarByClass(LocationCountDriver.class);
        job.setJobName("Phase-2: LocationCountDriver");

        FileInputFormat.addInputPath(job, input);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(LocationCountMapper.class);
        job.setReducerClass(LocationCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, output);
        if (job.waitForCompletion(true)) {
            return;
        } else {
            throw new Exception("LocationCountDriver Failed");
        }
    }
}
