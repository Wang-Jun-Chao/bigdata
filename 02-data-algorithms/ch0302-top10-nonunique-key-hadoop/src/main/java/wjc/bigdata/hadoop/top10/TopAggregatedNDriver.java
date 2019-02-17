package wjc.bigdata.hadoop.top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.data.algorithm.utils.HadoopPathUtils;

import java.io.IOException;

/**
 * TopNDriver: assumes that all K's are unique for all given (K,V) values.
 * Uniqueness of keys can be achieved by using AggregateByKeyDriver job.
 *
 * @author Mahmoud Parsian
 */
public class TopAggregatedNDriver extends Configured implements Tool {

    private static Logger THE_LOGGER = LoggerFactory.getLogger(TopAggregatedNDriver.class);

    /**
     * The main driver for "Top N" program.
     * Invoke this method to submit the map/reduce job.
     *
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 3 parameters
        if (args.length != 3) {
            THE_LOGGER.warn("usage TopNDriver <N> <input> <output>");
            System.exit(1);
        }

        THE_LOGGER.info("N=" + args[0]);
        THE_LOGGER.info("inputDir=" + args[1]);
        THE_LOGGER.info("outputDir=" + args[2]);
        int returnStatus = ToolRunner.run(new TopAggregatedNDriver(), args);
        System.exit(returnStatus);
    }

    @Override
    public int run(String[] args) throws Exception {

        String tmpOut = HadoopPathUtils.outputPath(args[2] + "-tmp");
        args[1] = HadoopPathUtils.inputPath(args[1]);
        args[2] = HadoopPathUtils.outputPath(args[2]);

        boolean status = aggregateJob(new String[]{args[1], tmpOut});
        if (!status) {
            return 0;
        }

        status = topNJob(new String[]{args[0], tmpOut, args[2]});
        return status ? 0 : 1;
    }

    private boolean aggregateJob(String[] args) throws Exception{
        Job job = new Job(getConf());
        job.setJobName("AggregateByKeyDriver");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(AggregateByKeyMapper.class);
        job.setReducerClass(AggregateByKeyReducer.class);
        job.setCombinerClass(AggregateByKeyReducer.class);

        // args[0] = input directory
        // args[1] = output directory
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPathFilter(job, MyPathFilter.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        THE_LOGGER.info("run(): status=" + status);


        return status;
    }

    private boolean topNJob(String[] args) throws Exception {
        Job job = new Job(new Configuration());
        // top N
        final int N = Integer.parseInt(args[0]);
        job.getConfiguration().setInt("N", N);
        job.setJobName("TopNDriver");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);

        // map()'s output (K,V)
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // reduce()'s output (K,V)
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // args[1] = input directory
        // args[2] = output directory
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean status = job.waitForCompletion(true);
        THE_LOGGER.info("run(): status=" + status);
        return status;
    }

}