package edu.usfca.cs.mr.reddit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This job counts up the occurrences of Obama, Hillary, and Trump in reddit
 * comments on subreddit /r/politics on a per-month basis.
 */
public class PoliticsJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "/r/politics analysis");

            /* Current class */
            job.setJarByClass(PoliticsJob.class);

            /* Mapper class */
            job.setMapperClass(PoliticsMapper.class);

            /* Combiner */
            job.setCombinerClass(PoliticsReducer.class);

            /* Reducer class */
            job.setReducerClass(PoliticsReducer.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(DateWritable.class);
            job.setMapOutputValueClass(PoliticsWritable.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(DateWritable.class);
            job.setOutputValueClass(PoliticsWritable.class);

            /* Let's try having a reduce task for each month. */
            job.setNumReduceTasks(12);

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job, new Path(args[0]));

            /* Job output path in HDFS. NOTE: if the output path already exists
             * and you try to create it, the job will fail. You may want to
             * automate the creation of new output directories here */
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            /* Wait (block) for the job to complete... */
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
