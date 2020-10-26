package Binning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Binning");
        job.setJarByClass(Driver.class);
        job.setMapperClass(BinningMapper.class);
        job.setNumReduceTasks(0);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Configure the MultipleOutputs by adding an output called "bins"
        // With the proper output format and mapper key/value pairs
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);

        // Enable the counters for the job
        // If there is a significant number of different named outputs, this
        // should be disabled
        MultipleOutputs.setCountersEnabled(job, true);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(args[1])))
            hdfs.delete(new Path(args[1]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}