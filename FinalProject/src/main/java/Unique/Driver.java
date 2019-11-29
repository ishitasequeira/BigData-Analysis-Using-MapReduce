package Unique;

import CountMapReduce.CountDriver;
import CountMapReduce.CountMapper;
import CountMapReduce.CountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setNumReduceTasks(1);
        job.setMapperClass(UniqueMapper.class);
        job.setReducerClass(UniqueReducer.class);
        job.setJarByClass(Driver.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        TextInputFormat.addInputPath(job,inputPath);
        TextOutputFormat.setOutputPath(job,outputPath);
        FileSystem hdfs = FileSystem.get(configuration);
        if(hdfs.exists(outputPath)) {
            hdfs.delete(outputPath,true);
        }

        int code = job.waitForCompletion(true)?0:1;

        System.exit(code);

    }
}
