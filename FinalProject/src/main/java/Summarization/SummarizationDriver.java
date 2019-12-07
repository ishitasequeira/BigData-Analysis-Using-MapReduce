package Summarization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SummarizationDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SummarizationDriver.class);

        job.setJobName("myjob");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        job.setMapperClass(SummarizationMapper.class);
        job.setReducerClass(SummarizationReducer.class);


        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem hdfs = FileSystem.get(configuration);
        if(hdfs.exists(outputPath)) {
            hdfs.delete(outputPath,true);
        }

        int code = job.waitForCompletion(true)?0:1;

        System.exit(code);

    }

}
