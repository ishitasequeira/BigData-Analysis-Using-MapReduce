package Partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Driver {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    //
      Configuration conf = new Configuration();
      Job job = new Job(conf,"Partitioning Pattern");
      job.setJarByClass(Driver.class);
      job.setMapperClass(YearPartitionerMapper.class);
      job.setNumReduceTasks(5);
      job.setReducerClass(YearPartitionerReducer.class);
      job.setPartitionerClass(YearPartitioner.class);
      YearPartitioner.setMinLastAccessDateYear(job,2011);

      job.setInputFormatClass(TextInputFormat.class);

      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      Path outputPath = new Path(args[1]);
      FileOutputFormat.setOutputPath(job, outputPath);
      FileInputFormat.addInputPath(job,new Path(args[0]));
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      FileSystem hdfs = FileSystem.get(conf);
      if(hdfs.exists(outputPath)) {
          hdfs.delete(outputPath,true);
      }

      System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
