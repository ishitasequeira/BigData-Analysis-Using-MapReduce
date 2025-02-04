package CountMapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CountDriver {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
      Configuration configuration = new Configuration();
      Job job = Job.getInstance(configuration);


      job.setNumReduceTasks(1);
      job.setMapperClass(CountMapper.class);
      job.setReducerClass(CountReducer.class);
      job.setJarByClass(CountDriver.class);
      job.setCombinerClass(CountReducer.class);
      Path inputPath = new Path(args[0]);
      Path outputPath = new Path(args[1]);
      job.setInputFormatClass(TextInputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setMapOutputKeyClass(Text.class);
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
