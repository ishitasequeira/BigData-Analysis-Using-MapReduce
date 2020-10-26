package BloomFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BloomFilterLocal {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    Job job = Job.getInstance(conf, "Bloom Filter");
    job.setJarByClass(BloomFilterLocal.class);
    job.setMapperClass(BloomFilterMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputDir);
    // delete output if exist
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir)) hdfs.delete(outputDir, true);
    boolean success = job.waitForCompletion(true);
    System.out.println(success);
  }

}
