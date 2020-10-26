package StandardDeviation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StandardDeviationDriver {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {		
      
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"standardDeviation");
		job.setJarByClass(StandardDeviationDriver.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setMapperClass(StandardDeviationMapper.class);
		job.setReducerClass(StandardDeviationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(args[1]))) {
			hdfs.delete(new Path(args[1]),true);
		}

		int code = job.waitForCompletion(true)?0:1;

		System.exit(code);
	}
}
