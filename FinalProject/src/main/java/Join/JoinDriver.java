package Join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinDriver {
    public static void main(String[] args) {
        try{
            Configuration conf = new Configuration();
            Job job = new Job(conf, "Reduce-side join");
            job.setJarByClass(JoinDriver.class);
            job.setReducerClass(JoinReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinMapper1.class);
            MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, JoinMapper2.class);
            Path outputPath = new Path(args[2]);

            FileOutputFormat.setOutputPath(job, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath);

            FileSystem hdfs = FileSystem.get(conf);
            if(hdfs.exists(outputPath)) {
                hdfs.delete(outputPath,true);
            }

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
