package Top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Top10Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJobName("part6-Counting Ip Addresses");
        job.setJarByClass(Top10.Top10Driver.class);

        Configuration mapConf1 = new Configuration(false);
        ChainMapper.addMapper(job, Top10Mapper1.class, LongWritable.class, Text.class,
                Text.class, Text.class,  mapConf1);

        Configuration reduceConf1 = new Configuration(false);
        ChainReducer.setReducer(job, Top10Reducer.class, Text.class, IntWritable.class,
                Text.class, Text.class, reduceConf1);

        Configuration mapConf2 = new Configuration(false);
        ChainReducer.addMapper(job, TopMapper2.class, Text.class, Text.class,
                NullWritable.class, Text.class,  mapConf2);

//        Configuration reduceConf2 = new Configuration(false);
//        ChainReducer.setReducer(job, MyReducer2.class, NullWritable.class, Text.class,
//                NullWritable.class, Text.class, reduceConf2);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

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
